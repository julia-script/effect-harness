import { arrayOverlay } from './arrays.ts'
import { cloneNode } from './clone.ts'
import { nodeBase, nodeParent } from './context.ts'
import type { OverlayContext, OverlayNode } from './context.ts'
import {
  addDenseCandidate,
  buildDenseRegions,
  denseCandidates,
  hasCoveringDenseRegion,
  recordDenseArrayPosition,
} from './dense.ts'
import type { DenseCandidates, DenseRegion } from './dense.ts'
import { emitArrayOperations, emitObjectOperations, MAX_DELTA_OPERATIONS } from './emitNode.ts'
import { isArray } from './json.ts'
import { RESERVED_SEGMENTS, reservedIndex, setOp } from './ops.ts'
import type { Op, Path } from './ops.ts'
import { resolvePath } from './paths.ts'

/**
 * The ops of a change, from its dirty nodes. Nodes emit shallowest first; a node below a
 * placement or a folded ancestor emits nothing, since the ancestor's payload carries it. Mutations
 * at or below a reserved key fold into a set of the nearest safe ancestor, and a batch past
 * `MAX_DELTA_OPERATIONS` folds into one replacement.
 */

const MAX_SIMPLE_OBJECT_NODES = 128

const isArrayNode = (node: OverlayNode): boolean => isArray(nodeBase(node))

const hasReservedMutation = (node: OverlayNode): boolean => {
  if (node.writeKey !== undefined && RESERVED_SEGMENTS.has(node.writeKey)) return true
  if (node.deleteKey !== undefined && RESERVED_SEGMENTS.has(node.deleteKey)) return true
  for (const key of node.writes?.keys() ?? []) if (RESERVED_SEGMENTS.has(key)) return true
  for (const key of node.deletes ?? []) if (RESERVED_SEGMENTS.has(key)) return true
  return false
}

const hasFoldedAncestor = (node: OverlayNode, folded: ReadonlySet<OverlayNode>): boolean => {
  for (let parent = nodeParent(node); parent !== undefined; parent = nodeParent(parent)) {
    if (folded.has(parent)) return true
  }
  return false
}

const hasPlacementAncestor = (node: OverlayNode): boolean => {
  for (let current: OverlayNode | undefined = node; current !== undefined;) {
    const parent = nodeParent(current)
    if (parent !== undefined && current.parentPlacement) return true
    current = parent
  }
  return false
}

const hasArrayAncestor = (node: OverlayNode): boolean => {
  for (let parent = nodeParent(node); parent !== undefined; parent = nodeParent(parent)) {
    if (isArrayNode(parent)) return true
  }
  return false
}

/** The dirty nodes when every one is an object outside arrays, placements, and reserved keys. */
const simpleObjectNodes = (context: OverlayContext): Array<OverlayNode> | undefined => {
  const nodes: Array<OverlayNode> = []
  for (const node of context.dirty) {
    if (isArrayNode(node) || hasReservedMutation(node) || hasPlacementAncestor(node)) {
      return undefined
    }
    if (hasArrayAncestor(node)) return undefined
    const path = resolvePath(node)
    if (path === undefined) continue
    if (reservedIndex(path) >= 0) return undefined
    nodes.push(node)
    if (nodes.length > MAX_SIMPLE_OBJECT_NODES) return undefined
  }
  return nodes
}

const depth = (node: OverlayNode): number => node.preparedPath!.length

/**
 * The common case of a few object edits. A stable insertion sort by depth avoids comparator and
 * bucket allocations, and when no container write normalized away, the candidate is cloned from
 * the draft directly instead of replayed from the ops.
 */
const emitSimpleObjectOperations = (context: OverlayContext): Array<Op> | undefined => {
  const nodes = simpleObjectNodes(context)
  if (nodes === undefined) return undefined
  for (let index = 1; index < nodes.length; index++) {
    const node = nodes[index]!
    let at = index
    while (at > 0 && depth(nodes[at - 1]!) > depth(node)) {
      nodes[at] = nodes[at - 1]!
      at -= 1
    }
    nodes[at] = node
  }
  const operations: Array<Op> = []
  let direct = true
  for (const node of nodes) {
    if (emitObjectOperations(node, node.preparedPath!, operations)) direct = false
    if (operations.length > MAX_DELTA_OPERATIONS) return [['r', cloneNode(context.root!)]]
  }
  context.simpleObjectMaterialization = direct
  return operations
}

const candidatesOf = (groups: Map<OverlayNode, DenseCandidates>, node: OverlayNode) => {
  let candidates = groups.get(node)
  if (candidates === undefined) {
    const base = nodeBase(node)
    candidates = denseCandidates(isArray(base) ? base.length : 0)
    groups.set(node, candidates)
  }
  return candidates
}

/** Dense regions of every array whose changed entries are dense enough to fold. */
const denseRegionsOf = (context: OverlayContext): Map<OverlayNode, Array<DenseRegion>> => {
  const groups = new Map<OverlayNode, DenseCandidates>()
  for (const node of context.dirty) {
    recordDenseArrayPosition(node, groups)
    const overlay = isArrayNode(node) ? arrayOverlay(node) : undefined
    if (overlay === undefined || overlay.structural || (overlay.baseOverrides?.size ?? 0) === 0) {
      continue
    }
    const candidates = candidatesOf(groups, node)
    for (const index of overlay.baseOverrides!.keys()) addDenseCandidate(candidates, index)
  }
  const regions = new Map<OverlayNode, Array<DenseRegion>>()
  for (const [array, candidates] of groups) {
    const path = resolvePath(array)
    if (path === undefined || reservedIndex(path) >= 0) continue
    const built = buildDenseRegions(candidates)
    if (built.length > 0) regions.set(array, built)
  }
  return regions
}

interface Emission {
  readonly paths: Map<OverlayNode, Path>
  readonly forcedFolds: Map<OverlayNode, Path>
}

/** The path each node emits at, with reserved mutations folded into their safe ancestor. */
const emissionPaths = (
  context: OverlayContext,
  regions: ReadonlyMap<OverlayNode, ReadonlyArray<DenseRegion>>,
): Emission => {
  const paths = new Map<OverlayNode, Path>()
  const forcedFolds = new Map<OverlayNode, Path>()
  for (const node of context.dirty) {
    const path = resolvePath(node)
    if (path === undefined || hasCoveringDenseRegion(node, path, regions)) continue
    paths.set(node, path)
    if (!isArrayNode(node) && hasReservedMutation(node)) forcedFolds.set(node, path)
    const reservedAt = reservedIndex(path)
    if (reservedAt < 0) continue
    let ancestor = node
    for (let at = path.length; at > reservedAt; at--) ancestor = nodeParent(ancestor)!
    forcedFolds.set(ancestor, path.slice(0, reservedAt))
  }
  for (const [node, path] of forcedFolds) paths.set(node, path)
  for (const node of regions.keys()) {
    const path = resolvePath(node)
    if (path !== undefined && !hasCoveringDenseRegion(node, path, regions)) paths.set(node, path)
  }
  return { paths, forcedFolds }
}

/** Emission paths grouped by depth, shallowest first. */
const byDepth = (paths: ReadonlyMap<OverlayNode, Path>): Array<Array<OverlayNode>> => {
  let maxDepth = 0
  for (const path of paths.values()) maxDepth = Math.max(maxDepth, path.length)
  const buckets: Array<Array<OverlayNode>> = Array.from({ length: maxDepth + 1 }, () => [])
  for (const [node, path] of paths) buckets[path.length]!.push(node)
  return buckets
}

/** The ops of an open change's edits. */
export const emitOperations = (context: OverlayContext): Array<Op> => {
  const simple = emitSimpleObjectOperations(context)
  if (simple !== undefined) return simple
  const operations: Array<Op> = []
  const regions = denseRegionsOf(context)
  const { paths, forcedFolds } = emissionPaths(context, regions)
  const folded = new Set<OverlayNode>()
  for (const bucket of byDepth(paths)) {
    for (const node of bucket) {
      const path = paths.get(node)!
      if (hasPlacementAncestor(node) || hasFoldedAncestor(node, folded)) continue
      if (forcedFolds.has(node)) {
        operations.push(setOp(path, cloneNode(node)))
        folded.add(node)
        continue
      }
      if (isArrayNode(node)) emitArrayOperations(node, path, operations, regions.get(node))
      else emitObjectOperations(node, path, operations)
      if (operations.length > MAX_DELTA_OPERATIONS) return [['r', cloneNode(context.root!)]]
    }
  }
  return operations
}
