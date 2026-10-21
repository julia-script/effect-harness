import { arrayOverlay, baseElement } from './arrays.ts'
import { nodeBase, nodeParent, storedValue } from './context.ts'
import type { OverlayNode } from './context.ts'
import { isArray } from '../../internal/json.ts'
import type { Path } from './ops.ts'
import { resolvePath } from './paths.ts'

/**
 * Dense-region folding. When at least 256 entries of a non-structural array changed, in a run
 * where at least half the entries changed, the run is emitted as one splice of its current
 * entries instead of an op per edit.
 */

export interface DenseRegion {
  readonly start: number
  readonly length: number
}

export interface DenseCandidates {
  readonly indices: Array<number>
  bits: Uint8Array | undefined
  readonly length: number
}

const MIN_DENSE_EDITS = 256

export const denseCandidates = (length: number): DenseCandidates => ({
  indices: [],
  bits: undefined,
  length,
})

/** Record a changed index; past 256 candidates, switch to a bitmap over the array. */
export const addDenseCandidate = (candidates: DenseCandidates, index: number): void => {
  if (candidates.bits !== undefined) {
    candidates.bits[index] = 1
    return
  }
  candidates.indices.push(index)
  if (candidates.indices.length < MIN_DENSE_EDITS) return
  candidates.bits = new Uint8Array(candidates.length)
  for (const existing of candidates.indices) candidates.bits[existing] = 1
  candidates.indices.length = 0
}

/**
 * The nearest array ancestor of a dirty node and the base index the node's subtree sits at, when
 * that array is not structural and still holds the same container there.
 */
const denseArrayPosition = (
  node: OverlayNode,
): { readonly array: OverlayNode; readonly index: number } | undefined => {
  let child = node
  for (let parent = nodeParent(child); parent !== undefined; parent = nodeParent(child)) {
    if (isArray(nodeBase(parent))) {
      const overlay = arrayOverlay(parent)
      if (child.parentKind !== 1 || overlay.structural) return undefined
      const index = Number(child.parentKey)
      const reference = overlay.baseOverrides?.get(index)
      const current =
        reference === undefined
          ? baseElement(parent, index)
          : storedValue(parent.context, reference)
      return current === nodeBase(child) ? { array: parent, index } : undefined
    }
    child = parent
  }
  return undefined
}

export const recordDenseArrayPosition = (
  node: OverlayNode,
  groups: Map<OverlayNode, DenseCandidates>,
): void => {
  const position = denseArrayPosition(node)
  if (position === undefined) return
  let candidates = groups.get(position.array)
  if (candidates === undefined) {
    const base = nodeBase(position.array)
    candidates = denseCandidates(isArray(base) ? base.length : 0)
    groups.set(position.array, candidates)
  }
  addDenseCandidate(candidates, position.index)
}

/** The run starting at `start`: it ends at the second consecutive unchanged entry. */
const regionAt = (bits: Uint8Array, start: number) => {
  let at = start
  let end = start
  let count = 0
  let gap = 0
  while (at < bits.length) {
    if (bits[at] === 0) {
      gap += 1
      if (gap > 1) break
    } else {
      count += 1
      end = at
      gap = 0
    }
    at += 1
  }
  return { at, count, length: end - start + 1 }
}

export const buildDenseRegions = (candidates: DenseCandidates): Array<DenseRegion> => {
  const bits = candidates.bits
  if (bits === undefined) return []
  const regions: Array<DenseRegion> = []
  for (let at = 0; at < bits.length;) {
    while (at < bits.length && bits[at] === 0) at += 1
    if (at === bits.length) break
    const start = at
    const region = regionAt(bits, start)
    at = region.at
    if (region.count >= MIN_DENSE_EDITS && region.count * 2 >= region.length) {
      regions.push({ start, length: region.length })
    }
  }
  return regions
}

export const regionContaining = (regions: ReadonlyArray<DenseRegion>, index: number): boolean =>
  regions.some((region) => index >= region.start && index < region.start + region.length)

const startsWith = (path: Path, prefix: Path): boolean => {
  if (path.length <= prefix.length) return false
  for (let index = 0; index < prefix.length; index++) {
    if (path[index] !== prefix[index]) return false
  }
  return true
}

/** Whether an ancestor's dense region already carries the node at `path`. */
export const hasCoveringDenseRegion = (
  node: OverlayNode,
  path: Path,
  regions: ReadonlyMap<OverlayNode, ReadonlyArray<DenseRegion>>,
): boolean => {
  for (let parent = nodeParent(node); parent !== undefined; parent = nodeParent(parent)) {
    const covering = regions.get(parent)
    if (covering === undefined) continue
    const parentPath = resolvePath(parent)
    if (parentPath === undefined || !startsWith(path, parentPath)) continue
    const index = path[parentPath.length]
    if (typeof index === 'number' && regionContaining(covering, index)) return true
  }
  return false
}
