import { cloneNode } from './clone.ts'
import { createNode, nodeBase, RELEASED } from './context.ts'
import type { OverlayContext, TrackerHost } from './context.ts'
import { applyTrusted } from './apply.ts'
import { emitOperations } from './emit.ts'
import { equalTrustedJson } from '../../internal/json.ts'
import type { Container, Json } from '../../internal/json.ts'
import type { Op } from './ops.ts'

/** Creating, materializing, and releasing overlay contexts. */

/** Past this many nodes, releasing a context drops its references instead of clearing each node. */
const MAX_CLEARED_NODES = 4096

export const createContext = (
  tracker: WeakRef<TrackerHost>,
  owner: object,
  baseRevision: number,
  root: Container,
  replacement: boolean,
  base: Container,
): OverlayContext => {
  const context: OverlayContext = {
    owner,
    tracker,
    baseRevision,
    status: { value: replacement ? 'prepared' : 'open' },
    root: undefined,
    bases: [],
    stored: [],
    dirty: [],
    nodes: [],
    rawNodes: new WeakMap(),
    ops: undefined,
    replacement,
    replacementNoop: false,
    baseValue: base,
    overlayReleased: false,
    simpleObjectMaterialization: false,
    registryRef: undefined,
    problem: undefined,
  }
  context.root = createNode(context, root)
  return context
}

const ensureOperations = (context: OverlayContext): Array<Op> => {
  if (context.ops !== undefined) return context.ops
  if (context.replacement) {
    const base = context.tracker.deref()?.replacementBase(context)
    const value = nodeBase(context.root!)
    context.replacementNoop = base !== undefined && equalTrustedJson(base, value)
    context.ops = context.replacementNoop ? [] : [['r', value]]
  } else context.ops = context.dirty.length === 0 ? [] : emitOperations(context)
  return context.ops
}

/** Drop the context's strong overlay references in O(1); retained handles read as settled. */
export const releaseOverlayReferences = (context: OverlayContext): void => {
  context.tracker.deref()?.releaseContext(context)
  context.overlayReleased = true
  context.dirty.length = 0
  context.nodes.length = 0
  context.rawNodes = undefined
  context.ops = undefined
  context.root = undefined
  context.bases = undefined
  context.stored = undefined
  context.baseValue = undefined
}

/** Release the context and every node it created, so retained handles hold nothing. */
export const clearContext = (context: OverlayContext): void => {
  for (const node of context.nodes) {
    if (Array.isArray(node.target)) node.target.length = 0
    node.writes?.clear()
    node.deletes?.clear()
    node.readded?.clear()
    if (node.array !== undefined) {
      if (node.array.pieces !== undefined) node.array.pieces.length = 0
      node.array.root = undefined
      node.array.baseLocations = undefined
      node.array.insertLocations = undefined
      node.array.baseOverrides?.clear()
      node.array.insertOverrides?.clear()
    }
    node.parentSource = undefined
    node.target = RELEASED
    node.proxy = RELEASED
    node.writeKey = undefined
    node.writeValue = undefined
    node.writes = undefined
    node.deleteKey = undefined
    node.deletes = undefined
    node.readded = undefined
    node.array = undefined
    node.subtreeDirty = false
    node.preparedPath = undefined
  }
  releaseOverlayReferences(context)
}

/**
 * Emit the change's ops and materialize its candidate. Operation payloads are already detached
 * from the overlay; the candidate and the batch share them under the immutable-transfer contract.
 */
export const materialize = (context: OverlayContext): { value: Json; ops: Array<Op> } => {
  const ops = ensureOperations(context)
  const base = context.baseValue!
  let value: Json = base
  if (ops.length > 0) {
    value = context.simpleObjectMaterialization ? cloneNode(context.root!) : applyTrusted(base, ops)
  }
  if (context.nodes.length > MAX_CLEARED_NODES) releaseOverlayReferences(context)
  else clearContext(context)
  return { value, ops }
}

export const abortContext = (context: OverlayContext): void => {
  const status = context.status.value
  if (status === 'aborted' || status === 'consumed' || status === 'stale') return
  context.status.value = 'aborted'
  clearContext(context)
}
