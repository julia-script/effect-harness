import { arrayOverlay, baseElement, entryValueAt, located } from './arrays.ts'
import { cloneStored } from './clone.ts'
import { nodeBase, storedValue } from './context.ts'
import type { OverlayNode, StoredRef } from './context.ts'
import { regionContaining } from './dense.ts'
import type { DenseRegion } from './dense.ts'
import { equalTrustedJson, isContainer } from './json.ts'
import type { Json } from './json.ts'
import { baseObject, objectValue } from './objects.ts'
import { childPath, overlap } from './ops.ts'
import type { NonEmptyPath, Op, Path } from './ops.ts'
import { findEntryIndex } from './paths.ts'
import { locatePiece, piecesOf } from './pieces.ts'
import { buildArrayPlan } from './plan.ts'

/** The ops of one dirty node. */

/** Past this many ops a batch folds into one replacement of the root. */
export const MAX_DELTA_OPERATIONS = 4096

/** Emit the op that turns `before` into `after`, if they differ. Returns whether it emitted. */
export const emitChangedValue = (
  operations: Array<Op>,
  path: NonEmptyPath,
  before: Json | undefined,
  after: Json,
): boolean => {
  if (!isContainer(after) && before === after) return false
  if (isContainer(before) && isContainer(after) && equalTrustedJson(before, after)) return false
  if (typeof before === 'string' && typeof after === 'string') {
    // Slice and equality stay on V8's rope fast path; startsWith scans the entire accumulated
    // prefix on every append.
    if (after.length > before.length && after.slice(0, before.length) === before) {
      operations.push(['a', path, after.slice(before.length)])
      return true
    }
    const shared = overlap(before, after, 65_536)
    if (shared > 0) {
      operations.push(['t', path, before.length - shared])
      if (after.length > shared) operations.push(['a', path, after.slice(shared)])
      return true
    }
  }
  operations.push(['s', path, after])
  return true
}

/** Returns whether a container write normalized away, which rules out direct materialization. */
const emitObjectWrite = (
  node: OverlayNode,
  path: Path,
  operations: Array<Op>,
  key: string,
  value: Json,
): boolean => {
  const base = baseObject(node)
  const before =
    node.readded?.has(key) === true || !Object.hasOwn(base, key) ? undefined : base[key]
  const after = cloneStored(value, node.context)
  const emitted = emitChangedValue(operations, childPath(path, key), before, after)
  return isContainer(before) && isContainer(after) && !emitted
}

const emitDeletion = (node: OverlayNode, path: Path, operations: Array<Op>, key: string) => {
  if (Object.hasOwn(nodeBase(node), key)) operations.push(['d', childPath(path, key)])
}

/**
 * Emit an object node's writes and deletions. Returns whether a deeply equal container write
 * normalized away, so the candidate cannot be cloned from the draft directly.
 */
export const emitObjectOperations = (
  node: OverlayNode,
  path: Path,
  operations: Array<Op>,
): boolean => {
  // Immutable replay must encode delete-and-readd explicitly so string-key insertion order
  // matches the draft.
  if (node.readded !== undefined) {
    for (const key of node.readded) emitDeletion(node, path, operations, key)
  }
  let normalized = false
  const write = (key: string, value: Json): void => {
    if (emitObjectWrite(node, path, operations, key, value)) normalized = true
  }
  if (node.writeKey !== undefined) write(node.writeKey, objectValue(node, node.writeKey))
  if (node.writes !== undefined) {
    for (const [key, reference] of node.writes) {
      if (operations.length > MAX_DELTA_OPERATIONS) return normalized
      write(key, storedValue(node.context, reference))
    }
  }
  if (node.deleteKey !== undefined) emitDeletion(node, path, operations, node.deleteKey)
  if (node.deletes !== undefined) {
    for (const key of node.deletes) {
      if (operations.length > MAX_DELTA_OPERATIONS) return normalized
      emitDeletion(node, path, operations, key)
    }
  }
  return normalized
}

const cloneArrayRegion = (node: OverlayNode, region: DenseRegion): Array<Json> => {
  const overlay = arrayOverlay(node)
  const result: Array<Json> = []
  for (let index = region.start; index < region.start + region.length; index++) {
    const piece = locatePiece(overlay, index)
    result.push(cloneStored(entryValueAt(node, piece, located(overlay, piece)), node.context))
  }
  return result
}

const insertedItems = (node: OverlayNode, startPiece: number, endPiece: number): Array<Json> => {
  const pieces = piecesOf(arrayOverlay(node))
  const items: Array<Json> = []
  for (let pieceIndex = startPiece; pieceIndex < endPiece; pieceIndex++) {
    const piece = pieces[pieceIndex]!
    for (let offset = 0; offset < piece.length; offset++) {
      const value = entryValueAt(node, piece, piece.start + piece.step * offset)
      items.push(cloneStored(value, node.context))
    }
  }
  return items
}

const emitStructure = (node: OverlayNode, path: Path, operations: Array<Op>): void => {
  const plan = buildArrayPlan(node)
  for (let index = 0; index < plan.removeRuns.length; index += 2) {
    if (operations.length > MAX_DELTA_OPERATIONS) return
    operations.push(['p', path, plan.removeRuns[index]!, plan.removeRuns[index + 1]!, []])
  }
  if (plan.permutation !== undefined) operations.push(['m', path, plan.permutation])
  for (let run = 0; run < plan.insertRuns.length; run += 3) {
    if (operations.length > MAX_DELTA_OPERATIONS) return
    const items = insertedItems(node, plan.insertRuns[run + 1]!, plan.insertRuns[run + 2]!)
    operations.push(['p', path, plan.insertRuns[run]!, 0, items])
  }
}

const emitOverride = (
  node: OverlayNode,
  path: Path,
  operations: Array<Op>,
  regions: ReadonlyArray<DenseRegion> | undefined,
  [baseIndex, reference]: [number, StoredRef],
): void => {
  const index = findEntryIndex(arrayOverlay(node), 1, baseIndex)
  if (index === undefined || (regions !== undefined && regionContaining(regions, index))) return
  const after = cloneStored(storedValue(node.context, reference), node.context)
  emitChangedValue(operations, childPath(path, index), baseElement(node, baseIndex), after)
}

export const emitArrayOperations = (
  node: OverlayNode,
  path: Path,
  operations: Array<Op>,
  regions: ReadonlyArray<DenseRegion> | undefined,
): void => {
  const overlay = arrayOverlay(node)
  for (const region of regions ?? []) {
    if (operations.length > MAX_DELTA_OPERATIONS) return
    operations.push(['p', path, region.start, region.length, cloneArrayRegion(node, region)])
  }
  if (overlay.structural) emitStructure(node, path, operations)
  for (const entry of overlay.baseOverrides ?? []) {
    if (operations.length > MAX_DELTA_OPERATIONS) return
    emitOverride(node, path, operations, regions, entry)
  }
}
