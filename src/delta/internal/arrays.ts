import { clonePlacement } from './clone.ts'
import {
  markDirty,
  nodeBase,
  releaseStoredValue,
  replaceStoredValue,
  storedValue,
  storeValue,
} from './context.ts'
import type {
  ArrayOverlay,
  InsertPiece,
  InsertSource,
  OverlayContext,
  OverlayNode,
  Piece,
  StoredRef,
} from './context.ts'
import { Invalid, isArray, isContainer } from './json.ts'
import type { Json, JsonArray } from './json.ts'
import {
  arrayLength,
  createPieceNode,
  extendRightmostPiece,
  invalidatePieceCaches,
  joinNormalized,
  locatePiece,
  rightmost,
  splitPieceTree,
  treeFromPieces,
} from './pieces.ts'

/** Array overlays: reads through the piece table, and the edits that rewrite it. */

const baseArray = (node: OverlayNode): JsonArray => {
  const base = nodeBase(node)
  return isArray(base) ? base : []
}

export const arrayOverlay = (node: OverlayNode): ArrayOverlay => {
  if (node.array !== undefined) return node.array
  const base = baseArray(node)
  const overlay: ArrayOverlay = {
    root: undefined,
    pieces: undefined,
    baseOverrides: undefined,
    insertOverrides: undefined,
    structural: false,
    generation: 0,
    plan: undefined,
    seed: 0x9e_37_79_b9,
    locatedOffset: 0,
    baseLocations: undefined,
    insertLocations: undefined,
  }
  if (base.length > 0) {
    overlay.root = createPieceNode(overlay, {
      kind: 'base',
      start: 0,
      length: base.length,
      step: 1,
    })
  }
  node.array = overlay
  return overlay
}

/** The base element at `index` of an array node. */
export const baseElement = (node: OverlayNode, index: number): Json => baseArray(node)[index]!

export const baseOverridesForWrite = (overlay: ArrayOverlay): Map<number, StoredRef> => {
  overlay.baseOverrides ??= new Map()
  return overlay.baseOverrides
}

export const insertOverridesFor = (overlay: ArrayOverlay, source: InsertSource) => {
  overlay.insertOverrides ??= new Map()
  let overrides = overlay.insertOverrides.get(source)
  if (overrides === undefined) {
    overrides = new Map()
    overlay.insertOverrides.set(source, overrides)
  }
  return overrides
}

/** The source index of logical `index`, after `locatePiece` returned its piece. */
export const located = (overlay: ArrayOverlay, piece: Piece): number =>
  piece.start + piece.step * overlay.locatedOffset

export const entryValueAt = (node: OverlayNode, piece: Piece, sourceIndex: number): Json => {
  const overlay = arrayOverlay(node)
  if (piece.kind === 'base') {
    const reference = overlay.baseOverrides?.get(sourceIndex)
    return reference === undefined
      ? baseElement(node, sourceIndex)
      : storedValue(node.context, reference)
  }
  // Overrides may hold `null`, so presence is checked with `has`.
  const overrides = overlay.insertOverrides?.get(piece.source)
  const reference =
    overrides?.has(sourceIndex) === true
      ? overrides.get(sourceIndex)!
      : piece.source.refs[sourceIndex]!
  return storedValue(node.context, reference)
}

export const hasEntryOverrideAt = (overlay: ArrayOverlay, piece: Piece, sourceIndex: number) =>
  piece.kind === 'base'
    ? (overlay.baseOverrides?.has(sourceIndex) ?? false)
    : (overlay.insertOverrides?.get(piece.source)?.has(sourceIndex) ?? false)

const structuralChange = (node: OverlayNode, overlay: ArrayOverlay): void => {
  invalidatePieceCaches(overlay)
  overlay.structural = true
  overlay.generation += 1
  markDirty(node)
}

/** Append into the tail's own source when the tail is the end of an insert run. */
const appendToTail = (node: OverlayNode, overlay: ArrayOverlay, addition: Piece): boolean => {
  if (overlay.root === undefined || addition.kind !== 'insert') return false
  const tail = rightmost(overlay.root).piece
  if (tail.kind !== 'insert' || tail.step !== 1) return false
  if (tail.start + tail.length !== tail.source.refs.length) return false
  for (let offset = 0; offset < addition.length; offset++) {
    tail.source.refs.push(addition.source.refs[addition.start + offset]!)
  }
  extendRightmostPiece(overlay.root, addition.length)
  structuralChange(node, overlay)
  return true
}

/** Replace `remove` logical elements at `index` with the `inserted` pieces. */
export const replacePieceRange = (
  node: OverlayNode,
  index: number,
  remove: number,
  inserted: Array<Piece>,
): void => {
  if (remove === 0 && inserted.length === 0) return
  const overlay = arrayOverlay(node)
  const append = remove === 0 && index === arrayLength(overlay) && inserted.length === 1
  if (append && appendToTail(node, overlay, inserted[0]!)) return
  const [left, rest] = splitPieceTree(overlay, overlay.root, index)
  const [, right] = splitPieceTree(overlay, rest, remove)
  const middle = treeFromPieces(overlay, inserted)
  overlay.root = joinNormalized(overlay, joinNormalized(overlay, left, middle), right)
  structuralChange(node, overlay)
}

/** One insert piece over already-copied values. */
export const insertPiece = (
  context: OverlayContext,
  items: ReadonlyArray<Json>,
  start = 0,
): Array<InsertPiece> => {
  if (items.length === start) return []
  const refs: Array<StoredRef> = []
  for (let index = start; index < items.length; index++) {
    refs.push(storeValue(context, items[index]!))
  }
  return [{ kind: 'insert', source: { refs }, start: 0, length: refs.length, step: 1 }]
}

/**
 * One insert piece over copies of supplied values, reusing `values` as its source, or the first
 * value that is not strict JSON, in which case nothing was stored.
 */
export const insertPlacementPiece = (
  context: OverlayContext,
  values: Array<unknown>,
  start = 0,
): Array<InsertPiece> | Invalid => {
  if (values.length === start) return []
  const storedLength = context.stored!.length
  const refs: Array<StoredRef> = []
  for (let index = 0; index < start; index++) refs.push(null)
  for (let index = start; index < values.length; index++) {
    const copied = clonePlacement(values[index])
    if (copied instanceof Invalid) {
      context.stored!.length = storedLength
      return copied
    }
    refs.push(storeValue(context, copied))
  }
  return [{ kind: 'insert', source: { refs }, start, length: values.length - start, step: 1 }]
}

const overrideBase = (node: OverlayNode, sourceIndex: number, stored: Json): void => {
  const overlay = arrayOverlay(node)
  const existing = overlay.baseOverrides?.get(sourceIndex)
  if (!isContainer(stored) && stored === baseElement(node, sourceIndex)) {
    if (existing !== undefined) releaseStoredValue(node.context, existing)
    overlay.baseOverrides?.delete(sourceIndex)
  } else if (existing === undefined) {
    baseOverridesForWrite(overlay).set(sourceIndex, storeValue(node.context, stored))
  } else {
    overlay.baseOverrides!.set(sourceIndex, replaceStoredValue(node.context, existing, stored))
  }
}

const overrideInsert = (node: OverlayNode, piece: InsertPiece, index: number, stored: Json) => {
  const overlay = arrayOverlay(node)
  const overrides = overlay.insertOverrides?.get(piece.source)
  const existing = overrides?.get(index)
  if (!isContainer(stored) && stored === storedValue(node.context, piece.source.refs[index]!)) {
    if (existing !== undefined) releaseStoredValue(node.context, existing)
    overrides?.delete(index)
    if (overrides?.size === 0) overlay.insertOverrides?.delete(piece.source)
    return
  }
  const target = overrides ?? insertOverridesFor(overlay, piece.source)
  target.set(
    index,
    existing === undefined
      ? storeValue(node.context, stored)
      : replaceStoredValue(node.context, existing, stored),
  )
}

/** Set logical `index`, which is at most the length; writing at the length appends. */
export const setArrayIndex = (node: OverlayNode, index: number, stored: Json): void => {
  const overlay = arrayOverlay(node)
  const length = arrayLength(overlay)
  if (index === length) {
    replacePieceRange(node, length, 0, insertPiece(node.context, [stored]))
    return
  }
  const piece = locatePiece(overlay, index)
  const sourceIndex = located(overlay, piece)
  if (!isContainer(stored) && entryValueAt(node, piece, sourceIndex) === stored) return
  if (piece.kind === 'base') overrideBase(node, sourceIndex, stored)
  else overrideInsert(node, piece, sourceIndex, stored)
  markDirty(node)
}

/** Shrinking removes elements; growing appends `null`s. */
export const setArrayLength = (node: OverlayNode, next: number): void => {
  const current = arrayLength(arrayOverlay(node))
  if (next === current) return
  if (next < current) {
    replacePieceRange(node, next, current - next, [])
    return
  }
  const nulls = Array.from({ length: next - current }, () => null)
  replacePieceRange(node, current, 0, insertPiece(node.context, nulls))
}

/** The array index a property names, or `undefined` for any other key. */
export const arrayIndex = (property: PropertyKey): number | undefined => {
  if (typeof property !== 'string' || property.length === 0 || property.length > 10) {
    return undefined
  }
  if (property === '0') return 0
  const first = property.codePointAt(0) ?? 0
  if (first < 49 || first > 57) return undefined
  let index = first - 48
  for (let offset = 1; offset < property.length; offset++) {
    const digit = (property.codePointAt(offset) ?? 0) - 48
    if (digit < 0 || digit > 9) return undefined
    index = index * 10 + digit
    if (index >= 4_294_967_295) return undefined
  }
  return index
}

/** A valid array length, or `undefined`. */
export const toArrayLength = (value: unknown): number | undefined => {
  const number = Number(value)
  return Number.isInteger(number) && number >= 0 && number < 4_294_967_296 ? number : undefined
}

export const toIntegerOrInfinity = (value: unknown): number => {
  const number = Number(value)
  if (Number.isNaN(number) || number === 0) return 0
  return Number.isFinite(number) ? Math.trunc(number) : number
}

export const clampIndex = (value: number, length: number): number => {
  if (value === Number.NEGATIVE_INFINITY) return 0
  if (value < 0) return Math.max(length + value, 0)
  return Math.min(value, length)
}
