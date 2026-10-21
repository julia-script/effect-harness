import {
  arrayOverlay,
  baseElement,
  baseOverridesForWrite,
  entryValueAt,
  insertOverridesFor,
  insertPiece,
  located,
  replacePieceRange,
} from './arrays.ts'
import { clonePlacementStored } from './clone.ts'
import {
  createNode,
  markDirty,
  reject,
  releaseStoredValue,
  replaceStoredValue,
  storedValue,
  storeValue,
} from './context.ts'
import type { ArrayOverlay, InsertSource, OverlayNode, Piece } from './context.ts'
import { isContainer } from '../../internal/json.ts'
import type { Json } from '../../internal/json.ts'
import {
  appendMergedPiece,
  arrayLength,
  locatePiece,
  piecesOf,
  replaceAllPieces,
  singletonPiece,
} from './pieces.ts'

/**
 * `sort` over the piece table. Each element gets a token: its base index, or `-n` for the n-th
 * inserted entry. The comparator sees draft handles; the sorted tokens become pieces again.
 * Overrides are snapshotted first and restored after, so comparator writes keep their values.
 */

interface Tokens {
  readonly sources: Array<InsertSource>
  readonly indices: Array<number>
}

const tokenSource = (token: number, tokens: Tokens): InsertSource | undefined =>
  token < 0 ? tokens.sources[-token - 1] : undefined

const tokenIndex = (token: number, tokens: Tokens): number =>
  token < 0 ? tokens.indices[-token - 1]! : token

const overrideOf = (overlay: ArrayOverlay, source: InsertSource | undefined, index: number) =>
  source === undefined
    ? overlay.baseOverrides?.get(index)
    : overlay.insertOverrides?.get(source)?.get(index)

const tokenValue = (node: OverlayNode, source: InsertSource | undefined, index: number): Json => {
  const override = overrideOf(arrayOverlay(node), source, index)
  if (override !== undefined) return storedValue(node.context, override)
  return source === undefined
    ? baseElement(node, index)
    : storedValue(node.context, source.refs[index]!)
}

/** What the comparator sees for a token: the value, or a draft handle of it. */
const publicSortValue = (node: OverlayNode, token: number, tokens: Tokens): unknown => {
  const source = tokenSource(token, tokens)
  const index = tokenIndex(token, tokens)
  const value = tokenValue(node, source, index)
  if (!isContainer(value)) return value
  return createNode(node.context, value, node, {
    kind: source === undefined ? 1 : 2,
    key: index,
    source,
    placed: source !== undefined || (arrayOverlay(node).baseOverrides?.has(index) ?? false),
  }).proxy
}

type Snapshot = Map<InsertSource | undefined, Map<number, Json>>

const snapshotOverrides = (node: OverlayNode): Snapshot => {
  const overlay = arrayOverlay(node)
  const snapshot: Snapshot = new Map()
  const take = (source: InsertSource | undefined, overrides: Map<number, unknown> | undefined) => {
    if (overrides === undefined) return
    const values = new Map<number, Json>()
    for (const index of overrides.keys()) values.set(index, tokenValue(node, source, index))
    snapshot.set(source, values)
  }
  take(undefined, overlay.baseOverrides)
  for (const [source, overrides] of overlay.insertOverrides ?? []) take(source, overrides)
  return snapshot
}

const restoreOverride = (node: OverlayNode, token: number, tokens: Tokens, snapshot: Snapshot) => {
  const overlay = arrayOverlay(node)
  const source = tokenSource(token, tokens)
  const index = tokenIndex(token, tokens)
  const existing = overrideOf(overlay, source, index)
  const value = snapshot.get(source)?.get(index)
  if (value === undefined) {
    if (existing !== undefined) releaseStoredValue(node.context, existing)
    const overrides =
      source === undefined ? overlay.baseOverrides : overlay.insertOverrides?.get(source)
    overrides?.delete(index)
    if (source !== undefined && overrides?.size === 0) overlay.insertOverrides?.delete(source)
    return
  }
  const overrides =
    source === undefined ? baseOverridesForWrite(overlay) : insertOverridesFor(overlay, source)
  overrides.set(
    index,
    existing === undefined
      ? storeValue(node.context, value)
      : replaceStoredValue(node.context, existing, value),
  )
}

const hasOverrides = (overlay: ArrayOverlay): boolean =>
  (overlay.baseOverrides?.size ?? 0) > 0 || (overlay.insertOverrides?.size ?? 0) > 0

const sameTokenAt = (overlay: ArrayOverlay, at: number, token: number, tokens: Tokens) => {
  const piece = locatePiece(overlay, at)
  const source = tokenSource(token, tokens)
  return (
    located(overlay, piece) === tokenIndex(token, tokens) &&
    ((piece.kind === 'base' && source === undefined) ||
      (piece.kind === 'insert' && piece.source === source))
  )
}

const piecesFromOrder = (order: ReadonlyArray<number>, tokens: Tokens): Array<Piece> => {
  const pieces: Array<Piece> = []
  for (const token of order) {
    const source = tokenSource(token, tokens)
    const start = tokenIndex(token, tokens)
    appendMergedPiece(
      pieces,
      source === undefined
        ? { kind: 'base', start, length: 1, step: 1 }
        : { kind: 'insert', source, start, length: 1, step: 1 },
    )
  }
  return pieces
}

/** Replace every repeated entry, which a reentrant comparator can create, with a copy. */
const deduplicate = (node: OverlayNode): void => {
  const overlay = arrayOverlay(node)
  const seen = new Map<InsertSource | undefined, Set<number>>()
  const next: Array<Piece> = []
  let duplicated = false
  for (const piece of piecesOf(overlay)) {
    const source = piece.kind === 'insert' ? piece.source : undefined
    let indices = seen.get(source)
    if (indices === undefined) {
      indices = new Set()
      seen.set(source, indices)
    }
    for (let offset = 0; offset < piece.length; offset++) {
      const index = piece.start + piece.step * offset
      if (indices.has(index)) {
        duplicated = true
        const copy = clonePlacementStored(entryValueAt(node, piece, index), node.context)
        appendMergedPiece(next, insertPiece(node.context, [copy])[0]!)
      } else appendMergedPiece(next, singletonPiece(piece, index))
      indices.add(index)
    }
  }
  if (!duplicated) return
  replaceAllPieces(overlay, next)
  overlay.structural = true
  overlay.generation += 1
  overlay.plan = undefined
  markDirty(node)
}

const defaultOrder = (left: unknown, right: unknown): number => {
  const a = String(left)
  const b = String(right)
  if (a === b) return 0
  return a < b ? -1 : 1
}

/** Every element's token in current order, and what the comparator sees for each. */
const collect = (node: OverlayNode, tokens: Tokens) => {
  const order: Array<number> = []
  const baseValues: Array<unknown> = []
  const insertedValues: Array<unknown> = []
  for (const piece of piecesOf(arrayOverlay(node))) {
    for (let offset = 0; offset < piece.length; offset++) {
      const index = piece.start + piece.step * offset
      if (piece.kind === 'base') {
        order.push(index)
        baseValues[index] = publicSortValue(node, index, tokens)
      } else {
        tokens.sources.push(piece.source)
        tokens.indices.push(index)
        const token = -tokens.sources.length
        order.push(token)
        insertedValues.push(publicSortValue(node, token, tokens))
      }
    }
  }
  const valueOf = (token: number): unknown =>
    token < 0 ? insertedValues[-token - 1] : baseValues[token]
  return { order, valueOf }
}

export const sort = (node: OverlayNode, comparator: unknown): unknown => {
  if (comparator !== undefined && typeof comparator !== 'function') {
    reject(node.context, 'Comparator must be a function')
    return node.proxy
  }
  const overlay = arrayOverlay(node)
  const tokens: Tokens = { sources: [], indices: [] }
  const { order, valueOf } = collect(node, tokens)
  const snapshot = snapshotOverrides(node)
  const generation = overlay.generation
  const compare = (left: unknown, right: unknown): number => {
    if (typeof comparator !== 'function') return defaultOrder(left, right)
    const result: unknown = Reflect.apply(comparator, undefined, [left, right])
    return Number(result)
  }
  order.sort((left, right) => compare(valueOf(left), valueOf(right)))
  if (snapshot.size > 0 || hasOverrides(overlay)) {
    for (const token of order) restoreOverride(node, token, tokens, snapshot)
  }
  const reentrant = overlay.generation !== generation
  const length = arrayLength(overlay)
  const unchanged =
    length >= order.length &&
    order.every((token, index) => sameTokenAt(overlay, index, token, tokens))
  if (!unchanged) {
    replacePieceRange(node, 0, Math.min(order.length, length), piecesFromOrder(order, tokens))
  }
  if (reentrant) deduplicate(node)
  return node.proxy
}
