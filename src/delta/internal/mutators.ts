import {
  arrayOverlay,
  clampIndex,
  insertPiece,
  insertPlacementPiece,
  replacePieceRange,
  setArrayLength,
  toIntegerOrInfinity,
} from './arrays.ts'
import { clonePlacement } from './clone.ts'
import {
  isSettledContext,
  isWritable,
  markDirty,
  nodeBase,
  nodeForTarget,
  reject,
} from './context.ts'
import type { OverlayNode } from './context.ts'
import { getArrayIndex } from './handler.ts'
import { Invalid, isArray, isContainer } from './json.ts'
import type { Json } from './json.ts'
import { arrayLength, piecesOf, replaceAllPieces } from './pieces.ts'
import { sort } from './sort.ts'

/**
 * The array mutators of a draft: `push`, `pop`, `shift`, `unshift`, `splice`, `reverse`, `sort`,
 * `fill`, and `copyWithin`, edited as pieces. Borrowed onto another receiver, or called on a
 * settled handle, each runs the native method, so a settled handle refuses it like any write.
 */

type Method = (this: unknown, ...args: Array<unknown>) => unknown
type Body = (node: OverlayNode, args: Array<unknown>) => unknown

/** The node a mutator edits, or `undefined` when the native method should run instead. */
const receiverNode = (receiver: unknown): OverlayNode | undefined => {
  const node = isContainer(receiver) ? nodeForTarget(receiver) : undefined
  if (node === undefined || isSettledContext(node.context) || !isWritable(node.context)) {
    return undefined
  }
  return node
}

const method = (name: keyof typeof Array.prototype & string, body: Body): Method =>
  function (this: unknown, ...args: Array<unknown>): unknown {
    const node = receiverNode(this)
    if (node === undefined) {
      const native: unknown = Reflect.get(Array.prototype, name)
      return typeof native === 'function' ? Reflect.apply(native, this, args) : undefined
    }
    if (!isArray(nodeBase(node))) {
      reject(node.context, 'Array mutator called on incompatible receiver')
      return undefined
    }
    return body(node, args)
  }

const lengthOf = (node: OverlayNode): number => arrayLength(arrayOverlay(node))

const insertAt = (node: OverlayNode, index: number, items: Array<unknown>): boolean => {
  const pieces = insertPlacementPiece(node.context, items)
  if (pieces instanceof Invalid) return reject(node.context, pieces.message) && false
  replacePieceRange(node, index, 0, pieces)
  return true
}

const removeAt = (node: OverlayNode, index: (length: number) => number): unknown => {
  const length = lengthOf(node)
  if (length === 0) return undefined
  const at = index(length)
  const value = getArrayIndex(node, at)
  replacePieceRange(node, at, 1, [])
  return value
}

const spliceRemoveCount = (args: Array<unknown>, length: number, start: number): number => {
  if (args.length === 0) return 0
  if (args.length === 1) return length - start
  return Math.min(Math.max(toIntegerOrInfinity(args[1]), 0), length - start)
}

const splice: Body = (node, args) => {
  const length = lengthOf(node)
  const start = args.length === 0 ? 0 : clampIndex(toIntegerOrInfinity(args[0]), length)
  const remove = spliceRemoveCount(args, length, start)
  const removed = Array.from({ length: remove }, (_, offset) => getArrayIndex(node, start + offset))
  const pieces = insertPlacementPiece(node.context, args, Math.min(2, args.length))
  if (pieces instanceof Invalid) {
    reject(node.context, pieces.message)
    return []
  }
  replacePieceRange(node, start, remove, pieces)
  setArrayLength(node, length - remove + Math.max(args.length - 2, 0))
  return removed
}

const reverse: Body = (node) => {
  const overlay = arrayOverlay(node)
  if (arrayLength(overlay) < 2) return node.proxy
  const pieces = piecesOf(overlay).toReversed()
  for (const piece of pieces) {
    piece.start += piece.step * (piece.length - 1)
    piece.step = piece.step === 1 ? -1 : 1
  }
  replaceAllPieces(overlay, pieces)
  overlay.structural = true
  overlay.generation += 1
  overlay.plan = undefined
  markDirty(node)
  return node.proxy
}

/** Replace `[start, start + values.length)` with copies of `values`. */
const overwrite = (node: OverlayNode, start: number, values: ReadonlyArray<unknown>): unknown => {
  const items: Array<Json> = []
  for (const value of values) {
    const copied = clonePlacement(value)
    if (copied instanceof Invalid) {
      reject(node.context, copied.message)
      return node.proxy
    }
    items.push(copied)
  }
  replacePieceRange(node, start, items.length, insertPiece(node.context, items))
  return node.proxy
}

const endIndex = (value: unknown, length: number): number =>
  value === undefined ? length : clampIndex(toIntegerOrInfinity(value), length)

const fill: Body = (node, [supplied, startArg, endArg]) => {
  const length = lengthOf(node)
  const start = startArg === undefined ? 0 : clampIndex(toIntegerOrInfinity(startArg), length)
  const end = endIndex(endArg, length)
  if (end <= start) return node.proxy
  return overwrite(
    node,
    start,
    Array.from({ length: end - start }, () => supplied),
  )
}

const copyWithin: Body = (node, [targetArg, startArg, endArg]) => {
  const length = lengthOf(node)
  const target = clampIndex(toIntegerOrInfinity(targetArg), length)
  const start = clampIndex(toIntegerOrInfinity(startArg), length)
  const end = endIndex(endArg, length)
  const count = Math.min(Math.max(end - start, 0), length - target)
  const values = Array.from({ length: count }, (_, offset) => getArrayIndex(node, start + offset))
  return overwrite(node, target, values)
}

const MUTATORS: ReadonlyMap<PropertyKey, Method> = new Map<PropertyKey, Method>([
  [
    'push',
    method('push', (node, items) => {
      const length = lengthOf(node)
      return insertAt(node, length, items) ? length + items.length : length
    }),
  ],
  ['pop', method('pop', (node) => removeAt(node, (length) => length - 1))],
  ['shift', method('shift', (node) => removeAt(node, () => 0))],
  [
    'unshift',
    method('unshift', (node, items) => {
      insertAt(node, 0, items)
      return lengthOf(node)
    }),
  ],
  ['splice', method('splice', splice)],
  ['reverse', method('reverse', reverse)],
  ['sort', method('sort', (node, [comparator]) => sort(node, comparator))],
  ['fill', method('fill', fill)],
  ['copyWithin', method('copyWithin', copyWithin)],
])

/** The draft method an array property names, if it is a mutator. */
export const arrayMutator = (property: PropertyKey): Method | undefined => MUTATORS.get(property)
