import { defineData, emptyLike, isArray, isContainer } from '../../internal/json.ts'
import type { Container, Json } from '../../internal/json.ts'
import type { Op, Path, Segment } from './ops.ts'

/**
 * Replay of the tracker's own ops onto an immutable base, after Chord's `applyImmutableTrusted`
 * and `applyImmutable`. Each touched container is copied once; untouched subtrees stay shared
 * with the base. The ops come from the tracker, so they are not validated again.
 *
 * Batches with splices or permutations use native `splice` and copy an appended array once with
 * `concat`, which is faster for large structural batches; others splice with `copyWithin`.
 */

type MutableContainer = Array<Json> | { [key: string]: Json }
type ArrayOp = Extract<Op, readonly ['p' | 'm', ...ReadonlyArray<unknown>]>
type SpliceOp = Extract<Op, readonly ['p', ...ReadonlyArray<unknown>]>
type LeafOp = Extract<Op, readonly ['s' | 'd' | 'a' | 't', ...ReadonlyArray<unknown>]>
type Copied = { readonly root: Json; readonly target: MutableContainer }

interface Replay {
  readonly owned: WeakSet<object>
  readonly native: boolean
}

const CHUNK = 10_000

const read = (target: Container, key: Segment): Json =>
  isArray(target) ? target[Number(key)]! : target[String(key)]!

const isOwned = (owned: WeakSet<object>, value: Json): value is MutableContainer =>
  isContainer(value) && owned.has(value)

const shallowCopy = (value: Container): MutableContainer => {
  if (isArray(value)) return [...value]
  const result: { [key: string]: Json } = emptyLike(value)
  for (const key of Object.keys(value)) defineData(result, key, value[key]!)
  return result
}

const own = (owned: WeakSet<object>, value: Container): MutableContainer => {
  const copy = shallowCopy(value)
  owned.add(copy)
  return copy
}

/** Copy the root and the containers along the first `length` segments of `path`. */
const copyPath = (
  root: Json,
  path: Path,
  length: number,
  owned: WeakSet<object>,
): Copied | undefined => {
  if (!isContainer(root)) return undefined
  const copiedRoot = isOwned(owned, root) ? root : own(owned, root)
  let destination: MutableContainer = copiedRoot
  for (let index = 0; index < length; index++) {
    const segment = path[index]!
    const child = read(destination, segment)
    if (!isContainer(child)) return undefined
    if (isOwned(owned, child)) {
      destination = child
      continue
    }
    const copy = own(owned, child)
    defineData(destination, segment, copy)
    destination = copy
  }
  return { root: copiedRoot, target: destination }
}

const spliceCopyWithin = (
  target: Array<Json>,
  start: number,
  remove: number,
  items: ReadonlyArray<Json>,
) => {
  const length = target.length
  const delta = items.length - remove
  if (delta > 0) {
    target.length = length + delta
    target.copyWithin(start + items.length, start + remove, length)
  } else if (delta < 0) {
    target.copyWithin(start + items.length, start + remove, length)
    target.length = length + delta
  }
  for (let index = 0; index < items.length; index++) {
    defineData(target, start + index, items[index]!)
  }
}

const spliceNative = (
  target: Array<Json>,
  start: number,
  remove: number,
  items: ReadonlyArray<Json>,
) => {
  target.splice(start, remove)
  for (let offset = 0; offset < items.length; offset += CHUNK) {
    target.splice(start + offset, 0, ...items.slice(offset, offset + CHUNK))
  }
}

const permute = (target: Array<Json>, permutation: ReadonlyArray<number>): void => {
  const previous = [...target]
  for (let index = 0; index < permutation.length; index++) {
    target[index] = previous[permutation[index]!]!
  }
}

/**
 * An append to an array not yet copied in this replay, built as one copy with the items instead
 * of a copy that the insert then grows. `undefined` for any other splice.
 */
const appendCopy = (root: Json, op: SpliceOp, owned: WeakSet<object>): Json | undefined => {
  const [, path, index, remove, items] = op
  if (remove !== 0) return undefined
  let target: Json = root
  for (const segment of path) {
    if (!isContainer(target) || !Object.hasOwn(target, segment)) return undefined
    target = read(target, segment)
  }
  if (!isContainer(target) || !isArray(target) || owned.has(target) || index !== target.length) {
    return undefined
  }
  const appended = [...target, ...items]
  owned.add(appended)
  if (path.length === 0) return appended
  const copied = copyPath(root, path, path.length - 1, owned)
  if (copied === undefined) return undefined
  defineData(copied.target, path.at(-1)!, appended)
  return copied.root
}

const applyArrayOp = (root: Json, op: ArrayOp, replay: Replay): Json => {
  if (op[0] === 'p' && replay.native) {
    const appended = appendCopy(root, op, replay.owned)
    if (appended !== undefined) return appended
  }
  const copied = copyPath(root, op[1], op[1].length, replay.owned)
  if (copied === undefined || !Array.isArray(copied.target)) return root
  if (op[0] === 'm') permute(copied.target, op[2])
  else if (replay.native) spliceNative(copied.target, op[2], op[3], op[4])
  else spliceCopyWithin(copied.target, op[2], op[3], op[4])
  return copied.root
}

const deleteEntry = (target: MutableContainer, key: Segment): void => {
  if (Array.isArray(target)) target.splice(Number(key), 1)
  else Reflect.deleteProperty(target, key)
}

const applyLeafOp = (root: Json, op: LeafOp, replay: Replay): Json => {
  const path = op[1]
  const copied = copyPath(root, path, path.length - 1, replay.owned)
  if (copied === undefined) return root
  const key = path.at(-1)!
  if (op[0] === 's') defineData(copied.target, key, op[2])
  else if (op[0] === 'd') deleteEntry(copied.target, key)
  else {
    const current = read(copied.target, key)
    if (typeof current !== 'string') return copied.root
    defineData(copied.target, key, op[0] === 'a' ? `${current}${op[2]}` : current.slice(op[2]))
  }
  return copied.root
}

const isArrayOp = (op: Op): op is ArrayOp => op[0] === 'p' || op[0] === 'm'

/** Apply the tracker's own ops to `base` without mutating it. */
export const applyTrusted = (base: Json, operations: ReadonlyArray<Op>): Json => {
  const replay: Replay = { owned: new WeakSet(), native: operations.some((op) => isArrayOp(op)) }
  let root = base
  for (const op of operations) {
    if (op[0] === 'r') root = op[1]
    else root = isArrayOp(op) ? applyArrayOp(root, op, replay) : applyLeafOp(root, op, replay)
  }
  return root
}
