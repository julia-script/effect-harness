import { Effect } from 'effect'
import type { Op } from '../schema/Document.ts'
import type { JsonObject } from '../schema/Json.ts'
import { StorageError } from '../StorageError.ts'

/**
 * Document delta replay for storage backends, after pi's `chord/delta` applier.
 *
 * Ops are data, so every op is validated before use: unknown verbs, wrong arities, and path
 * segments that reach the prototype chain are rejected, and writes use `defineProperty` so an
 * inherited setter never runs. The helpers below report a problem as a message; `applyBatches`
 * turns the first one into a `StorageError`.
 */

type Segment = string | number
type Path = ReadonlyArray<Segment>
type Container = unknown[] | Record<string, unknown>
type ArrayOp = Extract<Op, readonly ['p' | 'm', ...ReadonlyArray<unknown>]>
type LeafOp = Extract<Op, readonly ['s' | 'd' | 'a' | 't', ...ReadonlyArray<unknown>]>
type StringOp = Extract<Op, readonly ['a' | 't', ...ReadonlyArray<unknown>]>
/** Why an op could not be applied; `undefined` when it was. */
type Problem = string | undefined

interface Shape {
  readonly arity: number
  /** Whether the path must be non-empty; absent for `r`, which has no path. */
  readonly nonEmptyPath?: boolean
  readonly check?: (op: ReadonlyArray<unknown>) => boolean
}

/** Segments that would reach the prototype chain through `parent[key] = value`. */
const RESERVED_SEGMENTS: ReadonlySet<string> = new Set(['__proto__', 'constructor', 'prototype'])

const isContainer = (value: unknown): value is Container =>
  value !== null && typeof value === 'object'

const isNonNegativeInt = (value: unknown): value is number =>
  typeof value === 'number' && Number.isInteger(value) && value >= 0

export const isJsonObject = (value: unknown): value is JsonObject =>
  isContainer(value) && !Array.isArray(value)

const unresolvable = (path: Path): string => `Unresolvable document path: ${JSON.stringify(path)}`

const define = (target: Container, key: Segment, value: unknown): void => {
  Object.defineProperty(target, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  })
}

const pathProblem = (path: unknown, nonEmpty: boolean): Problem => {
  if (!Array.isArray(path)) return 'Document op path is not an array'
  if (nonEmpty && path.length === 0) return 'Document op path is empty'
  const segments: ReadonlyArray<unknown> = path
  const unsafe = segments.find((segment) =>
    typeof segment === 'string' ? RESERVED_SEGMENTS.has(segment) : !isNonNegativeInt(segment),
  )
  return unsafe === undefined
    ? undefined
    : `Unsafe document path segment: ${JSON.stringify(unsafe)}`
}

const isPermutation = (value: unknown): boolean => {
  if (!Array.isArray(value)) return false
  const seen = new Uint8Array(value.length)
  for (const index of value) {
    if (!isNonNegativeInt(index) || index >= value.length || seen[index] !== 0) return false
    seen[index] = 1
  }
  return true
}

/** Arity, path, and payload rules of each decoded verb. */
const SHAPES: ReadonlyMap<unknown, Shape> = new Map<string, Shape>([
  ['r', { arity: 2 }],
  ['s', { arity: 3, nonEmptyPath: true }],
  ['d', { arity: 2, nonEmptyPath: true }],
  ['a', { arity: 3, nonEmptyPath: true, check: (op) => typeof op[2] === 'string' }],
  ['t', { arity: 3, nonEmptyPath: true, check: (op) => isNonNegativeInt(op[2]) }],
  [
    'p',
    {
      arity: 5,
      nonEmptyPath: false,
      check: (op) => isNonNegativeInt(op[2]) && isNonNegativeInt(op[3]) && Array.isArray(op[4]),
    },
  ],
  ['m', { arity: 3, nonEmptyPath: false, check: (op) => isPermutation(op[2]) }],
])

/** Verb, arity, and payload shape of a decoded op. */
const opProblem = (op: ReadonlyArray<unknown>): Problem => {
  const verb = String(op[0])
  const shape = SHAPES.get(op[0])
  // Silently skipping an unknown verb is how a newer producer's op vanishes.
  if (shape === undefined) return `Unknown document op verb: ${verb}`
  if (op.length !== shape.arity) return `Document op ${verb} arity`
  const path = shape.nonEmptyPath === undefined ? undefined : pathProblem(op[1], shape.nonEmptyPath)
  if (path !== undefined) return path
  return shape.check === undefined || shape.check(op) ? undefined : `Document op ${verb} shape`
}

/** Walk own properties only, so a path never escapes into the prototype chain. */
const resolve = (root: unknown, path: Path): Container | undefined => {
  let node = root
  for (const segment of path) {
    if (!isContainer(node) || (Array.isArray(node) && typeof segment !== 'number')) return undefined
    if (!Object.hasOwn(node, segment)) return undefined
    const child: unknown = Reflect.get(node, segment)
    node = child
  }
  return isContainer(node) ? node : undefined
}

const applyArrayOp = (root: unknown, op: ArrayOp): Problem => {
  const target = resolve(root, op[1])
  if (!Array.isArray(target)) return unresolvable(op[1])
  if (op[0] === 'p') {
    target.splice(op[2], op[3], ...structuredClone(op[4]))
    return undefined
  }
  const permutation = op[2]
  if (target.length !== permutation.length) return unresolvable(op[1])
  const previous = [...target]
  for (const [index, from] of permutation.entries()) target[index] = previous[from]
  return undefined
}

const applyStringOp = (parent: Container, key: Segment, op: StringOp): Problem => {
  const current: unknown = Object.hasOwn(parent, key) ? Reflect.get(parent, key) : undefined
  if (typeof current !== 'string') return unresolvable(op[1])
  define(parent, key, op[0] === 'a' ? `${current}${op[2]}` : current.slice(op[2]))
  return undefined
}

const applyLeafOp = (root: unknown, op: LeafOp): Problem => {
  const path = op[1]
  const parent = resolve(root, path.slice(0, -1))
  const key = path.at(-1) ?? ''
  // Writing past the end would leave a hole that does not survive a JSON round trip.
  const inRange = !Array.isArray(parent) || (typeof key === 'number' && key <= parent.length)
  if (parent === undefined || !inRange) return unresolvable(path)
  if (op[0] === 's') {
    define(parent, key, structuredClone(op[2]))
    return undefined
  }
  if (op[0] !== 'd') return applyStringOp(parent, key, op)
  if (!Array.isArray(parent)) {
    Reflect.deleteProperty(parent, key)
    return undefined
  }
  if (typeof key !== 'number' || key >= parent.length) return unresolvable(path)
  parent.splice(key, 1)
  return undefined
}

const isArrayOp = (op: Op): op is ArrayOp => op[0] === 'p' || op[0] === 'm'

/**
 * Replay op batches on a detached copy of `base`. Neither the base nor any op payload is shared
 * with the result, so callers may hand it out without cloning again.
 */
export const applyBatches = Effect.fnUntraced(function* (
  base: JsonObject,
  batches: Iterable<ReadonlyArray<Op>>,
) {
  let root: unknown = structuredClone(base)
  for (const ops of batches) {
    for (const op of ops) {
      const invalid = opProblem(op)
      if (invalid !== undefined) return yield* new StorageError({ message: invalid })
      if (op[0] === 'r') {
        root = structuredClone(op[1])
        continue
      }
      const problem = isArrayOp(op) ? applyArrayOp(root, op) : applyLeafOp(root, op)
      if (problem !== undefined) return yield* new StorageError({ message: problem })
    }
  }
  return root
})
