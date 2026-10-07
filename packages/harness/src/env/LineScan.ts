/**
 * Incremental byte scanning for exact text-line selection offsets.
 *
 * @since 0.0.0
 */
import { identity } from 'effect/Function'
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Effect from 'effect/Effect'
import * as Predicate from 'effect/Predicate'
// Adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
import { rangeDecoder, hasBom } from './Decode.ts'
import { FileError, FileInvalid, FileUnknown, type LineScan } from '../Env.ts'
import * as Result from 'effect/Result'
const NEWLINE = 10
const encoder = new TextEncoder()
const decodedBytes = (self: string): number => encoder.encode(self).length
const TypeId = '~@effect-harness/harness/env/LineScan'
/**
 * LineScan state contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface State extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: typeof TypeId
  readonly startLine: number
  readonly endLine: number
  position: number
  newlines: number
  lineStart: number
  start: number | undefined
  end: number | undefined
  firstLineEnd: number | undefined
  lastLineStart: number | undefined
  selectedBytes: number
  firstLineBytes: number
  selection: TextDecoder | undefined
  firstLine: TextDecoder | undefined
  head: Array<number> | undefined
  bom: boolean
}
/**
 * Checks whether an unknown value satisfies the State contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isState = (u: unknown): u is State => Predicate.hasProperty(u, TypeId)
function makeImpl(startLine: number, options: make.Options = {}): Result.Result<State, FileError> {
  const endLine = options.endLine ?? Infinity
  if (
    !Number.isSafeInteger(startLine) ||
    startLine < 0 ||
    !(endLine > startLine) ||
    (endLine !== Infinity && !Number.isSafeInteger(endLine))
  )
    return Result.fail(
      new FileError({ reason: new FileInvalid({ message: 'Invalid line range' }) }),
    )
  const state: State = Object.assign(Object.create(StateProto), {
    [TypeId]: TypeId,
    startLine,
    endLine,
    position: 0,
    newlines: 0,
    lineStart: 0,
    start: undefined,
    end: undefined,
    firstLineEnd: undefined,
    lastLineStart: undefined,
    selectedBytes: 0,
    firstLineBytes: 0,
    selection: undefined,
    firstLine: undefined,
    head: [],
    bom: false,
  })
  Object.defineProperty(state, TypeId, { enumerable: false })
  if (startLine === 0) begin(state, 0)
  return Result.succeed(state)
}
/**
 * Scans byte data synchronously and mutates offsets; native decoder and accessor faults can throw.
 *
 * @category unsafe
 * @since 0.0.0
 */
export function pushUnsafe(self: State, chunk: Uint8Array): void {
  if (self.head !== undefined) {
    const take = Math.min(3 - self.head.length, chunk.length)
    self.head.push(...chunk.subarray(0, take))
    if (self.head.length < 3) return
    releaseHead(self)
    chunk = chunk.subarray(take)
  }
  process(self, chunk)
}
function releaseHead(self: State): void {
  const head = Uint8Array.from(self.head ?? [])
  self.head = undefined
  self.bom = hasBom(head)
  process(self, head)
}
function process(self: State, chunk: Uint8Array): void {
  const base = self.position
  let from = 0
  for (
    let index = chunk.indexOf(NEWLINE);
    index !== -1;
    index = chunk.indexOf(NEWLINE, index + 1)
  ) {
    // The newline ends line `state.newlines`. It belongs to the selection between selected lines only.
    feed(self, chunk, base, from, index)
    const line = self.newlines
    const position = base + index
    if (line === self.startLine) endFirstLine(self, position)
    if (line === self.endLine - 1) endSelection(self, position)
    feed(self, chunk, base, index, index + 1)
    from = index + 1
    self.newlines++
    self.lineStart = position + 1
    if (self.newlines === self.startLine) begin(self, self.lineStart)
    if (self.newlines === self.endLine - 1) self.lastLineStart = self.lineStart
  }
  feed(self, chunk, base, from, chunk.length)
  self.position += chunk.length
}
/**
 * Completes scanner offsets synchronously; native decoder and accessor faults can throw.
 *
 * @category unsafe
 * @since 0.0.0
 */
export function finishUnsafe(self: State): LineScan {
  if (self.head !== undefined) releaseHead(self)
  const size = self.position
  if (self.start === undefined) {
    return {
      newlines: self.newlines,
      start: size,
      end: size,
      firstLineEnd: size,
      lastLineStart: size,
      selectedBytes: 0,
      firstLineBytes: 0,
    }
  }
  if (self.firstLineEnd === undefined) endFirstLine(self, size)
  if (self.end === undefined) endSelection(self, size)
  return {
    newlines: self.newlines,
    start: self.start,
    end: self.end ?? size,
    firstLineEnd: self.firstLineEnd ?? size,
    // A selection that reaches past the last line ends with the last line.
    lastLineStart: self.lastLineStart ?? self.lineStart,
    selectedBytes: self.selectedBytes,
    firstLineBytes: self.firstLineBytes,
  }
}
function begin(self: State, start: number): void {
  self.start = start
  if (self.startLine === self.endLine - 1) self.lastLineStart = start
  self.selection = rangeDecoder()
  self.firstLine = rangeDecoder()
}
function endFirstLine(self: State, position: number): void {
  self.firstLineEnd = position
  if (self.firstLine !== undefined) self.firstLineBytes += decodedBytes(self.firstLine.decode())
  self.firstLine = undefined
}
function endSelection(self: State, position: number): void {
  self.end = position
  if (self.selection !== undefined) self.selectedBytes += decodedBytes(self.selection.decode())
  self.selection = undefined
}
function feed(self: State, chunk: Uint8Array, base: number, from: number, to: number): void {
  // Decoding the whole file drops a leading byte-order mark.
  if (self.bom && base + from < 3) from = Math.min(to, 3 - base)
  if (to <= from) return
  const bytes = chunk.subarray(from, to)
  if (self.selection !== undefined) {
    self.selectedBytes += decodedBytes(self.selection.decode(bytes, { stream: true }))
  }
  if (self.firstLine !== undefined) {
    self.firstLineBytes += decodedBytes(self.firstLine.decode(bytes, { stream: true }))
  }
}

/**
 * Admits incremental data through the typed failure boundary.
 *
 * @category combinators
 * @since 0.0.0
 */
export const push = (self: State, chunk: Uint8Array): Effect.Effect<void, FileError> =>
  Effect.try({
    try: () => pushUnsafe(self, chunk),
    catch: (self) =>
      new FileError({
        reason: new FileUnknown({ message: 'Unable to scan file bytes', cause: self }),
      }),
  })
/**
 * Returns completed scanner offsets through the typed file error boundary.
 *
 * @category combinators
 * @since 0.0.0
 */
export const finish = (self: State): Effect.Effect<LineScan, FileError> =>
  Effect.try({
    try: () => finishUnsafe(self),
    catch: (cause) =>
      new FileError({ reason: new FileUnknown({ message: 'Unable to finish file scan', cause }) }),
  })
/**
 * Creates a mutable line scanner or synchronously throws the typed range failure.
 *
 * @category unsafe
 * @since 0.0.0
 */
export const makeUnsafe = (startLine: number, options: make.Options = {}): State =>
  Result.getOrThrow(make(startLine, options))
/**
 * Type contracts owned by `make`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace make {
  /**
   * Configuration accepted by make.
   *
   * @category models
   * @since 0.0.0
   */
  interface Options {
    readonly endLine?: number | undefined
  }
}

const StateProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/env/LineScan/State' }
  },
}

/**
 * Validates the line range and returns a mutable byte scanner with typed failures.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = (
  startLine: number,
  options: make.Options = {},
): Result.Result<State, FileError> =>
  Result.try({
    try: () => makeImpl(startLine, options),
    catch: (cause) =>
      new FileError({ reason: new FileInvalid({ message: 'Invalid line range', cause }) }),
  }).pipe(Result.flatMap(identity))
