/**
 * Incremental byte scanning for exact text-line selection offsets.
 */
import { identity } from 'effect/Function'
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Effect from 'effect/Effect'
import * as Predicate from 'effect/Predicate'
// Adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
import { makeRangeDecoder, hasBom } from './Decode.ts'
import type { LineScan } from 'effect-harness/NativeFiles'
import { FileError, FileInvalidError, FileUnknownError } from '../FileError.ts'
import * as Result from 'effect/Result'
const NEWLINE = 10
const encoder = new TextEncoder()
const decodedBytes = (self: string): number => encoder.encode(self).length
const TypeId = '~effect-harness/env/LineScan'
/**
 * Incremental newline and byte-offset accumulator for a selected line window.
 *
 * @category models
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
 * Checks whether a value carries the nominal `State` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
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
      new FileError({ reason: new FileInvalidError({ message: 'Invalid line range' }) }),
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
  if (startLine === 0) beginUnsafe(state, 0)
  return Result.succeed(state)
}
/**
 * Scans byte data synchronously and mutates offsets; native decoder and accessor faults can throw.
 *
 * @category unsafe
 */
export function pushUnsafe(self: State, chunk: Uint8Array): void {
  if (self.head !== undefined) {
    const take = Math.min(3 - self.head.length, chunk.length)
    self.head.push(...chunk.subarray(0, take))
    if (self.head.length < 3) return
    releaseHeadUnsafe(self)
    chunk = chunk.subarray(take)
  }
  processUnsafe(self, chunk)
}
function releaseHead(self: State): Result.Result<void, MutationFailure> {
  return Result.try({
    try: () => {
      const head = Uint8Array.from(self.head ?? [])
      self.head = undefined
      self.bom = hasBom(head)
      processUnsafe(self, head)
    },
    catch: (cause) => ({ cause }),
  })
}

// The public Effect.try/Result boundary retains its original cause and owned mutation timing.
function releaseHeadUnsafe(self: State): void {
  return Result.getOrThrowWith(releaseHead(self), (failure) => failure.cause)
}
function process(self: State, chunk: Uint8Array): Result.Result<void, MutationFailure> {
  return Result.try({
    try: () => {
      const base = self.position
      let from = 0
      for (
        let index = chunk.indexOf(NEWLINE);
        index !== -1;
        index = chunk.indexOf(NEWLINE, index + 1)
      ) {
        // The newline ends line `state.newlines`. It belongs to the selection between selected lines only.
        feedUnsafe(self, chunk, base, from, index)
        const line = self.newlines
        const position = base + index
        if (line === self.startLine) endFirstLineUnsafe(self, position)
        if (line === self.endLine - 1) endSelectionUnsafe(self, position)
        feedUnsafe(self, chunk, base, index, index + 1)
        from = index + 1
        self.newlines++
        self.lineStart = position + 1
        if (self.newlines === self.startLine) beginUnsafe(self, self.lineStart)
        if (self.newlines === self.endLine - 1) self.lastLineStart = self.lineStart
      }
      feedUnsafe(self, chunk, base, from, chunk.length)
      self.position += chunk.length
    },
    catch: (cause) => ({ cause }),
  })
}

// The public Effect.try/Result boundary retains its original cause and owned mutation timing.
function processUnsafe(self: State, chunk: Uint8Array): void {
  return Result.getOrThrowWith(process(self, chunk), (failure) => failure.cause)
}
/**
 * Completes scanner offsets synchronously; native decoder and accessor faults can throw.
 *
 * @category unsafe
 */
export function finishUnsafe(self: State): LineScan {
  if (self.head !== undefined) releaseHeadUnsafe(self)
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
  if (self.firstLineEnd === undefined) endFirstLineUnsafe(self, size)
  if (self.end === undefined) endSelectionUnsafe(self, size)
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
function begin(self: State, start: number): Result.Result<void, MutationFailure> {
  return Result.try({
    try: () => {
      self.start = start
      if (self.startLine === self.endLine - 1) self.lastLineStart = start
      self.selection = makeRangeDecoder()
      self.firstLine = makeRangeDecoder()
    },
    catch: (cause) => ({ cause }),
  })
}

// The public Effect.try/Result boundary retains its original cause and owned mutation timing.
function beginUnsafe(self: State, start: number): void {
  return Result.getOrThrowWith(begin(self, start), (failure) => failure.cause)
}
function endFirstLine(self: State, position: number): Result.Result<void, MutationFailure> {
  return Result.try({
    try: () => {
      self.firstLineEnd = position
      if (self.firstLine !== undefined) self.firstLineBytes += decodedBytes(self.firstLine.decode())
      self.firstLine = undefined
    },
    catch: (cause) => ({ cause }),
  })
}

// The public Effect.try/Result boundary retains its original cause and owned mutation timing.
function endFirstLineUnsafe(self: State, position: number): void {
  return Result.getOrThrowWith(endFirstLine(self, position), (failure) => failure.cause)
}
function endSelection(self: State, position: number): Result.Result<void, MutationFailure> {
  return Result.try({
    try: () => {
      self.end = position
      if (self.selection !== undefined) self.selectedBytes += decodedBytes(self.selection.decode())
      self.selection = undefined
    },
    catch: (cause) => ({ cause }),
  })
}

// The public Effect.try/Result boundary retains its original cause and owned mutation timing.
function endSelectionUnsafe(self: State, position: number): void {
  return Result.getOrThrowWith(endSelection(self, position), (failure) => failure.cause)
}
function feed(
  self: State,
  chunk: Uint8Array,
  base: number,
  from: number,
  to: number,
): Result.Result<void, MutationFailure> {
  return Result.try({
    try: () => {
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
    },
    catch: (cause) => ({ cause }),
  })
}

// The public Effect.try/Result boundary retains its original cause and owned mutation timing.
function feedUnsafe(self: State, chunk: Uint8Array, base: number, from: number, to: number): void {
  return Result.getOrThrowWith(feed(self, chunk, base, from, to), (failure) => failure.cause)
}

/**
 * Admits incremental data through the typed failure boundary.
 *
 * @category combinators
 */
export const push = (self: State, chunk: Uint8Array): Effect.Effect<void, FileError> =>
  Effect.try({
    try: () => pushUnsafe(self, chunk),
    catch: (cause) =>
      new FileError({
        reason: new FileUnknownError({ message: 'Unable to scan file bytes', cause }),
      }),
  })
/**
 * Returns completed scanner offsets through the typed file error boundary.
 *
 * @category combinators
 */
export const finish = (self: State): Effect.Effect<LineScan, FileError> =>
  Effect.try({
    try: () => finishUnsafe(self),
    catch: (cause) =>
      new FileError({
        reason: new FileUnknownError({ message: 'Unable to finish file scan', cause }),
      }),
  })
/**
 * Creates a mutable line scanner or synchronously throws the typed range failure.
 *
 * @category unsafe
 */
export const makeUnsafe = (startLine: number, options: make.Options = {}): State =>
  Result.getOrThrow(make(startLine, options))
/**
 * Type-level contracts for `make`.
 *
 */
export declare namespace make {
  /**
   * Configuration accepted by make.
   *
   * @category models
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
 */
export const make = (
  startLine: number,
  options: make.Options = {},
): Result.Result<State, FileError> =>
  Result.try({
    try: () => makeImpl(startLine, options),
    catch: (cause) =>
      new FileError({ reason: new FileInvalidError({ message: 'Invalid line range', cause }) }),
  }).pipe(Result.flatMap(identity))

/** Private native-fault token: safe Results retain the original cause for the existing outer boundary. */
interface MutationFailure {
  readonly cause: unknown
}
