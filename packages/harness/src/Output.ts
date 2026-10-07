/**
 * Incremental output retention with exact UTF-8 limits and UTF-16 deltas.
 */
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import { dual } from 'effect/Function'
import * as Data from 'effect/Data'
import * as Predicate from 'effect/Predicate'
// Output slicing adapted from pi-durable (MIT), pinned 636703a0.
import * as SynchronizedRef from 'effect/SynchronizedRef'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import { OutputError, OutputFailure } from './OutputError.ts'

/**
 * Retention limits of one tool's output.
 *
 * @category schemas
 */
export const Limits = Schema.Struct({
  maxBytes: Schema.Natural,
  maxLines: Schema.Natural,
  retain: Schema.Literals(['head', 'tail']),
})
/**
 * Byte, line and retention policy for model-visible tool output.
 *
 * @category models
 */
export type Limits = typeof Limits.Type
/**
 * Normalized output policy with explicit byte and line bounds.
 *
 * @category models
 */
export type OutputLimits = Limits
/**
 * Default head-retention limits of 50 KiB and 2000 lines.
 *
 * @category constants
 */
export const defaults: OutputLimits = { maxBytes: 50 * 1024, maxLines: 2000, retain: 'head' }

/**
 * Retained output and what the limits dropped.
 *
 * @category models
 */
export interface BoundedOutput {
  readonly text: string
  readonly droppedBytes: number
  readonly droppedLines: number
}

/**
 * An exact slice of the input within the limits, and what it left out.
 *
 * @category models
 */
export interface OutputSlice {
  readonly text: string
  readonly bytes: number
  readonly droppedBytes: number
  readonly droppedLines: number
}

const NEWLINE = 0x0a

const encoder = new TextEncoder()
/** Slices decode exactly: a U+FEFF at a slice's start is text, not a byte-order mark. */
const decoder = new TextDecoder('utf-8', { ignoreBOM: true })

/**
 * Removes control characters that break display and transcripts; tabs and newlines stay.
 *
 * @category combinators
 */
export function sanitizeOutput(self: string): string {
  return Array.from(self)
    .filter((character) => {
      const code = character.codePointAt(0) ?? 0
      return !(code <= 8 || (code >= 11 && code <= 31) || (code >= 0xfff9 && code <= 0xfffb))
    })
    .join('')
}

/**
 * Bound `text` to whole lines within the limits: the first lines for `head`, the last lines for `tail`. The result is an
 * exact slice, trailing newline included. A single line longer than `maxBytes` is cut at the byte limit on a character
 * boundary.
 */
function boundOutputImpl(self: string, limits: OutputLimits): OutputSlice {
  const bytes = encoder.encode(self)
  const [from, to] = limits.retain === 'head' ? headRange(bytes, limits) : tailRange(bytes, limits)
  const kept = bytes.subarray(from, to)
  return {
    text: kept.length === bytes.length ? self : decoder.decode(kept),
    bytes: kept.length,
    droppedBytes: bytes.length - kept.length,
    droppedLines: lineCount(bytes) - lineCount(kept),
  }
}
/**
 * Returns an exact whole-line slice within UTF-8 byte and line limits.
 *
 * @category combinators
 */
export const boundOutput: {
  (limits: OutputLimits): (self: string) => OutputSlice
  (self: string, limits: OutputLimits): OutputSlice
} = dual(2, boundOutputImpl)

function headRange(bytes: Uint8Array, limits: OutputLimits): [number, number] {
  if (limits.maxLines === 0 || limits.maxBytes === 0) return [0, 0]
  let end = bytes.length
  let lines = 0
  for (
    let index = bytes.indexOf(NEWLINE);
    index !== -1;
    index = bytes.indexOf(NEWLINE, index + 1)
  ) {
    if (++lines === limits.maxLines) {
      end = index + 1
      break
    }
  }
  if (end > limits.maxBytes) {
    const newline = bytes.lastIndexOf(NEWLINE, limits.maxBytes - 1)
    end = newline === -1 ? characterEnd(bytes, limits.maxBytes) : newline + 1
  }
  return [0, end]
}

function tailRange(bytes: Uint8Array, limits: OutputLimits): [number, number] {
  if (limits.maxLines === 0 || limits.maxBytes === 0) return [bytes.length, bytes.length]
  // A trailing newline ends the last line rather than starting another.
  const last = bytes[bytes.length - 1] === NEWLINE ? bytes.length - 2 : bytes.length - 1
  let start = 0
  let lines = 1
  for (let index = last < 0 ? -1 : bytes.lastIndexOf(NEWLINE, last); index !== -1;) {
    if (lines === limits.maxLines) {
      start = index + 1
      break
    }
    lines++
    index = index === 0 ? -1 : bytes.lastIndexOf(NEWLINE, index - 1)
  }
  if (bytes.length - start > limits.maxBytes) {
    const from = bytes.length - limits.maxBytes
    const newline = bytes.indexOf(NEWLINE, from - 1)
    // The first line starting inside the byte window, or a cut of the last line when it alone is too long.
    start = newline !== -1 && newline + 1 < bytes.length ? newline + 1 : characterStart(bytes, from)
  }
  return [start, bytes.length]
}

/** The last character boundary at or before `index`. */
function characterEndImpl(bytes: Uint8Array, index: number): number {
  let end = index
  while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--
  return end
}
/**
 * Finds a UTF-8 character boundary at or before a byte offset.
 *
 * @category combinators
 */
export const characterEnd: {
  (index: number): (self: Uint8Array) => number
  (self: Uint8Array, index: number): number
} = dual(2, characterEndImpl)

/** The first character boundary at or after `index`. */
function characterStart(bytes: Uint8Array, index: number): number {
  let start = index
  while (start < bytes.length && ((bytes[start] ?? 0) & 0xc0) === 0x80) start++
  return start
}

function lineCount(bytes: Uint8Array): number {
  if (bytes.length === 0) return 0
  let newlines = 0
  for (let index = bytes.indexOf(NEWLINE); index !== -1; index = bytes.indexOf(NEWLINE, index + 1))
    newlines++
  return newlines + (bytes[bytes.length - 1] === NEWLINE ? 0 : 1)
}

/** Data owned by one invocation; sibling functions manage its incremental decoder and retention window. */
const TypeId = '~@effect-harness/harness/Output'
/**
 * Incremental bounded output buffer.
 *
 * @category models
 */
export interface Buffer extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: typeof TypeId
  readonly limits: OutputLimits
  readonly decoder: TextDecoder
  started: boolean
  chunks: Array<{ readonly text: string; readonly bytes: number; readonly newlines: number }>
  storedBytes: number
  storedNewlines: number
  full: boolean
  totalBytes: number
  totalNewlines: number
  endsWithNewline: boolean
}
/**
 * Metadata describing output omitted before a retained window.
 *
 * @category models
 */
export interface Skip {
  readonly bytes: number
  readonly newlines: number
  readonly endsWithNewline: boolean
}
/**
 * Checks whether a value carries the nominal `Buffer` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isBuffer = (u: unknown): u is Buffer => Predicate.hasProperty(u, TypeId)
/**
 * Creates a fresh mutable buffer, cloning chunk storage and preserving other capability accessors.
 *
 * @category constructors
 */
export const makeBuffer = (
  input: Omit<Buffer, typeof TypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable>,
): Buffer => {
  const handle: Buffer = Object.create(BufferProto)
  // Reading chunks once establishes owned storage. Other accessors remain live,
  // while data fields become writable even when the source configuration is frozen.
  const descriptors = Object.getOwnPropertyDescriptors(input)
  for (const key of Reflect.ownKeys(descriptors)) {
    if (key === 'chunks' || key === TypeId) continue
    const descriptor = Object.getOwnPropertyDescriptor(input, key)
    if (descriptor === undefined) continue
    Object.defineProperty(
      handle,
      key,
      descriptor.get !== undefined || descriptor.set !== undefined
        ? descriptor
        : { ...descriptor, writable: true, configurable: true },
    )
  }
  Object.defineProperty(handle, 'chunks', {
    value: [...input.chunks],
    writable: true,
    configurable: true,
    enumerable: true,
  })
  Object.defineProperty(handle, TypeId, { value: TypeId, enumerable: false })
  return handle
}
/**
 * Creates an empty mutable UTF-8 output buffer with the supplied retention limits.
 *
 * @category constructors
 */
export function make(limits: OutputLimits = defaults): Buffer {
  return makeBuffer({
    limits,
    decoder: new TextDecoder('utf-8', { ignoreBOM: true }),
    started: false,
    chunks: [],
    storedBytes: 0,
    storedNewlines: 0,
    full: false,
    totalBytes: 0,
    totalNewlines: 0,
    endsWithNewline: true,
  })
}
/**
 * String/skip boundaries flush incomplete byte sequences.
 *
 * **Details**
 *
 * Only a BOM at the stream's very start is removed.
 *
 * @category combinators
 */
export const push = (
  self: Buffer,
  chunk: string | Uint8Array,
  skipped?: Skip,
): Effect.Effect<boolean, OutputError> =>
  Effect.try({
    try: () => pushUnsafe(self, chunk, skipped),
    catch: (cause) =>
      cause instanceof OutputError
        ? cause
        : new OutputError({
            reason: new OutputFailure({ message: 'Unable to retain output', cause }),
          }),
  })

// Native decoder/accessor and mutable buffer operations run only through the public Effect.try boundary.
const pushUnsafe = (self: Buffer, chunk: string | Uint8Array, skipped?: Skip): boolean => {
  if (skipped !== undefined && self.limits.retain !== 'tail')
    throw new OutputError({
      reason: new OutputFailure({ message: 'Skipped output requires tail retention' }),
    })
  const pending = typeof chunk === 'string' || skipped !== undefined ? self.decoder.decode() : ''
  let text = typeof chunk === 'string' ? chunk : self.decoder.decode(chunk, { stream: true })
  const first = !self.started && pending === '' && skipped === undefined
  if (pending !== '' || text !== '' || skipped !== undefined) self.started = true
  if (first && typeof chunk !== 'string' && text.startsWith('\ufeff')) text = text.slice(1)
  if (skipped === undefined) return accept(self, pending + text)
  accept(self, pending)
  if (skipped.bytes > 0) {
    self.totalBytes += skipped.bytes
    self.totalNewlines += skipped.newlines
    self.endsWithNewline = skipped.endsWithNewline
    self.chunks = []
    self.storedBytes = 0
    self.storedNewlines = 0
  }
  accept(self, text)
  return true
}
/**
 * Flushes incremental decoding synchronously; native decoder and accessor faults can throw.
 *
 * @category unsafe
 */
export function endUnsafe(self: Buffer): void {
  accept(self, self.decoder.decode())
}
const byteLength = (self: string): number => encoder.encode(self).length
function accept(self: Buffer, text: string): boolean {
  if (text.length === 0) return false
  const bytes = byteLength(text)
  const newlines = countNewlines(text)
  self.totalBytes += bytes
  self.totalNewlines += newlines
  self.endsWithNewline = text.endsWith('\n')
  if (self.full) return true
  self.chunks.push({ text, bytes, newlines })
  self.storedBytes += bytes
  self.storedNewlines += newlines
  if (self.limits.retain === 'head') {
    self.full =
      self.storedBytes > self.limits.maxBytes || self.storedNewlines >= self.limits.maxLines
    return true
  }
  while (self.chunks.length > 1) {
    const first = self.chunks[0]
    if (first === undefined) break
    const bytesAfter = self.storedBytes - first.bytes
    const newlinesAfter = self.storedNewlines - first.newlines
    if (bytesAfter <= self.limits.maxBytes + 1 && newlinesAfter <= self.limits.maxLines + 1) break
    self.chunks.shift()
    self.storedBytes = bytesAfter
    self.storedNewlines = newlinesAfter
  }
  return true
}
/**
 * Snapshot cadence never changes the retained tail; raw byte counts include later-sanitized controls.
 *
 * @category unsafe
 */
export function snapshotUnsafe(self: Buffer): BoundedOutput {
  const stored = self.chunks.map((chunk) => chunk.text).join('')
  const kept = boundOutput(stored, self.limits)
  const storedLines = lines(self.storedNewlines, stored === '' || stored.endsWith('\n'))
  const keptLines = storedLines - kept.droppedLines
  if (self.limits.retain === 'tail' || self.chunks.length > 1) {
    const text = self.limits.retain === 'tail' ? tailMargin(stored, self.limits) : stored
    self.storedBytes = byteLength(text)
    self.storedNewlines = countNewlines(text)
    self.chunks =
      text === '' ? [] : [{ text, bytes: self.storedBytes, newlines: self.storedNewlines }]
  }
  return {
    text: sanitizeOutput(kept.text),
    droppedBytes: self.totalBytes - kept.bytes,
    droppedLines: lines(self.totalNewlines, self.endsWithNewline) - keptLines,
  }
}
/**
 * The shortest suffix of `text` with more than `maxBytes` bytes or more than `maxLines` newlines, or all of it. The
 * tail window of any text that ends with this suffix, followed by anything, is the same as of `text` followed by it.
 */
function tailMargin(self: string, limits: OutputLimits): string {
  const bytes = encoder.encode(self)
  const byteStart =
    bytes.length > limits.maxBytes ? characterEnd(bytes, bytes.length - limits.maxBytes - 1) : 0
  let lineStart = 0
  let newlines = 0
  for (
    let index = bytes.lastIndexOf(NEWLINE);
    index !== -1;
    index = bytes.lastIndexOf(NEWLINE, index - 1)
  ) {
    if (++newlines > limits.maxLines) {
      lineStart = index
      break
    }
    if (index === 0) break
  }
  const start = Math.max(byteStart, lineStart)
  return start === 0 ? self : decoder.decode(bytes.subarray(start))
}

/** Lines of text with `newlines` newlines; a final unterminated line counts. */
function lines(self: number, terminated: boolean): number {
  return self + (terminated ? 0 : 1)
}

function countNewlines(self: string): number {
  let count = 0
  for (let index = self.indexOf('\n'); index !== -1; index = self.indexOf('\n', index + 1)) count++
  return count
}

/**
 * Append or replacement needed to update an output projection.
 *
 * @category models
 */
export type Delta = Data.TaggedEnum<{
  append: { readonly trimStart: number; readonly text: string }
  set: { readonly text: string }
}>
/**
 * Constructors and matchers for incremental UTF-16 output changes.
 *
 * @category constants
 */
export const Delta = Data.taggedEnum<Delta>()
/** Longest suffix/prefix overlap, with bounded work and a whole-value fallback. UTF-16 offsets suit JS text clients. */
function deltaImpl(self: string, that: string, maxScan = 65536): Delta {
  if (that.startsWith(self)) return Delta.append({ trimStart: 0, text: that.slice(self.length) })
  if (Math.min(self.length, that.length) > maxScan) return Delta.set({ text: that })
  for (let overlap = Math.min(self.length, that.length); overlap > 0; overlap--)
    if (self.endsWith(that.slice(0, overlap)))
      return Delta.append({ trimStart: self.length - overlap, text: that.slice(overlap) })
  return Delta.set({ text: that })
}
/**
 * Returns incremental changes between the previous and current values.
 *
 * @category combinators
 */
export const delta: {
  (that: string, maxScan?: number): (self: string) => Delta
  (self: string, that: string, maxScan?: number): Delta
} = dual((args) => typeof args[1] === 'string', deltaImpl)

/** Shared invocation retention state is immutable; TextDecoder is private native streaming state. */
type WindowState = Readonly<Omit<Buffer, 'decoder' | 'chunks' | typeof TypeId>> & {
  readonly chunks: ReadonlyArray<{
    readonly text: string
    readonly bytes: number
    readonly newlines: number
  }>
}
/**
 * Incremental bounded window that tracks output changes.
 *
 * @category models
 */
export interface Window {
  readonly push: (chunk: string | Uint8Array, skipped?: Skip) => Effect.Effect<boolean, OutputError>
  readonly reset: Effect.Effect<void>
  readonly end: Effect.Effect<void, OutputError>
  readonly snapshot: Effect.Effect<BoundedOutput, OutputError>
}
/**
 * Creates a serialized output window whose commands share one decoder and retention state.
 *
 * @category constructors
 */
export const makeWindow = Effect.fnUntraced(function* (
  limits: OutputLimits = defaults,
): Effect.fn.Return<Window> {
  const nativeDecoder = new TextDecoder('utf-8', { ignoreBOM: true })
  const initial = (): WindowState => {
    const { decoder: _decoder, [TypeId]: _brand, ...state } = make(limits)
    return state
  }
  const state = yield* SynchronizedRef.make(initial())
  const local = (self: WindowState): Buffer =>
    makeBuffer({
      ...self,
      chunks: [...self.chunks],
      decoder: nativeDecoder,
    })
  const stored = (self: Buffer): WindowState => {
    const { decoder: _decoder, [TypeId]: _brand, ...next } = self
    return next
  }
  return {
    push: Effect.fnUntraced(function* (chunk: string | Uint8Array, skipped?: Skip) {
      return yield* SynchronizedRef.modifyEffect(
        state,
        Effect.fnUntraced(function* (current) {
          const working = local(current)
          const changed = yield* push(working, chunk, skipped)
          return [changed, stored(working)] as const
        }),
      )
    }),
    reset: SynchronizedRef.modify(state, () => {
      nativeDecoder.decode()
      return [undefined, initial()] as const
    }),
    end: SynchronizedRef.modifyEffect(state, (current) => {
      const working = local(current)
      return Effect.map(end(working), () => [undefined, stored(working)] as const)
    }),
    snapshot: SynchronizedRef.modifyEffect(state, (current) => {
      const working = local(current)
      return Effect.map(snapshot(working), (value) => [value, stored(working)] as const)
    }),
  }
})

/**
 * Checks whether a value satisfies the decoded `Limits` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isOutputLimits: (u: unknown) => u is Limits = Schema.is(Limits)

/**
 * Flushes incremental decoding through the typed output error boundary.
 *
 * @category combinators
 */
export const end = (self: Buffer): Effect.Effect<void, OutputError> =>
  Effect.try({
    try: () => endUnsafe(self),
    catch: (cause) =>
      new OutputError({
        reason: new OutputFailure({ message: 'Unable to finish output decoding', cause }),
      }),
  })
/**
 * Returns a typed snapshot of retained output and its dropped counts.
 *
 * @category combinators
 */
export const snapshot = (self: Buffer): Effect.Effect<BoundedOutput, OutputError> =>
  Effect.try({
    try: () => snapshotUnsafe(self),
    catch: (cause) =>
      new OutputError({
        reason: new OutputFailure({ message: 'Unable to snapshot output', cause }),
      }),
  })

const BufferProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/Output/Buffer' }
  },
}
