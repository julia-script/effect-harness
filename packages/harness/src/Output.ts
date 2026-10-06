// Output slicing adapted from pi-durable (MIT), pinned 636703a0.
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import { OutputError } from './Error.ts'

/** Retention limits of one tool's output. */
export const Limits = Schema.Struct({
  maxBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxLines: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  retain: Schema.Literals(['head', 'tail']),
})
export type OutputLimits = typeof Limits.Type
export const defaults: OutputLimits = { maxBytes: 50 * 1024, maxLines: 2000, retain: 'head' }

/** Retained output and what the limits dropped. */
export type BoundedOutput = {
  readonly text: string
  readonly droppedBytes: number
  readonly droppedLines: number
}

/** An exact slice of the input within the limits, and what it left out. */
export type OutputSlice = {
  readonly text: string
  readonly bytes: number
  readonly droppedBytes: number
  readonly droppedLines: number
}

const NEWLINE = 0x0a

const encoder = new TextEncoder()
/** Slices decode exactly: a U+FEFF at a slice's start is text, not a byte-order mark. */
const decoder = new TextDecoder('utf-8', { ignoreBOM: true })

/** Remove control characters that break display and transcripts; tabs and newlines stay. */
export function sanitizeOutput(text: string): string {
  return Array.from(text)
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
export function boundOutput(text: string, limits: OutputLimits): OutputSlice {
  const bytes = encoder.encode(text)
  const [from, to] = limits.retain === 'head' ? headRange(bytes, limits) : tailRange(bytes, limits)
  const kept = bytes.subarray(from, to)
  return {
    text: kept.length === bytes.length ? text : decoder.decode(kept),
    bytes: kept.length,
    droppedBytes: bytes.length - kept.length,
    droppedLines: lineCount(bytes) - lineCount(kept),
  }
}

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
export function characterEnd(bytes: Uint8Array, index: number): number {
  let end = index
  while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--
  return end
}

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
export interface Buffer {
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
export interface Skip {
  readonly bytes: number
  readonly newlines: number
  readonly endsWithNewline: boolean
}
export function make(limits: OutputLimits = defaults): Buffer {
  return {
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
  }
}
/** String/skip boundaries flush incomplete byte sequences. Only a BOM at the stream's very start is removed. */
export const push = Effect.fnUntraced(function* (
  buffer: Buffer,
  chunk: string | Uint8Array,
  skipped?: Skip,
) {
  if (skipped !== undefined && buffer.limits.retain !== 'tail')
    return yield* new OutputError({ message: 'Skipped output requires tail retention' })
  const pending = typeof chunk === 'string' || skipped !== undefined ? buffer.decoder.decode() : ''
  let text = typeof chunk === 'string' ? chunk : buffer.decoder.decode(chunk, { stream: true })
  const first = !buffer.started && pending === '' && skipped === undefined
  if (pending !== '' || text !== '' || skipped !== undefined) buffer.started = true
  if (first && typeof chunk !== 'string' && text.startsWith('\ufeff')) text = text.slice(1)
  if (skipped === undefined) return accept(buffer, pending + text)
  accept(buffer, pending)
  if (skipped.bytes > 0) {
    buffer.totalBytes += skipped.bytes
    buffer.totalNewlines += skipped.newlines
    buffer.endsWithNewline = skipped.endsWithNewline
    buffer.chunks = []
    buffer.storedBytes = 0
    buffer.storedNewlines = 0
  }
  accept(buffer, text)
  return true
})
export function end(buffer: Buffer): void {
  accept(buffer, buffer.decoder.decode())
}
const byteLength = (text: string): number => encoder.encode(text).length
function accept(buffer: Buffer, text: string): boolean {
  if (text.length === 0) return false
  const bytes = byteLength(text)
  const newlines = countNewlines(text)
  buffer.totalBytes += bytes
  buffer.totalNewlines += newlines
  buffer.endsWithNewline = text.endsWith('\n')
  if (buffer.full) return true
  buffer.chunks.push({ text, bytes, newlines })
  buffer.storedBytes += bytes
  buffer.storedNewlines += newlines
  if (buffer.limits.retain === 'head') {
    buffer.full =
      buffer.storedBytes > buffer.limits.maxBytes || buffer.storedNewlines >= buffer.limits.maxLines
    return true
  }
  while (buffer.chunks.length > 1) {
    const first = buffer.chunks[0]
    if (first === undefined) break
    const bytesAfter = buffer.storedBytes - first.bytes
    const newlinesAfter = buffer.storedNewlines - first.newlines
    if (bytesAfter <= buffer.limits.maxBytes + 1 && newlinesAfter <= buffer.limits.maxLines + 1)
      break
    buffer.chunks.shift()
    buffer.storedBytes = bytesAfter
    buffer.storedNewlines = newlinesAfter
  }
  return true
}
/** Snapshot cadence never changes the retained tail; raw byte counts include later-sanitized controls. */
export function snapshot(buffer: Buffer): BoundedOutput {
  const stored = buffer.chunks.map((chunk) => chunk.text).join('')
  const kept = boundOutput(stored, buffer.limits)
  const storedLines = lines(buffer.storedNewlines, stored === '' || stored.endsWith('\n'))
  const keptLines = storedLines - kept.droppedLines
  if (buffer.limits.retain === 'tail' || buffer.chunks.length > 1) {
    const text = buffer.limits.retain === 'tail' ? tailMargin(stored, buffer.limits) : stored
    buffer.storedBytes = byteLength(text)
    buffer.storedNewlines = countNewlines(text)
    buffer.chunks =
      text === '' ? [] : [{ text, bytes: buffer.storedBytes, newlines: buffer.storedNewlines }]
  }
  return {
    text: sanitizeOutput(kept.text),
    droppedBytes: buffer.totalBytes - kept.bytes,
    droppedLines: lines(buffer.totalNewlines, buffer.endsWithNewline) - keptLines,
  }
}
/**
 * The shortest suffix of `text` with more than `maxBytes` bytes or more than `maxLines` newlines, or all of it. The
 * tail window of any text that ends with this suffix, followed by anything, is the same as of `text` followed by it.
 */
function tailMargin(text: string, limits: OutputLimits): string {
  const bytes = encoder.encode(text)
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
  return start === 0 ? text : decoder.decode(bytes.subarray(start))
}

/** Lines of text with `newlines` newlines; a final unterminated line counts. */
function lines(newlines: number, terminated: boolean): number {
  return newlines + (terminated ? 0 : 1)
}

function countNewlines(text: string): number {
  let count = 0
  for (let index = text.indexOf('\n'); index !== -1; index = text.indexOf('\n', index + 1)) count++
  return count
}

export type Delta =
  | { readonly type: 'append'; readonly trimStart: number; readonly text: string }
  | { readonly type: 'set'; readonly text: string }
/** Longest suffix/prefix overlap, with bounded work and a whole-value fallback. UTF-16 offsets suit JS text clients. */
export function delta(previous: string, current: string, maxScan = 65536): Delta {
  if (current.startsWith(previous))
    return { type: 'append', trimStart: 0, text: current.slice(previous.length) }
  if (Math.min(previous.length, current.length) > maxScan) return { type: 'set', text: current }
  for (let overlap = Math.min(previous.length, current.length); overlap > 0; overlap--)
    if (previous.endsWith(current.slice(0, overlap)))
      return { type: 'append', trimStart: previous.length - overlap, text: current.slice(overlap) }
  return { type: 'set', text: current }
}
