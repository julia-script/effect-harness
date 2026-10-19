/**
 * Output bounding, ported from Chord's `boundOutput`: whole lines within byte and line limits,
 * the first lines for `head` and the last for `tail`. A single line longer than `maxBytes` is cut
 * on a character boundary.
 */
import type { Content, OutputLimits } from '../Tool.ts'

/** What bounding kept, and what it left out. */
export interface Bounded {
  readonly text: string
  readonly droppedBytes: number
  readonly droppedLines: number
}

const NEWLINE = 0x0a
const encoder = new TextEncoder()
/** Slices decode exactly: a U+FEFF at a slice's start is text, not a byte-order mark. */
const decoder = new TextDecoder('utf-8', { ignoreBOM: true })

/** The last character boundary at or before `index`. */
const characterEnd = (bytes: Uint8Array, index: number): number => {
  let end = index
  while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--
  return end
}

/** The first character boundary at or after `index`. */
const characterStart = (bytes: Uint8Array, index: number): number => {
  let start = index
  while (start < bytes.length && ((bytes[start] ?? 0) & 0xc0) === 0x80) start++
  return start
}

const lineCount = (bytes: Uint8Array): number => {
  if (bytes.length === 0) return 0
  let newlines = 0
  for (let index = bytes.indexOf(NEWLINE); index !== -1; index = bytes.indexOf(NEWLINE, index + 1))
    newlines++
  return newlines + (bytes.at(-1) === NEWLINE ? 0 : 1)
}

const headRange = (bytes: Uint8Array, limits: OutputLimits): readonly [number, number] => {
  if (limits.maxLines === 0 || limits.maxBytes === 0) return [0, 0]
  let end = bytes.length
  let lines = 0
  for (
    let index = bytes.indexOf(NEWLINE);
    index !== -1;
    index = bytes.indexOf(NEWLINE, index + 1)
  ) {
    lines++
    if (lines === limits.maxLines) {
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

const tailRange = (bytes: Uint8Array, limits: OutputLimits): readonly [number, number] => {
  if (limits.maxLines === 0 || limits.maxBytes === 0) return [bytes.length, bytes.length]
  // A trailing newline ends the last line rather than starting another.
  const last = bytes.at(-1) === NEWLINE ? bytes.length - 2 : bytes.length - 1
  let start = 0
  let lines = 1
  let index = last < 0 ? -1 : bytes.lastIndexOf(NEWLINE, last)
  while (index !== -1) {
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
    // The first line starting inside the byte window, or a cut of the last line when it alone is
    // too long.
    start = newline !== -1 && newline + 1 < bytes.length ? newline + 1 : characterStart(bytes, from)
  }
  return [start, bytes.length]
}

/** `text` within `limits`. */
export const bound = (text: string, limits: OutputLimits): Bounded => {
  const bytes = encoder.encode(text)
  const [from, to] = limits.retain === 'head' ? headRange(bytes, limits) : tailRange(bytes, limits)
  const kept = bytes.subarray(from, to)
  return {
    text: kept.length === bytes.length ? text : decoder.decode(kept),
    droppedBytes: bytes.length - kept.length,
    droppedLines: lineCount(bytes) - lineCount(kept),
  }
}

/**
 * Bound the text of result content. When the joined text exceeds the limits, the text items are
 * replaced by one bounded item at the place of the first (head) or last (tail) of them; other
 * content is kept.
 */
export const boundContent = (
  content: Content,
  limits: OutputLimits,
): { readonly content: Content; readonly droppedBytes: number; readonly droppedLines: number } => {
  const texts = content.flatMap((item) => (item.type === 'text' ? [item] : []))
  const bounded = bound(texts.map((item) => item.text).join(''), limits)
  if (bounded.droppedBytes === 0) return { content, droppedBytes: 0, droppedLines: 0 }
  const keep = limits.retain === 'head' ? texts[0] : texts.at(-1)
  const kept: Content = content.flatMap((item): Content => {
    if (item.type !== 'text') return [item]
    return item === keep ? [{ ...item, text: bounded.text }] : []
  })
  return { content: kept, droppedBytes: bounded.droppedBytes, droppedLines: bounded.droppedLines }
}
