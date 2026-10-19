/**
 * Truncating text to whole lines from its start, ported from pi's `truncate.ts`. Two limits apply
 * and whichever is hit first wins: a number of lines and a number of UTF-8 bytes.
 */
import { DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES } from '../CodingTools.ts'

const encoder = new TextEncoder()

export const utf8ByteLength = (text: string) => encoder.encode(text).length

/** A size as people read it. */
export const formatSize = (bytes: number) => {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
}

/** How text was cut; the text itself is `content`. */
export interface Truncation {
  readonly content: string
  readonly truncated: boolean
  readonly truncatedBy: 'lines' | 'bytes' | null
  readonly totalLines: number
  readonly totalBytes: number
  readonly outputLines: number
  readonly outputBytes: number
  /** The first line alone exceeds the byte limit, so `content` is empty. */
  readonly firstLineExceedsLimit: boolean
}

/** Lines as counted: a trailing newline adds none, and empty text has none. */
const linesOf = (content: string) => {
  if (content.length === 0) return []
  const lines = content.split('\n')
  if (content.endsWith('\n')) lines.pop()
  return lines
}

/** The whole lines of `content` from its start that fit both limits. Never a partial line. */
export const truncateHead = (
  content: string,
  maxLines = DEFAULT_MAX_LINES,
  maxBytes = DEFAULT_MAX_BYTES,
): Truncation => {
  const lines = linesOf(content)
  const totalBytes = utf8ByteLength(content)
  const totals = { totalLines: lines.length, totalBytes }
  if (lines.length <= maxLines && totalBytes <= maxBytes) {
    const whole = { truncated: false, truncatedBy: null, firstLineExceedsLimit: false }
    return { content, ...totals, ...whole, outputLines: lines.length, outputBytes: totalBytes }
  }
  if (utf8ByteLength(lines[0] ?? '') > maxBytes) {
    const none = { outputLines: 0, outputBytes: 0, firstLineExceedsLimit: true }
    return { content: '', truncated: true, truncatedBy: 'bytes', ...totals, ...none }
  }
  const kept: Array<string> = []
  let bytes = 0
  let byBytes = false
  for (const line of lines.slice(0, maxLines)) {
    const size = utf8ByteLength(line) + (kept.length > 0 ? 1 : 0)
    if (bytes + size > maxBytes) {
      byBytes = true
      break
    }
    kept.push(line)
    bytes += size
  }
  // Without a byte break, only omitted lines show the line limit was hit.
  const truncatedBy = byBytes || kept.length === lines.length ? 'bytes' : 'lines'
  const output = kept.join('\n')
  return {
    content: output,
    truncated: true,
    truncatedBy,
    ...totals,
    outputLines: kept.length,
    outputBytes: utf8ByteLength(output),
    firstLineExceedsLimit: false,
  }
}

/** The end of the longest prefix of `bytes` within `limit` that ends on a character boundary. */
export const characterEnd = (bytes: Uint8Array, limit: number) => {
  if (bytes.length <= limit) return bytes.length
  let end = limit
  // UTF-8 continuation bytes are 10xxxxxx.
  while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--
  return end
}
