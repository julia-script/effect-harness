/**
 * Whole-line text truncation with explicit byte and line counts.
 *
 * @since 0.0.0
 */
import { dual } from 'effect/Function'
// Adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
/**
 * Shared truncation utilities for tool outputs.
 *
 * **Details**
 *
 * Truncation is based on two independent limits - whichever is hit first wins:
 * - Line limit (default: 2000 lines)
 * - Byte limit (default: 50KB)
 *
 * Never returns partial lines. Tool output streams are bounded by `Output` instead.
 *
 * @category constants
 * @since 0.0.0
 */
export const DEFAULT_MAX_LINES = 2000
/**
 * Default UTF-8 text truncation limit in bytes.
 *
 * @category constants
 * @since 0.0.0
 */
export const DEFAULT_MAX_BYTES = 50 * 1024 // 50KB

/**
 * Truncate truncation result contract.
 *
 * @category models
 * @since 0.0.0
 */
export type TruncationResult = truncateHead.Result

/**
 * Truncate truncation options contract.
 *
 * @category models
 * @since 0.0.0
 */
export type TruncationOptions = truncateHead.Options

const encoder = new TextEncoder()
/**
 * Returns the UTF-8 byte length of text.
 *
 * @category combinators
 * @since 0.0.0
 */
export function utf8ByteLength(self: string): number {
  return encoder.encode(self).length
}

function splitLinesForCounting(self: string): Array<string> {
  if (self.length === 0) return []
  const lines = self.split('\n')
  if (self.endsWith('\n')) lines.pop()
  return lines
}

/**
 * Formats bytes as a readable size.
 *
 * @category combinators
 * @since 0.0.0
 */
export function formatSize(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes}B`
  } else if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)}KB`
  } else {
    return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  }
}

/**
 * Truncate content from the head (keep first N lines/bytes).
 * Suitable for file reads where you want to see the beginning.
 *
 * Never returns partial lines. If first line exceeds byte limit,
 * returns empty content with firstLineExceedsLimit=true.
 */
function truncateHeadImpl(self: string, options: TruncationOptions = {}): TruncationResult {
  return truncateHeadOf(
    self,
    { lines: splitLinesForCounting(self).length, bytes: utf8ByteLength(self) },
    options,
  )
}
/**
 * Returns complete leading lines within the requested text limits.
 *
 * @category combinators
 * @since 0.0.0
 */
export const truncateHead: {
  (options?: TruncationOptions): (self: string) => TruncationResult
  (self: string, options?: TruncationOptions): TruncationResult
} = dual((args) => typeof args[0] === 'string', truncateHeadImpl)

/**
 * `truncateHead` of a text known by a prefix and its totals (`lines` counted like `truncateHead`, ignoring a trailing
 * newline).
 *
 * **Details**
 *
 * The prefix must be the whole text, or longer than `maxBytes + 1` UTF-8 bytes, or hold at least `maxLines`
 * newlines; then the result equals `truncateHead` of the whole text.
 *
 * @category combinators
 * @since 0.0.0
 */
function truncateHeadOfImpl(
  self: string,
  totals: { readonly lines: number; readonly bytes: number },
  options: TruncationOptions = {},
): TruncationResult {
  const maxLines = options.maxLines ?? DEFAULT_MAX_LINES
  const maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES

  const totalBytes = totals.bytes
  const lines = splitLinesForCounting(self)
  const totalLines = totals.lines

  // Check if no truncation needed
  if (totalLines <= maxLines && totalBytes <= maxBytes) {
    return {
      content: self,
      truncated: false,
      truncatedBy: null,
      totalLines,
      totalBytes,
      outputLines: totalLines,
      outputBytes: totalBytes,
      lastLinePartial: false,
      firstLineExceedsLimit: false,
      maxLines,
      maxBytes,
    }
  }

  // Check if first line alone exceeds byte limit
  const firstLineBytes = utf8ByteLength(lines[0] ?? '')
  if (firstLineBytes > maxBytes) {
    return {
      content: '',
      truncated: true,
      truncatedBy: 'bytes',
      totalLines,
      totalBytes,
      outputLines: 0,
      outputBytes: 0,
      lastLinePartial: false,
      firstLineExceedsLimit: true,
      maxLines,
      maxBytes,
    }
  }

  // Collect complete lines that fit
  const outputLinesArr: Array<string> = []
  let outputBytesCount = 0
  let truncatedBy: 'lines' | 'bytes' = 'lines'

  for (let i = 0; i < lines.length && i < maxLines; i++) {
    const line = lines[i] ?? ''
    const lineBytes = utf8ByteLength(line) + (i > 0 ? 1 : 0) // +1 for newline

    if (outputBytesCount + lineBytes > maxBytes) {
      truncatedBy = 'bytes'
      break
    }

    outputLinesArr.push(line)
    outputBytesCount += lineBytes
  }

  // Without a byte break, only omitted lines prove the line limit was reached; otherwise a trailing newline exceeded bytes.
  if (truncatedBy !== 'bytes') truncatedBy = outputLinesArr.length < totalLines ? 'lines' : 'bytes'

  const outputContent = outputLinesArr.join('\n')
  const finalOutputBytes = utf8ByteLength(outputContent)

  return {
    content: outputContent,
    truncated: true,
    truncatedBy,
    totalLines,
    totalBytes,
    outputLines: outputLinesArr.length,
    outputBytes: finalOutputBytes,
    lastLinePartial: false,
    firstLineExceedsLimit: false,
    maxLines,
    maxBytes,
  }
}

/**
 * Type contracts owned by `truncateHead`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace truncateHead {
  /**
   * truncateHead result type contract.
   *
   * @category models
   * @since 0.0.0
   */
  interface Result {
    /** The truncated content */
    readonly content: string
    /** Whether truncation occurred */
    readonly truncated: boolean
    /** Which limit was hit: "lines", "bytes", or null if not truncated */
    readonly truncatedBy: 'lines' | 'bytes' | null
    /** Total number of lines in the original content */
    readonly totalLines: number
    /** Total number of bytes in the original content */
    readonly totalBytes: number
    /** Number of complete lines in the truncated output */
    readonly outputLines: number
    /** Number of bytes in the truncated output */
    readonly outputBytes: number
    /** Whether the last line was partially truncated (only for tail truncation edge case) */
    readonly lastLinePartial: boolean
    /** Whether the first line exceeded the byte limit (for head truncation) */
    readonly firstLineExceedsLimit: boolean
    /** The max lines limit that was applied */
    readonly maxLines: number
    /** The max bytes limit that was applied */
    readonly maxBytes: number
  }
  /**
   * Configuration accepted by truncateHead.
   *
   * @category models
   * @since 0.0.0
   */
  interface Options {
    /** Maximum number of lines (default: 2000) */
    readonly maxLines?: number | undefined
    /** Maximum number of bytes (default: 50KB) */
    readonly maxBytes?: number | undefined
  }
}

/**
 * Truncates a known leading text window using complete original byte and line totals.
 *
 * @category combinators
 * @since 0.0.0
 */
export const truncateHeadOf: {
  (
    totals: { readonly lines: number; readonly bytes: number },
    options?: TruncationOptions,
  ): (self: string) => TruncationResult
  (
    self: string,
    totals: { readonly lines: number; readonly bytes: number },
    options?: TruncationOptions,
  ): TruncationResult
} = dual((args) => typeof args[0] === 'string', truncateHeadOfImpl)
