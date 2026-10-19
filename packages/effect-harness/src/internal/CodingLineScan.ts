import * as Effect from 'effect/Effect'
import type * as FileReader from '../FileReader.js'
import { CodingFileError } from '../coding/FileError.js'

const chunkBytes = 64 * 1024

/** LF-separated line selection, matching split('\n') including the terminal empty line. */
export const scan = Effect.fn('CodingRead.scanLines')(function* (
  reader: FileReader.Reader,
  path: string,
  size: bigint,
  offset: number,
  limit: number,
) {
  let position = 0n
  let line = 1n
  let start = offset === 1 ? 0n : undefined
  let end = size
  let firstLineEnd = size
  const first = BigInt(offset)
  const last = first + BigInt(limit) - 1n
  while (position < size) {
    const count = Number(
      size - position < BigInt(chunkBytes) ? size - position : BigInt(chunkBytes),
    )
    const bytes = yield* reader.readAt(position, count)
    if (bytes.length === 0)
      return yield* new CodingFileError({
        reason: 'changed',
        path,
        message: `${path} shrank while it was read`,
      })
    for (let i = 0; i < bytes.length; i++) {
      if (bytes[i] !== 10) continue
      const boundary = position + BigInt(i)
      if (line === first) firstLineEnd = boundary
      if (line === last) end = boundary
      line++
      if (line === first) start = boundary + 1n
    }
    position += BigInt(bytes.length)
  }
  if (start === undefined)
    return yield* new CodingFileError({
      reason: 'beyondEOF',
      path,
      message: `Offset ${offset} is beyond end of file (${line} lines total)`,
    })
  if (line > BigInt(Number.MAX_SAFE_INTEGER))
    return yield* new CodingFileError({
      reason: 'invalidLimits',
      path,
      message: 'File line count exceeds safe integer range',
    })
  return { start, end, firstLineEnd, totalLines: Number(line) }
})

/** Trim only a cut UTF-8 code point, then let the fatal decoder validate the retained bytes. */
export const characterEnd = (bytes: Uint8Array, cap: number): number => {
  const end = Math.min(bytes.length, cap)
  if (end === bytes.length || end === 0) return end
  let start = end - 1
  while (start > 0 && (bytes[start]! & 0xc0) === 0x80) start--
  const first = bytes[start]!
  let width = 1
  if (first >= 0xc2 && first <= 0xdf) width = 2
  else if (first >= 0xe0 && first <= 0xef) width = 3
  else if (first >= 0xf0 && first <= 0xf4) width = 4
  return start + width > end ? start : end
}
