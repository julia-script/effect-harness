import * as ByteSize from 'effect/ByteSize'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as FileReader from '../FileReader.js'
import * as Tool from '../Tool.js'
import type * as ToolResult from '../ToolResult.js'
import * as Toolkit from '../Toolkit.js'
import { CodingFileError, FailureSchema, failureOutput, positiveInteger } from './FileError.js'
import * as FilePath from '../internal/CodingFilePath.js'
import * as LineScan from '../internal/CodingLineScan.js'

export const maxBytes = 50 * 1024
export const maxLines = 2000
export interface Options {
  /** May lower, never raise, the public 50 KiB/2000-line output guards. */
  readonly maxBytes?: number
  readonly maxLines?: number
}
export const Parameters = Schema.Struct({
  path: Schema.String,
  offset: Schema.optionalKey(Schema.Finite),
  limit: Schema.optionalKey(Schema.Finite),
  /** An absolute byte offset within offset's line, from a partial-line continuation. */
  byteOffset: Schema.optionalKey(Schema.String),
})
export type Parameters = typeof Parameters.Type
export const Outcome = Schema.Struct({
  path: Schema.String,
  offset: Schema.Finite,
  totalLines: Schema.Finite,
  fileBytes: Schema.String,
  outputLines: Schema.Finite,
  outputBytes: Schema.Finite,
  truncated: Schema.Boolean,
  continuation: Schema.optionalKey(
    Schema.Struct({
      offset: Schema.Finite,
      byteOffset: Schema.optionalKey(Schema.String),
    }),
  ),
})
export type Outcome = typeof Outcome.Type

/**
 * UTF-8 text only in this slice. Replay is safe because it causes no mutation,
 * but a replay/continuation can observe changed file contents. Image projection
 * and model image limits attach through the positional reader in a later slice.
 */
export const tool = Tool.makeResult('read', {
  description:
    'Read UTF-8 file text, bounded to 2000 lines and 50 KiB. offset is one-based; use continuation to read more.',
  parameters: Parameters,
  success: Outcome,
  failure: FailureSchema,
  replay: 'safe',
})
  .addDependency(FileReader.FileReader)
  .addDependency(FileSystem.FileSystem)
  .addDependency(Path.Path)
export const toolkit = Toolkit.make(tool)

const readerError = (cause: FileReader.FileReaderError) =>
  new CodingFileError({
    reason: 'filesystem',
    path: cause.path,
    message: cause.message,
    cause,
  })
const signature = (info: FileSystem.File.Info) => Option.map(info.mtime, (value) => value.getTime())
const unchanged = (before: FileSystem.File.Info, after: FileSystem.File.Info) =>
  ByteSize.toBigInt(before.size) === ByteSize.toBigInt(after.size) &&
  Option.getOrUndefined(signature(before)) === Option.getOrUndefined(signature(after))

/**
 * A scoped read with semantic errors visible to direct consumers. LF separates
 * lines; CRLF and trailing newlines are preserved, and a leading UTF-8 BOM is
 * removed. Allocation is bounded even for enormous lines. Invalid UTF-8 in the
 * returned window fails rather than silently replacing bytes; unshown text is
 * scanned as bytes. Scan and head use one opened identity. An in-place change
 * retries once (appends preserve the scanned prefix); this is not a file snapshot.
 */
export const read = Effect.fn('CodingRead.read')(function* (
  input: Parameters,
  options: Options = {},
) {
  const byteCap = options.maxBytes ?? maxBytes
  const lineCap = options.maxLines ?? maxLines
  const offset = input.offset ?? 1
  const limit = input.limit ?? lineCap
  yield* positiveInteger(input.path, 'maxBytes', byteCap, maxBytes)
  yield* positiveInteger(input.path, 'maxLines', lineCap, maxLines)
  yield* positiveInteger(input.path, 'offset', offset)
  yield* positiveInteger(input.path, 'limit', limit)
  const path = yield* FilePath.resolveRead(input.path)
  const service = yield* FileReader.FileReader
  const reader = yield* service.open(path).pipe(Effect.mapError(readerError))
  for (let attempt = 0; ; attempt++) {
    const before = yield* reader.stat.pipe(Effect.mapError(readerError))
    const size = ByteSize.toBigInt(before.size)
    const selection = yield* LineScan.scan(
      reader,
      path,
      size,
      offset,
      Math.min(limit, lineCap),
    ).pipe(
      Effect.mapError((cause) => (cause._tag === 'FileReaderError' ? readerError(cause) : cause)),
      Effect.result,
    )
    if (selection._tag === 'Failure') {
      if (selection.failure.reason === 'changed' && attempt === 0) continue
      return yield* selection.failure
    }
    const selected = selection.success
    let start = selected.start
    if (input.byteOffset !== undefined) {
      if (!/^(0|[1-9][0-9]*)$/.test(input.byteOffset))
        return yield* new CodingFileError({
          reason: 'invalidLimits',
          path,
          message: 'byteOffset must be a nonnegative decimal integer',
        })
      start = BigInt(input.byteOffset)
      if (start < selected.start || start > selected.firstLineEnd)
        return yield* new CodingFileError({
          reason: 'invalidLimits',
          path,
          message: 'byteOffset must be within the selected first line',
        })
    }
    if (start === 0n) {
      const prefix = yield* reader.readAt(0n, 3).pipe(Effect.mapError(readerError))
      if (prefix[0] === 0xef && prefix[1] === 0xbb && prefix[2] === 0xbf) start = 3n
    }
    const remaining = selected.end - start
    const length = Number(remaining < BigInt(byteCap + 4) ? remaining : BigInt(byteCap + 4))
    const bytes = yield* reader.readAt(start, length).pipe(Effect.mapError(readerError))
    const after = yield* reader.stat.pipe(Effect.mapError(readerError))
    if (!unchanged(before, after) && ByteSize.toBigInt(after.size) <= size) {
      if (attempt === 0) continue
      return yield* new CodingFileError({
        reason: 'changed',
        path,
        message: `${path} changed while it was read`,
      })
    }
    if (bytes.length < length) {
      if (attempt === 0) continue
      return yield* new CodingFileError({
        reason: 'changed',
        path,
        message: `${path} shrank while it was read`,
      })
    }
    let end = bytes.length
    let continuation: Outcome['continuation']
    let partial = false
    if (remaining > BigInt(byteCap)) {
      end = LineScan.characterEnd(bytes, byteCap)
      // Prefer complete lines. A huge first line returns a character-safe prefix.
      const newline = bytes.subarray(0, end + 1).lastIndexOf(10)
      if (newline >= 0) end = newline
      else partial = true
    }
    const text = yield* Effect.try({
      try: () =>
        new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes.subarray(0, end)),
      catch: (cause) =>
        new CodingFileError({
          reason: 'encoding',
          path,
          message: `${path} is not valid UTF-8 in the selected window`,
          cause,
        }),
    })
    const outputLines = text.length === 0 ? 0 : text.split('\n').length
    if (partial) {
      if (end === 0)
        return yield* new CodingFileError({
          reason: 'invalidLimits',
          path,
          message: 'maxBytes is too small for the next UTF-8 character',
        })
      continuation = { offset, byteOffset: (start + BigInt(end)).toString() }
    } else if (remaining > BigInt(byteCap)) {
      // The omitted LF begins the next line even if the preceding line is empty.
      continuation = { offset: offset + Math.max(1, outputLines) }
    } else if (offset + Math.min(limit, lineCap) <= selected.totalLines) {
      continuation = { offset: offset + Math.min(limit, lineCap) }
    }
    const outcome: Outcome = {
      path,
      offset,
      totalLines: selected.totalLines,
      fileBytes: size.toString(),
      outputLines,
      outputBytes: end,
      truncated: continuation !== undefined,
      ...(continuation === undefined ? {} : { continuation }),
    }
    let message: string | undefined
    if (continuation !== undefined) {
      message = partial
        ? `Line ${offset} exceeds the byte cap. Continue with offset=${offset}, byteOffset=${continuation.byteOffset}.`
        : `More text remains. Continue with offset=${continuation.offset}.`
    }
    return {
      content: [
        Prompt.textPart({ text }),
        ...(message === undefined ? [] : [Prompt.textPart({ text: message })]),
      ],
      structuredOutput: outcome,
      diagnostics:
        message === undefined ? [] : [{ severity: 'info', code: 'continuation', message }],
    } satisfies ToolResult.Output<Outcome>
  }
})

/** Ready-made handlers; supply bound FileSystem/Path and FileReader.layer. */
export const layer = (options: Options = {}) => {
  const captured = {
    maxBytes: options.maxBytes ?? maxBytes,
    maxLines: options.maxLines ?? maxLines,
  }
  return toolkit.toLayer({
    read: (input) => read(input, captured).pipe(Effect.mapError(failureOutput)),
  })
}
