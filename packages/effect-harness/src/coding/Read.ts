import * as ByteSize from 'effect/ByteSize'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as FileReader from '../FileReader.js'
import * as ImagePreparation from '../ImagePreparation.js'
import * as ImageRead from './ImageRead.js'
import * as ImageReadPolicy from './ImageReadPolicy.js'
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
 * UTF-8 text. Replay is safe because it causes no mutation, but a
 * replay/continuation can observe changed contents. withImages adds image reads.
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
  return yield* readOpened(input, options, path, reader)
})

const readOpened = Effect.fn('CodingRead.readOpened')(function* (
  input: Parameters,
  options: Options,
  path: string,
  reader: FileReader.Reader,
) {
  const byteCap = options.maxBytes ?? maxBytes
  const lineCap = options.maxLines ?? maxLines
  const offset = input.offset ?? 1
  const limit = input.limit ?? lineCap
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

export interface ImageOptions extends Options {
  /** Whole binary source allocation budget; positive safe integer at most 2^32-1. */
  readonly maxSourceBytes: number
}
export const ImageOutcome = Schema.Union([Outcome, ImageRead.Outcome, ImageRead.Placeholder])
export type ImageOutcome = typeof ImageOutcome.Type
export const ImageFailure = Schema.Union([FailureSchema, ImageRead.Failure])

/**
 * Read text or a whole image through one scoped opened identity. Text retains
 * pagination/BOM/CRLF behavior; offset/limit/byteOffset select text only. Image
 * headers and filename MIME must agree. The bound preparer validates all pixels.
 * A non-vision policy returns a text placeholder without allocating the source
 * or invoking a codec. Image changes (including appends) retry once then fail.
 */
export const readWithImages = Effect.fn('CodingRead.readWithImages')(function* (
  input: Parameters,
  policy: ImageReadPolicy.Policy,
  options: ImageOptions,
) {
  yield* ImageRead.validateSourceBudget(input.path, options.maxSourceBytes)
  yield* positiveInteger(input.path, 'maxBytes', options.maxBytes ?? maxBytes, maxBytes)
  yield* positiveInteger(input.path, 'maxLines', options.maxLines ?? maxLines, maxLines)
  yield* positiveInteger(input.path, 'offset', input.offset ?? 1)
  yield* positiveInteger(input.path, 'limit', input.limit ?? options.maxLines ?? maxLines)
  const path = yield* FilePath.resolveRead(input.path)
  const service = yield* FileReader.FileReader
  const reader = yield* service.open(path).pipe(Effect.mapError(readerError))
  for (let attempt = 0; ; attempt++) {
    const before = yield* reader.stat.pipe(Effect.mapError(readerError))
    const result = yield* Effect.gen(function* () {
      const mime = yield* ImageRead.inspect(reader, path, ByteSize.toBigInt(before.size))
      if (mime === undefined) return undefined
      return yield* ImageRead.read(reader, path, before, mime, policy, options.maxSourceBytes)
    }).pipe(Effect.result)
    const after = yield* reader.stat.pipe(Effect.mapError(readerError))
    if (
      !unchanged(before, after) ||
      (result._tag === 'Failure' &&
        result.failure._tag === 'CodingFileError' &&
        result.failure.reason === 'changed')
    ) {
      if (attempt === 0) continue
      return yield* new CodingFileError({
        reason: 'changed',
        path,
        message: `${path} changed while it was read`,
      })
    }
    if (result._tag === 'Failure') return yield* result.failure
    if (result.success !== undefined) return result.success
    return yield* readOpened(input, options, path, reader)
  }
})

/**
 * Ready-made image-aware declarations and handlers. Supply bound FileSystem,
 * Path, FileReader, ImagePreparation and ImageReadPolicy Layers. The policy's
 * current Effect runs per invocation with the caller's ToolExecution. No native
 * codec is imported here. Install either this read declaration or the text one.
 */
export const withImages = (options: ImageOptions) => {
  const captured = { ...options }
  const imageTool = Tool.makeResult('read', {
    description:
      'Read text (2000 lines/50 KiB) or PNG/JPEG/GIF/WebP/BMP images. Text supports continuation; images follow explicit model policy and source/output budgets. Models without vision receive a placeholder.',
    parameters: Parameters,
    success: ImageOutcome,
    failure: ImageFailure,
    replay: 'safe',
  })
    .addDependency(FileReader.FileReader)
    .addDependency(FileSystem.FileSystem)
    .addDependency(Path.Path)
    .addDependency(ImagePreparation.ImagePreparation)
    .addDependency(ImageReadPolicy.ImageReadPolicy)
  const imageToolkit = Toolkit.make(imageTool)
  const handlers = imageToolkit.toLayer({
    read: (input) =>
      Effect.gen(function* () {
        const resolver = yield* ImageReadPolicy.ImageReadPolicy
        const policy = yield* resolver.current
        return yield* readWithImages(input, policy, captured).pipe(
          Effect.mapError((error) => {
            if (error._tag === 'CodingFileError') return failureOutput(error)
            if (error._tag === 'ImageReadError') return ImageRead.failureOutput(error)
            return ImageRead.failureOutput(
              new ImageRead.ImageReadError({
                reason: 'preparation',
                path: input.path,
                message: `Image preparation failed: ${error._tag}`,
                cause: error,
              }),
            )
          }),
        )
      }),
  })
  return { tool: imageTool, toolkit: imageToolkit, layer: handlers }
}
