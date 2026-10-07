/**
 * Scoped text and image reading through portable Env capabilities.
 */
import * as Option from 'effect/Option'
import * as DateTime from 'effect/DateTime'
// Read selection/truncation adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
import * as SchemaField from '../SchemaField.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
// effect-review-allow P9-namespace-alias-equals-module: effect/ai/Tool and ../Tool.ts both bind Tool; AiTool preserves the checked imported-name collision.
import * as Prompt from 'effect/ai/Prompt'
import { Env, type BinaryReader, type FileInfo } from '../Env.ts'
import { ToolError, ToolExecution } from '../ToolError.ts'
import { Invocation, Result, type ToolResult, type Diagnostic } from '../Invocation.ts'
import * as Metadata from '../Tool.ts'
// effect-review-allow P9-namespace-alias-equals-module: ../Tool.ts and effect/ai/Tool both bind Tool; Metadata preserves the checked imported-name collision.
import { characterEnd } from '../Output.ts'
import { rangeDecoder, hasBom } from '../env/Decode.ts'
import * as Image from './Image.ts'
import * as path from './internal/path.ts'
import * as Truncate from './Truncate.ts'
/**
 * Schema for file path and optional one-based text window.
 *
 * @category schemas
 */
export const Parameters = Schema.Struct({
  path: Schema.String,
  offset: SchemaField.optional(Schema.Finite),
  limit: SchemaField.optional(Schema.Finite),
})
/**
 * Decoded parameters passed to the coding-tool handler.
 *
 * @category models
 */
export type Input = Parameters
/**
 * Native read tool for bounded text line windows.
 *
 * **Details**
 *
 * Paths resolve through Env. Text windows use one-based offsets and library byte/line
 * limits.
 *
 * **Gotchas**
 *
 * Recognized images return unsupported_image; this tool does not decode or resize them.
 *
 * @category constants
 */
export const tool = AiTool.make('read', {
  description:
    'Read text files; first 2000 lines or 50KB. Continue large files with offset/limit. Recognized images are unsupported.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .annotate(Metadata.Metadata, {
    replay: 'unsafe',
    project: (result) => Metadata.decodeResult('read', result),
  })
const sliceIndex = (value: number): number => (Number.isNaN(value) ? 0 : Math.trunc(value))
const readHead = Effect.fnUntraced(function* (
  reader: BinaryReader,
  start: number,
  end: number,
  bom: boolean,
) {
  const decoder = rangeDecoder()
  let text = ''
  let newlines = 0
  for (let position = bom && start === 0 ? 3 : start; position < end;) {
    const bytes = yield* reader.read(position, Math.min(65536, end - position))
    if (bytes.length === 0) break
    position += bytes.length
    const decoded = decoder.decode(bytes, { stream: true })
    text += decoded
    for (const character of decoded) if (character === '\n') newlines++
    if (
      newlines >= Truncate.DEFAULT_MAX_LINES ||
      Truncate.utf8ByteLength(text) > Truncate.DEFAULT_MAX_BYTES + 1
    )
      return text
  }
  return text + decoder.decode()
})
const readText = Effect.fnUntraced(function* (
  reader: BinaryReader,
  info: FileInfo,
  input: Input,
): Effect.fn.Return<ToolResult, import('../Env.ts').FileError | ToolError> {
  const { path, offset, limit } = input
  const mime = yield* Image.detectSupportedImageMimeTypeOf({ size: info.size, read: reader.read })
  if (Option.isSome(mime))
    return {
      content: [],
      isError: true,
      diagnostics: [
        {
          kind: 'unsupported_image',
          detail: { severity: 'error' },
          message: `${path} is an image (${mime.value}); reading images is not supported`,
        },
      ],
    }
  const startLine = offset ? Math.max(0, offset - 1) : 0
  const display = startLine + 1
  const sliceStart = sliceIndex(startLine)
  const scanStart = Number.isSafeInteger(sliceStart) ? sliceStart : 0
  const requestedEnd =
    limit === undefined ? undefined : Math.max(scanStart + 1, sliceIndex(startLine + limit))
  const scanEnd =
    requestedEnd !== undefined && Number.isSafeInteger(requestedEnd) ? requestedEnd : undefined
  const scanOf = (endLine: number | undefined) =>
    reader.scanLines({ startLine: scanStart, ...(endLine === undefined ? {} : { endLine }) })
  let scan = yield* scanOf(scanEnd)
  const total = scan.newlines + 1
  if (startLine >= total)
    return yield* new ToolError({
      reason: new ToolExecution({
        name: 'read',
        message: `Offset ${offset} is beyond end of file (${total} lines total)`,
      }),
    })
  let userLimited: number | undefined
  let count = total - sliceStart
  if (limit !== undefined) {
    const endLine = Math.min(startLine + limit, total)
    userLimited = endLine - startLine
    const relative = sliceIndex(endLine)
    const end = relative < 0 ? Math.max(total + relative, 0) : relative
    count = Math.max(0, end - sliceStart)
    if (count > 0 && relative < 0) scan = yield* scanOf(end)
  }
  const empty = count === 0
  const endsWithNewline =
    !empty && scan.lastLineStart === scan.end && scan.lastLineStart > scan.start
  const totals = {
    lines: empty || scan.selectedBytes === 0 ? 0 : count - (endsWithNewline ? 1 : 0),
    bytes: empty ? 0 : scan.selectedBytes,
  }
  const first = yield* reader.read(0, 3)
  const head = empty ? '' : yield* readHead(reader, scan.start, scan.end, hasBom(first))
  const { content, ...truncation } = Truncate.truncateHeadOf(head, totals)
  let text = content
  const diagnostics: Array<Diagnostic> = []
  let details: Schema.Json | undefined
  if (truncation.firstLineExceedsLimit) {
    const integral = Number.isInteger(startLine)
    const bytes = new TextEncoder().encode(integral ? (head.split('\n')[0] ?? '') : '')
    const size = integral ? scan.firstLineBytes : 0
    const end = characterEnd(bytes, Truncate.DEFAULT_MAX_BYTES)
    text = new TextDecoder().decode(bytes.subarray(0, end))
    diagnostics.push({
      kind: 'truncated',
      detail: { severity: 'warn' },
      message: `Line ${display} is ${Truncate.formatSize(size)}, exceeds the ${Truncate.formatSize(Truncate.DEFAULT_MAX_BYTES)} limit; showing its first ${Truncate.formatSize(end)}. Use bash: sed -n '${display}p' ${path} | tail -c +${end + 1}`,
    })
    details = { truncation: { ...truncation, outputBytes: end, outputLines: 1 } }
  } else if (truncation.truncated) {
    const last = display + truncation.outputLines - 1
    const limitText =
      truncation.truncatedBy === 'lines'
        ? ''
        : ` (${Truncate.formatSize(Truncate.DEFAULT_MAX_BYTES)} limit)`
    diagnostics.push({
      kind: 'truncated',
      detail: { severity: 'info' },
      message: `Showing lines ${display}-${last} of ${total}${limitText}. Use offset=${last + 1} to continue.`,
    })
    details = { truncation }
  } else if (userLimited !== undefined && startLine + userLimited < total)
    diagnostics.push({
      kind: 'continuation',
      detail: { severity: 'info' },
      message: `${total - (startLine + userLimited)} more lines in file. Use offset=${startLine + userLimited + 1} to continue.`,
    })
  return {
    content: text === '' ? [] : [Prompt.textPart({ text })],
    ...(details === undefined ? {} : { details }),
    diagnostics,
  }
})
class FileChanged extends Schema.TaggedError<FileChanged>(
  '@effect-harness/harness/tools/Read/FileChanged',
)('FileChanged', {}) {}
/**
 * Reads bounded text or image data and retries a changed inode once within the same reader scope.
 *
 * @category combinators
 */
export const handler = Effect.fnUntraced(function* (
  input: Input,
): Effect.fn.Return<ToolResult, ToolError, Env | Invocation> {
  const env = yield* Env
  const absolute = yield* path.resolveRead(input.path).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolExecution({ name: 'read', message: cause.message, cause: cause }),
        }),
    ),
  )
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const reader = yield* env.openBinaryReader(absolute)
      const attempt = Effect.gen(function* () {
        const before = yield* reader.info
        const result = yield* readText(reader, before, input)
        const after = yield* reader.info
        if (
          after.size > before.size ||
          (after.size === before.size && DateTime.Equivalence(after.mtimeMs, before.mtimeMs))
        )
          return result
        return yield* new FileChanged({})
      })
      return yield* attempt.pipe(
        Effect.retry({ times: 1, while: (error) => error instanceof FileChanged }),
        Effect.catchIf(
          (error) => error instanceof FileChanged,
          () =>
            Effect.fail(
              new ToolError({
                reason: new ToolExecution({
                  name: 'read',
                  message: `${input.path} changed while it was read`,
                }),
              }),
            ),
        ),
      )
    }),
  ).pipe(
    Effect.mapError((cause) =>
      cause instanceof ToolError
        ? cause
        : new ToolError({
            reason: new ToolExecution({ name: 'read', message: cause.message, cause: cause }),
          }),
    ),
  )
})

/**
 * Checks whether a value satisfies the decoded `Parameters` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isInput: (u: unknown) => u is Parameters = Schema.is(Parameters)

/**
 * File path and optional one-based text window.
 *
 * @category models
 */
export type Parameters = typeof Parameters.Type
