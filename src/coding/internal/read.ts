/**
 * The `read` handler, ported from pi's `tools/read.ts`: text by lines within the limits, or one
 * image, prepared by the `ImageProcessor` in context when there is one. Remarks about truncation and continuation are diagnostics; the content is only file text,
 * or the image.
 *
 * Unlike pi, it reads the whole file and then selects lines, which gives the same result.
 */
import { Effect, Option } from 'effect'
import { DEFAULT_MAX_BYTES } from '../CodingTools.ts'
import type { Config } from '../CodingTools.ts'
import type { Diagnostic, Output } from '../../tool/ToolResult.ts'
import { output, text } from '../../tool/ToolResult.ts'
import * as ImageProcessor from '../ImageProcessor.ts'
import * as Image from './image.ts'
import { characterEnd, formatSize, truncateHead, utf8ByteLength } from './truncate.ts'
import type { Truncation } from './truncate.ts'
import type { Workspace } from './workspace.ts'
import { resolveExisting } from './workspace.ts'

export interface Params {
  readonly path: string
  readonly offset?: number
  readonly limit?: number
}

const failed = (message: string, code?: string): Output =>
  output([], {
    diagnostics: [{ severity: 'error', message, ...(code === undefined ? {} : { code }) }],
  })

const imageError = (message: string) => failed(message, 'unsupported_image')

/** One image block, with what happened to it as an `info` diagnostic. */
const imageOutput = (
  image: { readonly data: string; readonly mimeType: string },
  notes: ReadonlyArray<string> = [],
): Output => {
  const message = [`Read image file [${image.mimeType}].`, ...notes].join(' ')
  const diagnostics: ReadonlyArray<Diagnostic> = [{ severity: 'info', code: 'image', message }]
  return output([{ type: 'image', data: image.data, mimeType: image.mimeType }], { diagnostics })
}

/** What the processor did to the image, for the model. */
const preparedNotes = (image: ImageProcessor.Prepared) => [
  ...(image.convertedFrom === undefined
    ? []
    : [`Converted from ${image.convertedFrom} to ${image.mimeType}.`]),
  ...(image.resized === undefined
    ? []
    : [
        `Resized from ${image.resized.from.width}x${image.resized.from.height} to ${image.resized.to.width}x${image.resized.to.height}. Multiply coordinates by ${(image.resized.from.width / image.resized.to.width).toFixed(2)} to map them to the original.`,
      ]),
]

/** One image block, prepared by the processor. */
const preparedImage = (
  processor: ImageProcessor.Service,
  bytes: Uint8Array,
  path: string,
  mimeType: string,
): Effect.Effect<Output, Output> =>
  Effect.flatMap(processor.prepare(bytes, mimeType, ImageProcessor.DEFAULT_LIMITS), (image) =>
    image === undefined
      ? Effect.fail(
          imageError(`${path} is an image (${mimeType}) that cannot be prepared for the model`),
        )
      : Effect.succeed(imageOutput(image, preparedNotes(image))),
  )

/** One image block, as it is: its format must be inline and it must fit the limits. */
const rawImage = (
  bytes: Uint8Array,
  path: string,
  mimeType: string,
): Effect.Effect<Output, Output> => {
  const unshrinkable = 'and no image processor is configured to shrink it'
  const limits = ImageProcessor.DEFAULT_LIMITS
  if (!Image.INLINE_IMAGE_TYPES.has(mimeType))
    return Effect.fail(
      imageError(
        `${path} is an image (${mimeType}) that needs converting, and no image processor is configured`,
      ),
    )
  if (Image.base64Length(bytes.length) > limits.maxBytes)
    return Effect.fail(
      imageError(
        `${path} is an image (${mimeType}) of ${formatSize(bytes.length)}, too large to send, ${unshrinkable}`,
      ),
    )
  const size = Image.sizeOf(bytes, mimeType)
  if (size === undefined)
    return Effect.fail(imageError(`${path} is an image (${mimeType}) whose size cannot be read`))
  if (size.width > limits.maxWidth || size.height > limits.maxHeight)
    return Effect.fail(
      imageError(
        `${path} is an image (${mimeType}) of ${size.width}x${size.height}, larger than ${limits.maxWidth}x${limits.maxHeight}, ${unshrinkable}`,
      ),
    )
  return Effect.succeed(imageOutput({ data: Image.toBase64(bytes), mimeType }))
}

/** `Array.prototype.slice`'s conversion of an index. */
const sliceIndex = (value: number) => (Number.isNaN(value) ? 0 : Math.trunc(value))

const decoder = new TextDecoder()

/** The details and diagnostic of a cut that kept whole lines. */
const truncatedNotes = (cut: Truncation, start: number, total: number) => {
  const end = start + cut.outputLines - 1
  const limit = cut.truncatedBy === 'lines' ? '' : ` (${formatSize(DEFAULT_MAX_BYTES)} limit)`
  const message = `Showing lines ${start}-${end} of ${total}${limit}. Use offset=${end + 1} to continue.`
  return { severity: 'info' as const, code: 'truncated', message }
}

/** The first line alone exceeds the limit: its start, cut on a character boundary. */
const longLine = (line: string, start: number, path: string) => {
  const bytes = new TextEncoder().encode(line)
  const end = characterEnd(bytes, DEFAULT_MAX_BYTES)
  const diagnostic: Diagnostic = {
    severity: 'warn',
    code: 'truncated',
    message: `Line ${start} is ${formatSize(bytes.length)}, exceeds the ${formatSize(DEFAULT_MAX_BYTES)} limit; showing its first ${formatSize(end)}. Use bash: sed -n '${start}p' ${path} | tail -c +${end + 1}`,
  }
  return { shown: decoder.decode(bytes.subarray(0, end)), diagnostic }
}

/** The lines `params` select of `content`, within the limits. */
const readText = (content: string, path: string, params: Params): Effect.Effect<Output, Output> => {
  const lines = content.split('\n')
  const startLine =
    params.offset === undefined || params.offset === 0 ? 0 : Math.max(0, params.offset - 1)
  if (startLine >= lines.length)
    return Effect.fail(
      text(`Offset ${params.offset} is beyond end of file (${lines.length} lines total)`),
    )
  const endLine =
    params.limit === undefined ? undefined : Math.min(startLine + params.limit, lines.length)
  const selected = lines.slice(
    sliceIndex(startLine),
    endLine === undefined ? undefined : sliceIndex(endLine),
  )
  const cut = truncateHead(selected.join('\n'))
  const start = startLine + 1
  const { content: _shown, ...truncation } = cut
  if (cut.firstLineExceedsLimit) {
    const { shown, diagnostic } = longLine(
      Number.isInteger(startLine) ? (selected[0] ?? '') : '',
      start,
      path,
    )
    const details = {
      truncation: { ...truncation, outputBytes: utf8ByteLength(shown), outputLines: 1 },
    }
    return Effect.succeed(text(shown, { diagnostics: [diagnostic], details }))
  }
  if (cut.truncated) {
    const diagnostics = [truncatedNotes(cut, start, lines.length)]
    return Effect.succeed(text(cut.content, { diagnostics, details: { truncation } }))
  }
  if (endLine !== undefined && endLine < lines.length) {
    const message = `${lines.length - endLine} more lines in file. Use offset=${endLine + 1} to continue.`
    return Effect.succeed(text(cut.content, { diagnostics: [{ severity: 'info', message }] }))
  }
  return Effect.succeed(text(cut.content))
}

/** Strip a UTF-8 byte order mark. */
const withoutBom = (bytes: Uint8Array) =>
  bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf ? bytes.subarray(3) : bytes

export const read = Effect.fnUntraced(function* (
  workspace: Workspace,
  params: Params,
  config: Config,
) {
  const absolute = yield* resolveExisting(workspace, config, params.path)
  const bytes = yield* Effect.mapError(workspace.fs.readFile(absolute), (error) =>
    failed(`Could not read ${params.path}: ${error.message}`),
  )
  const mimeType = Image.mimeTypeOf(bytes)
  if (mimeType !== undefined)
    return yield* Option.match(workspace.images, {
      onNone: () => rawImage(bytes, params.path, mimeType),
      onSome: (processor) => preparedImage(processor, bytes, params.path, mimeType),
    })
  return yield* readText(decoder.decode(withoutBom(bytes)), params.path, params)
})
