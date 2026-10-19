import * as ByteSize from 'effect/ByteSize'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as FileReader from '../FileReader.js'
import * as Images from '../ImagePreparation.js'
import type * as ToolResult from '../ToolResult.js'
import * as ImageOutput from '../internal/ToolImageOutput.js'
import { detect } from '../internal/ImageFormat.js'
import type { Policy } from './ImageReadPolicy.js'
import { CodingFileError, positiveInteger } from './FileError.js'

export { detect } from '../internal/ImageFormat.js'

/** Image file/adapter failures remain semantic to direct consumers. */
export class ImageReadError extends Schema.TaggedError<ImageReadError>()('ImageReadError', {
  reason: Schema.Literals([
    'sourceBudget',
    'unsupportedImage',
    'mimeMismatch',
    'preparation',
    'outputBudget',
    'invalidOutput',
  ]),
  path: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export const Failure = Schema.Struct({
  reason: ImageReadError.fields.reason,
  path: Schema.String,
  message: Schema.String,
})
export type Failure = typeof Failure.Type

export const Outcome = Schema.Struct({
  ...ImageOutput.Metadata.fields,
  kind: Schema.Literal('image'),
  path: Schema.String,
  fileBytes: Schema.String,
})
export type Outcome = typeof Outcome.Type
export const Placeholder = Schema.Struct({
  kind: Schema.Literal('image-placeholder'),
  path: Schema.String,
  fileBytes: Schema.String,
  mimeType: Schema.Literals(['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp']),
  reason: Schema.Literal('modelDoesNotSupportImages'),
})
export type Placeholder = typeof Placeholder.Type

export const failureOutput = (error: ImageReadError): ToolResult.Output<Failure> => ({
  content: [Prompt.textPart({ text: error.message })],
  structuredOutput: { reason: error.reason, path: error.path, message: error.message },
  diagnostics: [{ severity: 'error', code: error.reason, message: error.message }],
})

const readerError = (cause: FileReader.FileReaderError) =>
  new CodingFileError({ reason: 'filesystem', path: cause.path, message: cause.message, cause })
const extensionMime = (path: string): string | undefined => {
  const extension = path.slice(path.lastIndexOf('.') + 1).toLowerCase()
  switch (extension) {
    case 'png':
      return 'image/png'
    case 'jpg':
    case 'jpeg':
      return 'image/jpeg'
    case 'gif':
      return 'image/gif'
    case 'webp':
      return 'image/webp'
    case 'bmp':
      return 'image/bmp'
    case 'svg':
    case 'avif':
    case 'heic':
    case 'heif':
    case 'tif':
    case 'tiff':
    case 'ico':
      return `image/${extension}`
    default:
      return undefined
  }
}
const supported = (mime: string): mime is Images.InputMimeType =>
  mime === 'image/png' ||
  mime === 'image/jpeg' ||
  mime === 'image/gif' ||
  mime === 'image/webp' ||
  mime === 'image/bmp'

export const inspect = Effect.fn('CodingImageRead.inspect')(function* (
  reader: FileReader.Reader,
  path: string,
  size: bigint,
) {
  const bytes = yield* reader
    .readAt(0n, Number(size < 32n ? size : 32n))
    .pipe(Effect.mapError(readerError))
  const actual = detect(bytes)
  const declared = extensionMime(path)
  const mime = actual ?? declared
  if (mime === undefined) return undefined
  if (!supported(mime))
    return yield* new ImageReadError({
      reason: 'unsupportedImage',
      path,
      message: `Unsupported image format: ${mime}`,
    })
  if (actual === undefined || (declared !== undefined && declared !== actual))
    return yield* new ImageReadError({
      reason: 'mimeMismatch',
      path,
      message: 'Image header does not match the filename MIME type',
    })
  return mime
})

/** Source allocation must fit both the explicit source budget and a fixed-length Uint8Array. */
export const validateSourceBudget = (path: string, maxSourceBytes: number) =>
  positiveInteger(path, 'maxSourceBytes', maxSourceBytes, 0xffffffff)

export const read = Effect.fn('CodingImageRead.read')(function* (
  reader: FileReader.Reader,
  path: string,
  before: FileSystem.File.Info,
  mimeType: Images.InputMimeType,
  policy: Policy,
  maxSourceBytes: number,
) {
  yield* validateSourceBudget(path, maxSourceBytes)
  const size = ByteSize.toBigInt(before.size)
  if (!policy.vision) {
    const message = `Image file (${mimeType}) omitted: the current model does not support images.`
    return {
      content: [Prompt.textPart({ text: message })],
      structuredOutput: {
        kind: 'image-placeholder',
        path,
        fileBytes: size.toString(),
        mimeType,
        reason: 'modelDoesNotSupportImages',
      },
      diagnostics: [{ severity: 'info', code: 'modelDoesNotSupportImages', message }],
    } satisfies ToolResult.Output<Placeholder>
  }
  const limits = yield* Images.validateLimits(policy.limits)
  if (size <= 0n || size > BigInt(maxSourceBytes))
    return yield* new ImageReadError({
      reason: 'sourceBudget',
      path,
      message: `Image source exceeds the ${maxSourceBytes} byte allocation budget`,
    })
  // The opened handle's stat is checked before allocating any whole-source buffer.
  const bytes = new Uint8Array(Number(size))
  for (let position = 0; position < bytes.length;) {
    const length = Math.min(64 * 1024, bytes.length - position)
    const chunk = yield* reader.readAt(BigInt(position), length).pipe(Effect.mapError(readerError))
    if (chunk.length !== length)
      return yield* new CodingFileError({
        reason: 'changed',
        path,
        message: `${path} changed while it was read`,
      })
    bytes.set(chunk, position)
    position += chunk.length
  }
  if (detect(bytes) !== mimeType)
    return yield* new ImageReadError({
      reason: 'mimeMismatch',
      path,
      message: 'Image content changed MIME type after header inspection',
    })
  const output = yield* ImageOutput.prepare(bytes, mimeType, limits).pipe(
    Effect.catchTag('InvalidImageOutput', (error) =>
      Effect.fail(
        new ImageReadError({
          reason: error.reason,
          path,
          message: error.message,
          cause: error,
        }),
      ),
    ),
  )
  return {
    ...output,
    structuredOutput: {
      ...output.structuredOutput,
      kind: 'image',
      path,
      fileBytes: size.toString(),
    },
  } satisfies ToolResult.Output<Outcome>
})
