import * as ByteSize from 'effect/ByteSize'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import type * as FileReader from '../FileReader.js'
import * as Images from '../ImagePreparation.js'
import type * as ToolResult from '../ToolResult.js'
import type { Policy } from './ImageReadPolicy.js'
import { CodingFileError, positiveInteger } from './FileError.js'

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

const Positive = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
const Dimensions = Schema.Struct({ width: Positive, height: Positive })
const InputMime = Schema.Literals([
  'image/png',
  'image/jpeg',
  'image/gif',
  'image/webp',
  'image/bmp',
])
export const Outcome = Schema.Struct({
  kind: Schema.Literal('image'),
  path: Schema.String,
  fileBytes: Schema.String,
  mimeType: Schema.Literals(['image/png', 'image/jpeg', 'image/gif', 'image/webp']),
  outputBytes: Positive,
  base64Bytes: Positive,
  original: Dimensions,
  oriented: Dimensions,
  dimensions: Dimensions,
  orientation: Schema.Literals([1, 2, 3, 4, 5, 6, 7, 8]),
  passthrough: Schema.Boolean,
  animation: Schema.Literals(['unchanged', 'first-frame', 'static']),
  transparency: Schema.Literals(['unchanged', 'preserved', 'flattened-white', 'opaque']),
  convertedFrom: Schema.optionalKey(InputMime),
  resized: Schema.optionalKey(Schema.Struct({ from: Dimensions, to: Dimensions })),
})
export type Outcome = typeof Outcome.Type
export const Placeholder = Schema.Struct({
  kind: Schema.Literal('image-placeholder'),
  path: Schema.String,
  fileBytes: Schema.String,
  mimeType: InputMime,
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
const prefix = (bytes: Uint8Array, signature: ReadonlyArray<number>, offset = 0) =>
  signature.every((value, index) => bytes[offset + index] === value)
const ascii = (bytes: Uint8Array, value: string, offset = 0) =>
  prefix(
    bytes,
    Array.from(value, (character) => character.charCodeAt(0)),
    offset,
  )

/** Only bounded header bytes are inspected; full content validation belongs to ImagePreparation. */
export const detect = (bytes: Uint8Array): string | undefined => {
  if (prefix(bytes, [0x89, 0x50, 0x4e, 0x47])) return 'image/png'
  if (prefix(bytes, [0xff, 0xd8])) return 'image/jpeg'
  if (ascii(bytes, 'GIF8')) return 'image/gif'
  if (ascii(bytes, 'RIFF') && ascii(bytes, 'WEBP', 8)) return 'image/webp'
  if (ascii(bytes, 'BM')) return 'image/bmp'
  if (prefix(bytes, [0x49, 0x49, 0x2a, 0]) || prefix(bytes, [0x4d, 0x4d, 0, 0x2a]))
    return 'image/tiff'
  if (prefix(bytes, [0, 0, 1, 0])) return 'image/x-icon'
  if (ascii(bytes, 'ftyp', 4)) return 'image/unsupported-container'
  return undefined
}
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
  const prepared = yield* Images.prepare(bytes, mimeType, limits)
  // Recheck replaceable service output before the tool's JSON codec allocates base64.
  const base64Bytes = Images.base64Length(prepared.bytes.byteLength)
  if (!Number.isSafeInteger(base64Bytes) || base64Bytes > limits.maxBytes)
    return yield* new ImageReadError({
      reason: 'outputBudget',
      path,
      message: 'Prepared image exceeds the model base64 budget',
    })
  if (
    detect(prepared.bytes) !== prepared.mimeType ||
    prepared.dimensions.width > limits.maxWidth ||
    prepared.dimensions.height > limits.maxHeight
  )
    return yield* new ImageReadError({
      reason: 'invalidOutput',
      path,
      message: 'Prepared image MIME or dimensions violate model policy',
    })
  const { bytes: output, ...metadata } = prepared
  const outcome = yield* Schema.decodeEffect(Outcome)({
    ...metadata,
    kind: 'image',
    path,
    fileBytes: size.toString(),
    outputBytes: output.byteLength,
    base64Bytes,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new ImageReadError({
          reason: 'invalidOutput',
          path,
          message: 'Invalid prepared image metadata',
          cause,
        }),
    ),
  )
  const notes = [`Read image file (${outcome.mimeType}).`]
  if (outcome.convertedFrom !== undefined) notes.push(`Converted from ${outcome.convertedFrom}.`)
  if (outcome.resized !== undefined)
    notes.push(
      `Resized from ${outcome.resized.from.width}x${outcome.resized.from.height} to ${outcome.resized.to.width}x${outcome.resized.to.height}.`,
    )
  if (outcome.orientation !== 1) notes.push(`Normalized orientation ${outcome.orientation}.`)
  const message = notes.join(' ')
  return {
    content: [
      Prompt.filePart({ mediaType: outcome.mimeType, data: output }),
      Prompt.textPart({ text: message }),
    ],
    structuredOutput: outcome,
    diagnostics: [{ severity: 'info', code: 'image', message }],
  } satisfies ToolResult.Output<Outcome>
})
