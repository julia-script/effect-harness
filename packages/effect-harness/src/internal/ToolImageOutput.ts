import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Images from '../ImagePreparation.js'
import type * as ToolResult from '../ToolResult.js'
import { detect } from './ImageFormat.js'

/** A replacement preparer returned content that violates the declared model limits. */
export class InvalidImageOutput extends Schema.TaggedError<InvalidImageOutput>()(
  'InvalidImageOutput',
  {
    reason: Schema.Literals(['outputBudget', 'invalidOutput']),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

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
export const Metadata = Schema.Struct({
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
export type Metadata = typeof Metadata.Type

/** Shared result boundary for file readers and explicitly prepared custom-tool images. */
export const prepare = Effect.fn('ToolImageOutput.prepare')(function* (
  bytes: Uint8Array,
  mimeType: string,
  inputLimits: Images.Limits,
) {
  const limits = yield* Images.validateLimits(inputLimits)
  const prepared = yield* Images.prepare(bytes, mimeType, limits)
  // Check the replaceable service before the tool JSON codec allocates base64.
  const base64Bytes = Images.base64Length(prepared.bytes.byteLength)
  if (!Number.isSafeInteger(base64Bytes) || base64Bytes > limits.maxBytes)
    return yield* new InvalidImageOutput({
      reason: 'outputBudget',
      message: 'Prepared image exceeds the model base64 budget',
    })
  if (
    detect(prepared.bytes) !== prepared.mimeType ||
    prepared.dimensions.width > limits.maxWidth ||
    prepared.dimensions.height > limits.maxHeight
  )
    return yield* new InvalidImageOutput({
      reason: 'invalidOutput',
      message: 'Prepared image MIME or dimensions violate model policy',
    })
  const { bytes: output, ...metadata } = prepared
  const outcome = yield* Schema.decodeEffect(Metadata)({
    ...metadata,
    outputBytes: output.byteLength,
    base64Bytes,
  }).pipe(
    Effect.mapError(
      (cause) =>
        new InvalidImageOutput({
          reason: 'invalidOutput',
          message: 'Invalid prepared image metadata',
          cause,
        }),
    ),
  )
  const notes = [`Prepared image (${outcome.mimeType}).`]
  if (outcome.convertedFrom !== undefined) notes.push(`Converted from ${outcome.convertedFrom}.`)
  if (outcome.resized !== undefined) {
    const { from, to } = outcome.resized
    notes.push(
      `Resized from ${from.width}x${from.height} to ${to.width}x${to.height}. Map coordinates to the upright source by multiplying x by ${from.width / to.width} and y by ${from.height / to.height}.`,
    )
  }
  if (outcome.orientation !== 1)
    notes.push(
      `Normalized orientation ${outcome.orientation}; source coordinates refer to the upright image.`,
    )
  const message = notes.join(' ')
  return {
    content: [
      Prompt.filePart({ mediaType: outcome.mimeType, data: output }),
      Prompt.textPart({ text: message }),
    ],
    structuredOutput: outcome,
    diagnostics: [{ severity: 'info', code: 'image', message }],
  } satisfies ToolResult.StrictOutput<Metadata>
})
