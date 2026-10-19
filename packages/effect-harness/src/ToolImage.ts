import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as ImageReadPolicy from './coding/ImageReadPolicy.js'
import * as ImageOutput from './internal/ToolImageOutput.js'

export { Metadata, InvalidImageOutput } from './internal/ToolImageOutput.js'

/** Explicit image preparation cannot succeed for a model without image input. */
export class ImageModelUnsupported extends Schema.TaggedError<ImageModelUnsupported>()(
  'ImageModelUnsupported',
  {
    message: Schema.String,
  },
) {}

/**
 * Prepare custom-tool bytes as multimodal content with serializable resize metadata.
 *
 * Uses the same ImagePreparation and coding/ImageReadPolicy services as Read.withImages.
 * ToolExecution's request-pinned policy takes precedence over the bound fallback resolver.
 * Limits are explicit: maxBytes counts padded base64, independently of text OutputRetention.
 * Fails for unsupported vision or preparation errors; never silently omits an image.
 * Only tools that call this helper are processed. Supply both services as tool dependencies.
 */
export const prepare = Effect.fn('ToolImage.prepare')(function* (
  bytes: Uint8Array,
  mimeType: string,
) {
  const policy = yield* ImageReadPolicy.current
  if (!policy.vision)
    return yield* new ImageModelUnsupported({
      message: 'The current model does not support images',
    })
  return yield* ImageOutput.prepare(bytes, mimeType, policy.limits)
})
