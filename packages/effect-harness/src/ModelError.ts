/**
 * Semantic model failure reasons and their permanent wrapper.
 */
import * as Schema from 'effect/Schema'
const Payload = Schema.Struct({
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
})

/**
 * Failure reporting a provider/model reference absent from the catalogue.
 *
 * @category errors
 */
export class ModelNoModelError extends Schema.TaggedError<ModelNoModelError>(
  '@effect-harness/harness/ModelError/ModelNoModelError',
)('ModelNoModelError', Payload.fields) {
  get isRetryable(): boolean {
    return false
  }
}
/**
 * Failure reporting request options or behavior unsupported by a model descriptor.
 *
 * @category errors
 */
export class ModelUnsupportedError extends Schema.TaggedError<ModelUnsupportedError>(
  '@effect-harness/harness/ModelError/ModelUnsupportedError',
)('ModelUnsupportedError', Payload.fields) {
  get isRetryable(): boolean {
    return false
  }
}
/**
 * Failure reporting a provider response that violates the model contract.
 *
 * @category errors
 */
export class ModelInvalidResponseError extends Schema.TaggedError<ModelInvalidResponseError>(
  '@effect-harness/harness/ModelError/ModelInvalidResponseError',
)('ModelInvalidResponseError', Payload.fields) {
  get isRetryable(): boolean {
    return false
  }
}
/**
 * Schema for model error reason.
 *
 * @category schemas
 */
export const ModelErrorReason = Schema.Union([
  ModelNoModelError,
  ModelUnsupportedError,
  ModelInvalidResponseError,
])
/**
 * Decoded value validated by the `ModelErrorReason` schema.
 *
 * @category models
 */
export type ModelErrorReason = typeof ModelErrorReason.Type
/**
 * Structured model-selection, capability or response failure.
 *
 * **Details**
 *
 * The reason discriminator separates missing models, unsupported options and invalid
 * responses. Provider request errors may instead retain their native AiError contract.
 *
 * @category errors
 */
export class ModelError extends Schema.TaggedError<ModelError>(
  '@effect-harness/harness/ModelError/ModelError',
)('ModelError', { reason: ModelErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
}
