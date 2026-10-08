/**
 * Semantic model failure reasons and their permanent wrapper.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import * as Usage from './Usage.ts'
/**
 * Failure reporting a provider/model reference absent from the catalogue.
 *
 * @category errors
 */
export class ModelNoModel extends Schema.TaggedError<ModelNoModel>(
  '@effect-harness/harness/ModelError/ModelNoModel',
)('ModelNoModel', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  usage: SchemaField.optional(Usage.Usage),
}) {
  get isRetryable(): boolean {
    return false
  }
}
/**
 * Failure reporting request options or behavior unsupported by a model descriptor.
 *
 * @category errors
 */
export class ModelUnsupported extends Schema.TaggedError<ModelUnsupported>(
  '@effect-harness/harness/ModelError/ModelUnsupported',
)('ModelUnsupported', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  usage: SchemaField.optional(Usage.Usage),
}) {
  get isRetryable(): boolean {
    return false
  }
}
/**
 * Failure reporting a provider response that violates the model contract.
 *
 * @category errors
 */
export class ModelInvalidResponse extends Schema.TaggedError<ModelInvalidResponse>(
  '@effect-harness/harness/ModelError/ModelInvalidResponse',
)('ModelInvalidResponse', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  usage: SchemaField.optional(Usage.Usage),
}) {
  get isRetryable(): boolean {
    return false
  }
}
/**
 * Schema for model error reason.
 *
 * @category schemas
 */
export const ModelErrorReason = Schema.Union([ModelNoModel, ModelUnsupported, ModelInvalidResponse])
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
  get usage(): Usage.Usage | undefined {
    return this.reason.usage
  }
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
}
