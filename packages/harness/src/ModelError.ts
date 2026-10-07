/**
 * Semantic model failure reasons and their permanent wrapper.
 *
 * @since 0.0.0
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import * as Usage from './Usage.ts'
/**
 * Semantic model no model with its retained cause.
 *
 * @category errors
 * @since 0.0.0
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
 * Semantic model unsupported with its retained cause.
 *
 * @category errors
 * @since 0.0.0
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
 * Semantic model invalid response with its retained cause.
 *
 * @category errors
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const ModelErrorReason = Schema.Union([ModelNoModel, ModelUnsupported, ModelInvalidResponse])
/**
 * ModelError model error reason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ModelErrorReason = typeof ModelErrorReason.Type
/**
 * Semantic model error with its retained cause.
 *
 * @category errors
 * @since 0.0.0
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
