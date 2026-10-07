/**
 * Output conversion failures with their original causes.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
/**
 * Semantic output failure with its retained cause.
 *
 * @category errors
 */
export class OutputFailure extends Schema.TaggedError<OutputFailure>(
  '@effect-harness/harness/OutputError/OutputFailure',
)('OutputFailure', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
/**
 * Schema for output error reason.
 *
 * @category schemas
 */
export const OutputErrorReason = Schema.Union([OutputFailure])
/**
 * Decoded value validated by the `OutputErrorReason` schema.
 *
 * @category models
 */
export type OutputErrorReason = typeof OutputErrorReason.Type
/**
 * Semantic output error with its retained cause.
 *
 * @category errors
 */
export class OutputError extends Schema.TaggedError<OutputError>(
  '@effect-harness/harness/OutputError/OutputError',
)('OutputError', { reason: OutputErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
