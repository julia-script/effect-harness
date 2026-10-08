/**
 * Extension callback failures with their original causes.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
/**
 * Semantic hook failure with its retained cause.
 *
 * @category errors
 */
export class HookFailureError extends Schema.TaggedError<HookFailureError>(
  '@effect-harness/harness/HookError/HookFailureError',
)('HookFailureError', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
/**
 * Schema for hook error reason.
 *
 * @category schemas
 */
export const HookErrorReason = Schema.Union([HookFailureError])
/**
 * Decoded value validated by the `HookErrorReason` schema.
 *
 * @category models
 */
export type HookErrorReason = typeof HookErrorReason.Type
/**
 * Semantic hook error with its retained cause.
 *
 * @category errors
 */
export class HookError extends Schema.TaggedError<HookError>(
  '@effect-harness/harness/HookError/HookError',
)('HookError', { reason: HookErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
}
