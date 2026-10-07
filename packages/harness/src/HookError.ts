/**
 * Extension callback failures with their original causes.
 *
 * @since 0.0.0
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
/**
 * Semantic hook failure with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class HookFailure extends Schema.TaggedError<HookFailure>(
  '@effect-harness/harness/HookError/HookFailure',
)('HookFailure', { message: Schema.String, cause: SchemaField.optional(Schema.Defect()) }) {}
/**
 * Schema for hook error reason.
 *
 * @category schemas
 * @since 0.0.0
 */
export const HookErrorReason = Schema.Union([HookFailure])
/**
 * HookError hook error reason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type HookErrorReason = typeof HookErrorReason.Type
/**
 * Semantic hook error with its retained cause.
 *
 * @category errors
 * @since 0.0.0
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
