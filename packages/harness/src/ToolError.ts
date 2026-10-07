/**
 * Semantic tool failure reasons with preserved caught causes.
 *
 * @since 0.0.0
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
/**
 * Semantic tool unavailable with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolUnavailable extends Schema.TaggedError<ToolUnavailable>(
  '@effect-harness/harness/ToolError/ToolUnavailable',
)('ToolUnavailable', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Semantic tool blocked with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolBlocked extends Schema.TaggedError<ToolBlocked>(
  '@effect-harness/harness/ToolError/ToolBlocked',
)('ToolBlocked', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Semantic tool invalid parameters with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolInvalidParameters extends Schema.TaggedError<ToolInvalidParameters>(
  '@effect-harness/harness/ToolError/ToolInvalidParameters',
)('ToolInvalidParameters', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Semantic tool execution with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolExecution extends Schema.TaggedError<ToolExecution>(
  '@effect-harness/harness/ToolError/ToolExecution',
)('ToolExecution', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Semantic tool invalid result with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolInvalidResult extends Schema.TaggedError<ToolInvalidResult>(
  '@effect-harness/harness/ToolError/ToolInvalidResult',
)('ToolInvalidResult', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Semantic tool interrupted with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolInterrupted extends Schema.TaggedError<ToolInterrupted>(
  '@effect-harness/harness/ToolError/ToolInterrupted',
)('ToolInterrupted', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Schema for tool error reason.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolErrorReason = Schema.Union([
  ToolUnavailable,
  ToolBlocked,
  ToolInvalidParameters,
  ToolExecution,
  ToolInvalidResult,
  ToolInterrupted,
])
/**
 * ToolError tool error reason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolErrorReason = typeof ToolErrorReason.Type
/**
 * Semantic tool error with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolError extends Schema.TaggedError<ToolError>(
  '@effect-harness/harness/ToolError/ToolError',
)('ToolError', { reason: ToolErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  override get name(): string {
    return this.reason.name
  }
}
