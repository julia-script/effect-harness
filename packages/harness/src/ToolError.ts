/**
 * Semantic tool failure reasons with preserved caught causes.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
/**
 * Failure reporting a requested tool or local handler that is unavailable.
 *
 * @category errors
 */
export class ToolUnavailable extends Schema.TaggedError<ToolUnavailable>(
  '@effect-harness/harness/ToolError/ToolUnavailable',
)('ToolUnavailable', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Failure reporting a tool invocation blocked by hook policy.
 *
 * @category errors
 */
export class ToolBlocked extends Schema.TaggedError<ToolBlocked>(
  '@effect-harness/harness/ToolError/ToolBlocked',
)('ToolBlocked', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Failure decoding, repairing or encoding tool arguments.
 *
 * @category errors
 */
export class ToolInvalidParameters extends Schema.TaggedError<ToolInvalidParameters>(
  '@effect-harness/harness/ToolError/ToolInvalidParameters',
)('ToolInvalidParameters', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Failure raised while running a captured tool handler.
 *
 * @category errors
 */
export class ToolExecution extends Schema.TaggedError<ToolExecution>(
  '@effect-harness/harness/ToolError/ToolExecution',
)('ToolExecution', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Failure encoding or projecting a handler’s terminal result.
 *
 * @category errors
 */
export class ToolInvalidResult extends Schema.TaggedError<ToolInvalidResult>(
  '@effect-harness/harness/ToolError/ToolInvalidResult',
)('ToolInvalidResult', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
}) {}
/**
 * Failure reporting interruption before tool settlement.
 *
 * @category errors
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
 * Decoded value validated by the `ToolErrorReason` schema.
 *
 * @category models
 */
export type ToolErrorReason = typeof ToolErrorReason.Type
/**
 * Structured tool failure retaining its tool name and caught cause.
 *
 * **Details**
 *
 * Match reason._tag to distinguish availability, policy, argument, execution and result
 * failures. The message/name/cause accessors project the selected reason.
 *
 * **Gotchas**
 *
 * Effect interruption remains cancellation when settling a caught cause; it is not silently
 * converted into an ordinary failed tool result.
 *
 * @category errors
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
