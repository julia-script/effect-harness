/**
 * Semantic tool failure reasons with preserved caught causes.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
const Payload = Schema.Struct({
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  name: Schema.String,
})

/**
 * Failure reporting a requested tool or local handler that is unavailable.
 *
 * @category errors
 */
export class ToolUnavailableError extends Schema.TaggedError<ToolUnavailableError>(
  '@effect-harness/harness/ToolError/ToolUnavailableError',
)('ToolUnavailableError', Payload.fields) {}
/**
 * Failure reporting a tool invocation blocked by hook policy.
 *
 * @category errors
 */
export class ToolBlockedError extends Schema.TaggedError<ToolBlockedError>(
  '@effect-harness/harness/ToolError/ToolBlockedError',
)('ToolBlockedError', Payload.fields) {}
/**
 * Failure decoding, repairing or encoding tool arguments.
 *
 * @category errors
 */
export class ToolInvalidParametersError extends Schema.TaggedError<ToolInvalidParametersError>(
  '@effect-harness/harness/ToolError/ToolInvalidParametersError',
)('ToolInvalidParametersError', Payload.fields) {}
/**
 * Failure raised while running a captured tool handler.
 *
 * @category errors
 */
export class ToolExecutionError extends Schema.TaggedError<ToolExecutionError>(
  '@effect-harness/harness/ToolError/ToolExecutionError',
)('ToolExecutionError', Payload.fields) {}
/**
 * Failure encoding or projecting a handler’s terminal result.
 *
 * @category errors
 */
export class ToolInvalidResultError extends Schema.TaggedError<ToolInvalidResultError>(
  '@effect-harness/harness/ToolError/ToolInvalidResultError',
)('ToolInvalidResultError', Payload.fields) {}
/**
 * Failure reporting interruption before tool settlement.
 *
 * @category errors
 */
export class ToolInterruptedError extends Schema.TaggedError<ToolInterruptedError>(
  '@effect-harness/harness/ToolError/ToolInterruptedError',
)('ToolInterruptedError', Payload.fields) {}
/**
 * Schema for tool error reason.
 *
 * @category schemas
 */
export const ToolErrorReason = Schema.Union([
  ToolUnavailableError,
  ToolBlockedError,
  ToolInvalidParametersError,
  ToolExecutionError,
  ToolInvalidResultError,
  ToolInterruptedError,
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
