/**
 * Structured Workflow execution failures.
 */
import * as Schema from 'effect/Schema'

// Defect JSON is a diagnostic projection: messages and cause chains survive,
// while native Error subclasses/custom properties/stacks need not.
const fields = {
  message: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
  cause: Schema.optionalKey(Schema.Defect()),
}

/**
 * Workflow failure reporting that no usable model was selected.
 *
 * @category errors
 */
export class NoModel extends Schema.TaggedError<NoModel>(
  '@effect-harness/durable/workflow/ExecutionError/NoModel',
)('NoModel', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting input rejected while its conversation is busy.
 *
 * @category errors
 */
export class ConversationBusy extends Schema.TaggedError<ConversationBusy>(
  '@effect-harness/durable/workflow/ExecutionError/ConversationBusy',
)('ConversationBusy', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting incompatible reuse of an admission identity.
 *
 * @category errors
 */
export class RequestConflict extends Schema.TaggedError<RequestConflict>(
  '@effect-harness/durable/workflow/ExecutionError/RequestConflict',
)('RequestConflict', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting an unavailable requested tool.
 *
 * @category errors
 */
export class ToolUnavailable extends Schema.TaggedError<ToolUnavailable>(
  '@effect-harness/durable/workflow/ExecutionError/ToolUnavailable',
)('ToolUnavailable', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting invalid execution or tool arguments.
 *
 * @category errors
 */
export class InvalidArguments extends Schema.TaggedError<InvalidArguments>(
  '@effect-harness/durable/workflow/ExecutionError/InvalidArguments',
)('InvalidArguments', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure wrapping a model request failure.
 *
 * @category errors
 */
export class ModelError extends Schema.TaggedError<ModelError>(
  '@effect-harness/durable/workflow/ExecutionError/ModelError',
)('ModelError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting context beyond the selected model’s limit.
 *
 * @category errors
 */
export class ContextOverflow extends Schema.TaggedError<ContextOverflow>(
  '@effect-harness/durable/workflow/ExecutionError/ContextOverflow',
)('ContextOverflow', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting committed task cancellation.
 *
 * @category errors
 */
export class Aborted extends Schema.TaggedError<Aborted>(
  '@effect-harness/durable/workflow/ExecutionError/Aborted',
)('Aborted', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting a sealed Session or execution boundary.
 *
 * @category errors
 */
export class Closed extends Schema.TaggedError<Closed>(
  '@effect-harness/durable/workflow/ExecutionError/Closed',
)('Closed', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure wrapping rejected or uncertain domain persistence.
 *
 * @category errors
 */
export class Storage extends Schema.TaggedError<Storage>(
  '@effect-harness/durable/workflow/ExecutionError/Storage',
)('Storage', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting an inconsistent task or execution projection.
 *
 * @category errors
 */
export class InvalidState extends Schema.TaggedError<InvalidState>(
  '@effect-harness/durable/workflow/ExecutionError/InvalidState',
)('InvalidState', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Schema for structured harness Workflow failure reasons.
 *
 * @category schemas
 */
export const ExecutionErrorReason = Schema.Union([
  NoModel,
  ConversationBusy,
  RequestConflict,
  ToolUnavailable,
  InvalidArguments,
  ModelError,
  ContextOverflow,
  Aborted,
  Closed,
  Storage,
  InvalidState,
])
/**
 * Decoded value validated by the `ExecutionErrorReason` schema.
 *
 * @category models
 */
export type ExecutionErrorReason = typeof ExecutionErrorReason.Type

/**
 * Structured Workflow failure with preserved reason and optional domain detail.
 *
 * **Details**
 *
 * Runtime policies match reason._tag. The same schema encodes persisted failures with their
 * structured reason. Model/provider retry is selected by model policy.
 *
 * **Gotchas**
 *
 * A Workflow failure does not authorize repeating an external side effect. Reconcile its
 * domain receipt and replay policy first.
 *
 * @category errors
 */
export class ExecutionError extends Schema.TaggedError<ExecutionError>(
  '@effect-harness/durable/workflow/ExecutionError',
)('ExecutionError', { reason: ExecutionErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get detail(): Schema.Json | undefined {
    return this.reason.detail
  }
  /** A workflow failure is not itself permission to repeat a side effect. */
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
}
