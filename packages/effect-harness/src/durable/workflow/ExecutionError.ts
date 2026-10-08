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
export class NoModelError extends Schema.TaggedError<NoModelError>(
  '@effect-harness/durable/workflow/ExecutionError/NoModelError',
)('NoModelError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting input rejected while its conversation is busy.
 *
 * @category errors
 */
export class ConversationBusyError extends Schema.TaggedError<ConversationBusyError>(
  '@effect-harness/durable/workflow/ExecutionError/ConversationBusyError',
)('ConversationBusyError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting incompatible reuse of an admission identity.
 *
 * @category errors
 */
export class RequestConflictError extends Schema.TaggedError<RequestConflictError>(
  '@effect-harness/durable/workflow/ExecutionError/RequestConflictError',
)('RequestConflictError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting an unavailable requested tool.
 *
 * @category errors
 */
export class ToolUnavailableError extends Schema.TaggedError<ToolUnavailableError>(
  '@effect-harness/durable/workflow/ExecutionError/ToolUnavailableError',
)('ToolUnavailableError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting invalid execution or tool arguments.
 *
 * @category errors
 */
export class InvalidArgumentsError extends Schema.TaggedError<InvalidArgumentsError>(
  '@effect-harness/durable/workflow/ExecutionError/InvalidArgumentsError',
)('InvalidArgumentsError', fields) {
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
export class ContextOverflowError extends Schema.TaggedError<ContextOverflowError>(
  '@effect-harness/durable/workflow/ExecutionError/ContextOverflowError',
)('ContextOverflowError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting committed task cancellation.
 *
 * @category errors
 */
export class AbortedError extends Schema.TaggedError<AbortedError>(
  '@effect-harness/durable/workflow/ExecutionError/AbortedError',
)('AbortedError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting a sealed Session or execution boundary.
 *
 * @category errors
 */
export class ClosedError extends Schema.TaggedError<ClosedError>(
  '@effect-harness/durable/workflow/ExecutionError/Closed',
)('ClosedError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure wrapping rejected or uncertain domain persistence.
 *
 * @category errors
 */
export class StorageError extends Schema.TaggedError<StorageError>(
  '@effect-harness/durable/workflow/ExecutionError/StorageError',
)('StorageError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Workflow failure reporting an inconsistent task or execution projection.
 *
 * @category errors
 */
export class InvalidStateError extends Schema.TaggedError<InvalidStateError>(
  '@effect-harness/durable/workflow/ExecutionError/InvalidStateError',
)('InvalidStateError', fields) {
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
  NoModelError,
  ConversationBusyError,
  RequestConflictError,
  ToolUnavailableError,
  InvalidArgumentsError,
  ModelError,
  ContextOverflowError,
  AbortedError,
  ClosedError,
  StorageError,
  InvalidStateError,
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
  /**
   * Automatic retry eligibility of the structured reason; a workflow failure alone does not authorize repeating a side effect.
   */
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
}

/** Checks the decoded NoModel contract without decoding or coercing input.
 * @category guards
 */
export const isNoModel: (u: unknown) => u is NoModelError = Schema.is(Schema.toType(NoModelError))

/** Checks the decoded ConversationBusy contract without decoding or coercing input.
 * @category guards
 */
export const isConversationBusy: (u: unknown) => u is ConversationBusyError = Schema.is(
  Schema.toType(ConversationBusyError),
)

/** Checks the decoded RequestConflict contract without decoding or coercing input.
 * @category guards
 */
export const isRequestConflict: (u: unknown) => u is RequestConflictError = Schema.is(
  Schema.toType(RequestConflictError),
)

/** Checks the decoded ToolUnavailable contract without decoding or coercing input.
 * @category guards
 */
export const isToolUnavailable: (u: unknown) => u is ToolUnavailableError = Schema.is(
  Schema.toType(ToolUnavailableError),
)

/** Checks the decoded InvalidArguments contract without decoding or coercing input.
 * @category guards
 */
export const isInvalidArguments: (u: unknown) => u is InvalidArgumentsError = Schema.is(
  Schema.toType(InvalidArgumentsError),
)

/** Checks the decoded ModelError contract without decoding or coercing input.
 * @category guards
 */
export const isModelError: (u: unknown) => u is ModelError = Schema.is(Schema.toType(ModelError))

/** Checks the decoded ContextOverflow contract without decoding or coercing input.
 * @category guards
 */
export const isContextOverflow: (u: unknown) => u is ContextOverflowError = Schema.is(
  Schema.toType(ContextOverflowError),
)

/** Checks the decoded Aborted contract without decoding or coercing input.
 * @category guards
 */
export const isAborted: (u: unknown) => u is AbortedError = Schema.is(Schema.toType(AbortedError))

/** Checks the decoded Closed contract without decoding or coercing input.
 * @category guards
 */
export const isClosed: (u: unknown) => u is ClosedError = Schema.is(Schema.toType(ClosedError))

/** Checks the decoded Storage contract without decoding or coercing input.
 * @category guards
 */
export const isStorage: (u: unknown) => u is StorageError = Schema.is(Schema.toType(StorageError))

/** Checks the decoded InvalidState contract without decoding or coercing input.
 * @category guards
 */
export const isInvalidState: (u: unknown) => u is InvalidStateError = Schema.is(
  Schema.toType(InvalidStateError),
)

/** Checks the decoded ExecutionError contract without decoding or coercing input.
 * @category guards
 */
export const isExecutionError: (u: unknown) => u is ExecutionError = Schema.is(
  Schema.toType(ExecutionError),
)

/** Checks the decoded ExecutionErrorReason contract without decoding or coercing input.
 * @category guards
 */
export const isExecutionErrorReason: (u: unknown) => u is ExecutionErrorReason = Schema.is(
  Schema.toType(ExecutionErrorReason),
)
