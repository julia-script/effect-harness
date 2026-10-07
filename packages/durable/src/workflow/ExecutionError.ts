/**
 * Structured Workflow execution failures and legacy-compatible codecs.
 */
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'

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
 * Runtime policies match reason._tag. The code accessor retains the legacy discriminator for
 * persisted records. Model/provider retry is selected by model policy.
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
  /** Original wire discriminator, for persisted domain records only. Match reason._tag in runtime policies. */
  get code(): LegacyReason {
    return reasonCodes[this.reason._tag]
  }
}

const legacyReasons = {
  no_model: NoModel,
  conversation_busy: ConversationBusy,
  request_conflict: RequestConflict,
  tool_unavailable: ToolUnavailable,
  invalid_arguments: InvalidArguments,
  model_error: ModelError,
  context_overflow: ContextOverflow,
  aborted: Aborted,
  closed: Closed,
  storage: Storage,
  invalid_state: InvalidState,
} as const
const reasonCodes = {
  NoModel: 'no_model',
  ConversationBusy: 'conversation_busy',
  RequestConflict: 'request_conflict',
  ToolUnavailable: 'tool_unavailable',
  InvalidArguments: 'invalid_arguments',
  ModelError: 'model_error',
  ContextOverflow: 'context_overflow',
  Aborted: 'aborted',
  Closed: 'closed',
  Storage: 'storage',
  InvalidState: 'invalid_state',
} as const
/**
 * Compatibility code translated into a structured workflow failure reason.
 *
 * @category models
 */
export type LegacyReason = keyof typeof legacyReasons

/**
 * Schema for the compatible persisted Workflow error representation.
 *
 * @category schemas
 */
export const LegacyExecutionError = Schema.TaggedStruct('ExecutionError', {
  reason: Schema.Literals([
    'no_model',
    'conversation_busy',
    'request_conflict',
    'tool_unavailable',
    'invalid_arguments',
    'model_error',
    'context_overflow',
    'aborted',
    'closed',
    'storage',
    'invalid_state',
  ]),
  message: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
  cause: Schema.optionalKey(Schema.Defect()),
})
/** Decodes the frozen Workflow representation into structured runtime reasons. */
const fromLegacy = (input: typeof LegacyExecutionError.Type): ExecutionError => {
  const Reason = legacyReasons[input.reason]
  return new ExecutionError({
    reason: new Reason({
      message: input.message,
      ...(Object.hasOwn(input, 'detail') ? { detail: input.detail } : {}),
      ...(Object.hasOwn(input, 'cause') ? { cause: input.cause } : {}),
    }),
  })
}
const legacyCodec = LegacyExecutionError.pipe(
  Schema.decodeTo(
    Schema.toType(ExecutionError),
    SchemaTransformation.transform({
      decode: fromLegacy,
      encode: (error) => ({
        _tag: 'ExecutionError' as const,
        reason: error.code,
        message: error.message,
        ...(Object.hasOwn(error.reason, 'detail') ? { detail: error.reason.detail } : {}),
        ...(Object.hasOwn(error.reason, 'cause') ? { cause: error.reason.cause } : {}),
      }),
    }),
  ),
)
/**
 * Native Workflow codec.
 *
 * **Details**
 *
 * Accepts legacy/current errors; encoding keeps the original reason/message/detail wire shape.
 *
 * @category schemas
 */
export const ExecutionErrorCodec = Schema.Union([legacyCodec, ExecutionError])
