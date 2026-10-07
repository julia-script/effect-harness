/**
 * Structured Workflow execution failures and legacy-compatible codecs.
 *
 * @since 0.0.0
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
 * NoModel schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class NoModel extends Schema.TaggedError<NoModel>(
  '@effect-harness/durable/workflow/ExecutionError/NoModel',
)('NoModel', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * ConversationBusy schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class ConversationBusy extends Schema.TaggedError<ConversationBusy>(
  '@effect-harness/durable/workflow/ExecutionError/ConversationBusy',
)('ConversationBusy', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * RequestConflict schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class RequestConflict extends Schema.TaggedError<RequestConflict>(
  '@effect-harness/durable/workflow/ExecutionError/RequestConflict',
)('RequestConflict', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * ToolUnavailable schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class ToolUnavailable extends Schema.TaggedError<ToolUnavailable>(
  '@effect-harness/durable/workflow/ExecutionError/ToolUnavailable',
)('ToolUnavailable', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * InvalidArguments schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidArguments extends Schema.TaggedError<InvalidArguments>(
  '@effect-harness/durable/workflow/ExecutionError/InvalidArguments',
)('InvalidArguments', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * ModelError schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class ModelError extends Schema.TaggedError<ModelError>(
  '@effect-harness/durable/workflow/ExecutionError/ModelError',
)('ModelError', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * ContextOverflow schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class ContextOverflow extends Schema.TaggedError<ContextOverflow>(
  '@effect-harness/durable/workflow/ExecutionError/ContextOverflow',
)('ContextOverflow', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Aborted schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class Aborted extends Schema.TaggedError<Aborted>(
  '@effect-harness/durable/workflow/ExecutionError/Aborted',
)('Aborted', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Closed schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class Closed extends Schema.TaggedError<Closed>(
  '@effect-harness/durable/workflow/ExecutionError/Closed',
)('Closed', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * Storage schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class Storage extends Schema.TaggedError<Storage>(
  '@effect-harness/durable/workflow/ExecutionError/Storage',
)('Storage', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * InvalidState schema.
 *
 * @category errors
 * @since 0.0.0
 */
export class InvalidState extends Schema.TaggedError<InvalidState>(
  '@effect-harness/durable/workflow/ExecutionError/InvalidState',
)('InvalidState', fields) {
  get isRetryable(): boolean {
    return false
  }
}

/**
 * ExecutionErrorReason schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * ExecutionErrorReason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ExecutionErrorReason = typeof ExecutionErrorReason.Type

/**
 * A recoverable execution failure. Model/provider retry remains owned by the model policy.
 *
 * @category errors
 * @since 0.0.0
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
 * LegacyReason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type LegacyReason = keyof typeof legacyReasons

/**
 * LegacyExecutionError schema.
 *
 * @category schemas
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const ExecutionErrorCodec = Schema.Union([legacyCodec, ExecutionError])
