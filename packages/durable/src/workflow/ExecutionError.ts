import * as Schema from 'effect/Schema'

/** A recoverable harness execution failure, encoded by the native Workflow engine. */
export class ExecutionError extends Schema.TaggedError<ExecutionError>()('ExecutionError', {
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
}) {}
