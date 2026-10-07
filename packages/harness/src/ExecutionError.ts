/**
 * Semantic process execution failures with retained native causes and spill metadata.
 *
 * @since 0.0.0
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

/**
 * Schema for execution error code.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ExecutionErrorCode = Schema.Literals([
  'aborted',
  'timeout',
  'shell_unavailable',
  'spawn_error',
  'callback_error',
  'unknown',
])
/**
 * Error execution error code contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ExecutionErrorCode = typeof ExecutionErrorCode.Type
/**
 * Semantic execution aborted with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionAborted extends Schema.TaggedError<ExecutionAborted>(
  '@effect-harness/harness/ExecutionError/ExecutionAborted',
)('ExecutionAborted', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
}) {
  get code(): 'aborted' {
    return 'aborted'
  }
}
/**
 * Semantic execution timeout with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionTimeout extends Schema.TaggedError<ExecutionTimeout>(
  '@effect-harness/harness/ExecutionError/ExecutionTimeout',
)('ExecutionTimeout', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
}) {
  get code(): 'timeout' {
    return 'timeout'
  }
}
/**
 * Semantic execution shell unavailable with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionShellUnavailable extends Schema.TaggedError<ExecutionShellUnavailable>(
  '@effect-harness/harness/ExecutionError/ExecutionShellUnavailable',
)('ExecutionShellUnavailable', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
}) {
  get code(): 'shell_unavailable' {
    return 'shell_unavailable'
  }
}
/**
 * Semantic execution spawn error with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionSpawnError extends Schema.TaggedError<ExecutionSpawnError>(
  '@effect-harness/harness/ExecutionError/ExecutionSpawnError',
)('ExecutionSpawnError', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
}) {
  get code(): 'spawn_error' {
    return 'spawn_error'
  }
}
/**
 * Semantic execution callback error with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionCallbackError extends Schema.TaggedError<ExecutionCallbackError>(
  '@effect-harness/harness/ExecutionError/ExecutionCallbackError',
)('ExecutionCallbackError', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
}) {
  get code(): 'callback_error' {
    return 'callback_error'
  }
}
/**
 * Semantic execution unknown with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionUnknown extends Schema.TaggedError<ExecutionUnknown>(
  '@effect-harness/harness/ExecutionError/ExecutionUnknown',
)('ExecutionUnknown', {
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
}) {
  get code(): 'unknown' {
    return 'unknown'
  }
}
/**
 * Schema for execution error reason.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ExecutionErrorReason = Schema.Union([
  ExecutionAborted,
  ExecutionTimeout,
  ExecutionShellUnavailable,
  ExecutionSpawnError,
  ExecutionCallbackError,
  ExecutionUnknown,
])
/**
 * Error execution error reason contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ExecutionErrorReason = typeof ExecutionErrorReason.Type
/**
 * Semantic execution error with its retained cause.
 *
 * @category errors
 * @since 0.0.0
 */
export class ExecutionError extends Schema.TaggedError<ExecutionError>(
  '@effect-harness/harness/ExecutionError/ExecutionError',
)('ExecutionError', { reason: ExecutionErrorReason }) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get code(): ExecutionErrorCode {
    return this.reason.code
  }
  get spillPath(): string | undefined {
    return this.reason.spillPath
  }
}
