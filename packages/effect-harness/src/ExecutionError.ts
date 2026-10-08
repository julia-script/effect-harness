/**
 * Semantic process execution failures with retained native causes and spill metadata.
 */
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

const Payload = Schema.Struct({
  message: Schema.String,
  cause: SchemaField.optional(Schema.Defect()),
  spillPath: SchemaField.optional(Schema.String),
})

/**
 * Schema for execution error code.
 *
 * @category schemas
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
 * Decoded value validated by the `ExecutionErrorCode` schema.
 *
 * @category models
 */
export type ExecutionErrorCode = typeof ExecutionErrorCode.Type
/**
 * Process failure reporting scoped cancellation.
 *
 * @category errors
 */
export class ExecutionAbortedError extends Schema.TaggedError<ExecutionAbortedError>(
  '@effect-harness/harness/ExecutionError/ExecutionAbortedError',
)('ExecutionAbortedError', Payload.fields) {
  get code(): 'aborted' {
    return 'aborted'
  }
}
/**
 * Process failure reporting the configured timeout exceeded.
 *
 * @category errors
 */
export class ExecutionTimeoutError extends Schema.TaggedError<ExecutionTimeoutError>(
  '@effect-harness/harness/ExecutionError/ExecutionTimeoutError',
)('ExecutionTimeoutError', Payload.fields) {
  get code(): 'timeout' {
    return 'timeout'
  }
}
/**
 * Process failure reporting that no usable configured shell was found.
 *
 * @category errors
 */
export class ExecutionShellUnavailableError extends Schema.TaggedError<ExecutionShellUnavailableError>(
  '@effect-harness/harness/ExecutionError/ExecutionShellUnavailableError',
)('ExecutionShellUnavailableError', Payload.fields) {
  get code(): 'shell_unavailable' {
    return 'shell_unavailable'
  }
}
/**
 * Process failure reporting an unsuccessful command launch.
 *
 * @category errors
 */
export class ExecutionSpawnError extends Schema.TaggedError<ExecutionSpawnError>(
  '@effect-harness/harness/ExecutionError/ExecutionSpawnError',
)('ExecutionSpawnError', Payload.fields) {
  get code(): 'spawn_error' {
    return 'spawn_error'
  }
}
/**
 * Process failure reporting an output or progress callback failure.
 *
 * @category errors
 */
export class ExecutionCallbackError extends Schema.TaggedError<ExecutionCallbackError>(
  '@effect-harness/harness/ExecutionError/ExecutionCallbackError',
)('ExecutionCallbackError', Payload.fields) {
  get code(): 'callback_error' {
    return 'callback_error'
  }
}
/**
 * Process failure carrying an otherwise unclassified native cause.
 *
 * @category errors
 */
export class ExecutionUnknownError extends Schema.TaggedError<ExecutionUnknownError>(
  '@effect-harness/harness/ExecutionError/ExecutionUnknownError',
)('ExecutionUnknownError', Payload.fields) {
  get code(): 'unknown' {
    return 'unknown'
  }
}
/**
 * Schema for execution error reason.
 *
 * @category schemas
 */
export const ExecutionErrorReason = Schema.Union([
  ExecutionAbortedError,
  ExecutionTimeoutError,
  ExecutionShellUnavailableError,
  ExecutionSpawnError,
  ExecutionCallbackError,
  ExecutionUnknownError,
])
/**
 * Decoded value validated by the `ExecutionErrorReason` schema.
 *
 * @category models
 */
export type ExecutionErrorReason = typeof ExecutionErrorReason.Type
/**
 * Structured shell/process failure with optional spill-file diagnostics.
 *
 * **Details**
 *
 * Spawn, timeout and callback failures use this error channel. A completed command’s nonzero
 * exit code remains a shell result.
 *
 * **Gotchas**
 *
 * An existing spillPath may be useful for diagnostics; its filesystem and cleanup lifetime
 * remain owned by the invocation.
 *
 * @category errors
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
