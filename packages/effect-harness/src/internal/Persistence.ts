/** Opaque append log and named payload streams. No harness records cross this boundary. */
import type * as Effect from 'effect/Effect'
import type * as Errors from '../StorageError.js'

export interface Batch {
  readonly frame: string
  readonly append: ReadonlyArray<{ readonly key: string; readonly lines: ReadonlyArray<string> }>
}

export interface Replacement {
  readonly key: string
  readonly lines: ReadonlyArray<string>
}

/**
 * append acknowledges the whole batch only after its payloads and marker are stored
 * atomically. Persistent adapters must make them durable before acknowledging;
 * volatile adapters retain them only for the lifetime of their backing state.
 * An unsuccessful append may have committed: callers must close and recover.
 * replace is maintenance of already superseded payloads, never a domain commit.
 */
export interface Persistence {
  /** Reject a host context that cannot independently acknowledge durable writes. */
  readonly checkWrite: Effect.Effect<void, Errors.StorageError>
  /** Called after replay validation to accept the recoverable physical prefix. */
  readonly accept: Effect.Effect<void, Errors.StorageError>
  readonly journal: Effect.Effect<ReadonlyArray<string>, Errors.StorageError>
  readonly keys: Effect.Effect<ReadonlyArray<string>, Errors.StorageError>
  readonly read: (key: string) => Effect.Effect<ReadonlyArray<string>, Errors.StorageError>
  readonly append: (batch: Batch) => Effect.Effect<void, Errors.StorageError>
  readonly replace: (replacement: Replacement) => Effect.Effect<void, Errors.StorageError>
}
