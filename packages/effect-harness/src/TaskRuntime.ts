/** Invocation-local access to committed task state. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Schema from 'effect/Schema'
import type * as Record from './Record.ts'
import type { StorageError } from './StorageError.ts'
import type * as Session from './internal/Session.ts'
import type * as Task from './Task.ts'

/** All methods reject after the invocation ends, is aborted, or its harness closes. */
export class TaskRuntime extends Context.Service<
  TaskRuntime,
  {
    readonly taskId: Record.TaskId
    readonly conversationId: Record.ConversationId
    readonly transaction: <A, E, R>(
      change: (tx: Session.Transaction, current: Record.Task) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | StorageError, R>
    readonly commit: <E, R>(
      change: (
        tx: Session.Transaction,
        current: Record.Task,
      ) => Effect.Effect<Task.Transition<Schema.Json, Schema.Json> | void, E, R>,
    ) => Effect.Effect<void, E | StorageError, R>
    readonly checkpoint: (checkpoint: Schema.Json) => Effect.Effect<void, StorageError>
    readonly memo: (
      name: string,
      candidate?: Schema.Json,
    ) => Effect.Effect<Schema.Json | undefined, StorageError>
    readonly task: (id: Record.TaskId) => Effect.Effect<Option.Option<Record.Task>, StorageError>
    readonly outcomes: (
      ids: ReadonlyArray<Record.TaskId>,
    ) => Effect.Effect<ReadonlyArray<Schema.Json>, StorageError>
    /** The caller first saves the absolute target in its checkpoint. */
    readonly sleepUntil: (until: number) => Effect.Effect<void, StorageError>
    readonly now: Effect.Effect<number, StorageError>
  }
>()('effect-harness/TaskRuntime') {}
