/**
 * Durable task definitions, ported from pi's `defineTask` and `TaskDefinition`.
 *
 * A task is a state machine whose whole state is its durable checkpoint. Each checkpoint names a
 * `phase`, and the definition maps every phase to a handler. A handler runs one step of work and
 * commits the next checkpoint, a wait, or the task's outcome through its `Runtime`; the scheduler
 * runs the next phase in the same invocation, or resumes from the checkpoint after a restart.
 *
 * Handlers are Effects that need no services, so a definition is plain data the registry can hold.
 * A handler that fails, dies, or returns without durable progress faults the task. Cancellation is
 * interruption: when the task is abort-marked or the Scheduler closes, the running handler's fiber
 * is interrupted, together with every fiber it forked. The abort handler then runs in a fresh
 * invocation, also after a restart, and commits the task's outcome.
 */
import type { Effect, Schema } from 'effect'
import type { ConversationId, TaskId } from '../storage/schema/Ids.ts'
import type { JsonObject } from '../storage/schema/Json.ts'
import type {
  JoinPolicy,
  TaskOutcome,
  TaskOutcomeError,
  TaskRecord,
} from '../storage/schema/Task.ts'
import type { Service as SessionService, Tx } from '../session/Session.ts'
import type { CommitError, TxReadError } from '../session/SessionError.ts'
import type { StorageError } from '../storage/StorageError.ts'
import type { InvocationEnded, SchedulerClosed, TaskNotFound, TaskRejected } from './TaskError.ts'

/** A durable checkpoint: a JSON object naming the phase that runs next. */
export type Checkpoint = JsonObject & { readonly phase: string }

type LiveRecord = Extract<TaskRecord, { readonly memos?: unknown }>

/** The committed record of a task reserved by the invocation, typed by its definition. */
export type RunningTask<I, S> = Omit<LiveRecord, 'input' | 'state'> & {
  readonly input: I
  readonly state: { readonly status: 'running'; readonly checkpoint: S }
}

/** A terminal task record: the durable receipt of its outcome. */
export type TerminalTask = TaskRecord & {
  readonly state: { readonly status: 'terminal'; readonly outcome: TaskOutcome }
}

/**
 * An outcome a task commits for itself. `orphaned` and `faulted` are written only by the
 * scheduler, when no task code can run or the code broke its contract.
 */
export type Outcome<R> =
  | { readonly status: 'completed'; readonly result: R }
  | { readonly status: 'failed'; readonly error: TaskOutcomeError; readonly result?: R }
  | { readonly status: 'aborted'; readonly reason?: string; readonly result?: R }

/**
 * The state a task commits for itself: a replacement checkpoint, a wait, or its outcome. A wait
 * ends the invocation; the task resumes at `checkpoint` once every task in `on` is terminal.
 */
export type NextState<S, R> =
  | { readonly status: 'running'; readonly checkpoint: S }
  | {
      readonly status: 'waiting'
      readonly checkpoint: S
      readonly on: ReadonlyArray<TaskId>
      readonly policy: JoinPolicy
    }
  | { readonly status: 'terminal'; readonly outcome: Outcome<R> }

/** How a runtime operation fails besides its own callback's errors. */
export type RuntimeError =
  | InvocationEnded
  | TaskRejected
  | SchedulerClosed
  | TaskNotFound
  | CommitError
  | TxReadError
  | StorageError

/**
 * Operations of one task invocation. Every operation fails with `InvocationEnded` once the
 * invocation has ended. Document reads are the Session's own.
 */
export interface Runtime<I, S, R> extends Pick<
  SessionService,
  'snapshot' | 'snapshotAsOf' | 'watchDoc' | 'documentState'
> {
  readonly taskId: TaskId
  readonly conversationId: ConversationId
  /**
   * Commit after rereading the task on the line. Fails when the task is no longer running, the
   * invocation ended, the Scheduler is closing, or a run invocation's task is abort-marked. A
   * returned state replaces the task's state in the same commit; returning nothing leaves it. The
   * commit is bound to the task: entries record it as `byTaskId`, and `tx.createTask()` defaults
   * to its conversation. Inside the callback read only through `tx`.
   */
  readonly commit: <E>(
    change: (tx: Tx, current: RunningTask<I, S>) => Effect.Effect<NextState<S, R> | void, E>,
  ) => Effect.Effect<void, E | RuntimeError>
  /** Store `candidate` unless a memo named `name` exists, in one commit; the durable winner. */
  readonly memo: <T extends Schema.Json>(
    name: string,
    candidate: T,
  ) => Effect.Effect<T, RuntimeError>
  /** The committed memo named `name`. */
  readonly readMemo: (name: string) => Effect.Effect<Schema.Json | undefined, RuntimeError>
  /** The committed record of any task. */
  readonly getTask: (id: TaskId) => Effect.Effect<TaskRecord | undefined, RuntimeError>
  /** Wait for a task's terminal receipt; fails with `TaskNotFound` for a missing task. */
  readonly waitForTask: (id: TaskId) => Effect.Effect<TerminalTask, RuntimeError>
  /**
   * Abort a task this task owns, as `Scheduler.abort` does, and wait until it is terminal. Fails
   * with `TaskRejected` for a task another owns; succeeds at once for a terminal one.
   */
  readonly abortOwned: (id: TaskId) => Effect.Effect<void, RuntimeError>
  /**
   * Live tasks this task owns directly, in ID order, also those an earlier invocation created.
   * Tasks in conversations it owns are not included.
   */
  readonly ownedTasks: Effect.Effect<ReadonlyArray<TaskRecord>, RuntimeError>
  /** Outcomes of terminal tasks, in order, as after a wait; fails when one is not terminal. */
  readonly outcomes: (
    ids: ReadonlyArray<TaskId>,
  ) => Effect.Effect<ReadonlyArray<TaskOutcome>, RuntimeError>
}

/**
 * Runs one checkpoint phase. It must make durable progress, a changed checkpoint, a wait, or an
 * outcome, through `runtime.commit()`.
 */
export type Phase<I, P, S, R> = (
  task: RunningTask<I, P>,
  runtime: Runtime<I, S, R>,
) => Effect.Effect<void, unknown>

/** What a task definition says: its kind, version, first checkpoint, and handlers. */
export interface Definition<I extends Schema.Json, S extends Checkpoint, R extends Schema.Json> {
  /** Registered task kind, persisted as the record's `kind`. */
  readonly name: string
  /** Positive integer version of the stored input and checkpoint shapes. */
  readonly version: number
  /** The first checkpoint of a new task. */
  readonly initial: (input: I) => S
  /** Exhaustive phase map; each handler receives the task narrowed to its phase. */
  readonly phases: {
    readonly [P in S['phase']]: Phase<I, Extract<S, { readonly phase: P }>, S, R>
  }
  /** Runs in a fresh invocation after an abort mark, and must commit a terminal outcome. */
  readonly abort: (
    task: RunningTask<I, S>,
    runtime: Runtime<I, S, R>,
  ) => Effect.Effect<void, unknown>
  /** Convert a record stored by an older version; runs at reservation. */
  readonly migrate?: (
    input: Schema.Json,
    checkpoint: Schema.Json,
    fromVersion: number,
  ) => Effect.Effect<{ readonly input: I; readonly checkpoint: S }, unknown>
}

export const TypeId = '~effect-harness/task/Task'
export type TypeId = typeof TypeId

/** Any task definition made by `make`, with its types erased, as a `Registry` holds it. */
export interface Any {
  readonly [TypeId]: TypeId
  readonly name: string
  readonly version: number
}

/** An executable durable state machine, registered by `name`. */
export interface Task<I extends Schema.Json, S extends Checkpoint, R extends Schema.Json>
  extends Definition<I, S, R>, Any {}

/** Type a task definition. Pass it to `Tx.createTask()` and install it in a `Registry`. */
export const make = <I extends Schema.Json, S extends Checkpoint, R extends Schema.Json>(
  definition: Definition<I, S, R>,
): Task<I, S, R> => ({ ...definition, [TypeId]: TypeId })
