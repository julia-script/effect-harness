/**
 * The task `Scheduler`, ported from pi's `harness/scheduler.ts`.
 *
 * It reserves runnable tasks on the Session line, runs each one's phase handlers in an invocation
 * fiber off the line, and decides on the line, before every phase, whether the invocation goes on:
 *
 * 1. a task that is no longer running (terminal or waiting) stops;
 * 2. a closing Scheduler stops, keeping the checkpoint for the next one;
 * 3. an abort-marked task stops, and its abort handler runs in a fresh invocation;
 * 4. a failed or defective handler faults the task;
 * 5. a changed checkpoint runs the next phase, or hands over to a replacement definition;
 * 6. an unchanged checkpoint faults the task: the phase made no durable progress.
 *
 * Opening moves every task a crash left `running` back to `pending`. A task whose definition is
 * missing, older than the record, or fails to migrate stays live and blocked until the registry
 * changes; aborting it settles it as `orphaned`. Closing the scope interrupts every invocation and
 * writes no outcome, so the work resumes in the next Scheduler.
 *
 * Tasks and conversations form one ownership tree (spec §5.5). A task's outcome holds as
 * `completing` while its ordinary owned work is live, and its abort handler waits for that work:
 * aborts run bottom-up. An owner's cancellation intent, its abort mark or a held failed outcome,
 * abort-marks the live work below it (spec §5.4), and a failed task in a `failFast` wait marks the
 * others. Background tasks are owned by a conversation but outside its ordinary traversal: no
 * cascade, idle wait, or conversation abort reaches them or below them. A task created with
 * `abandonOnRestart` is abort-marked with reason `restart` by the next Scheduler.
 *
 * Conversation aborts and cascades also withdraw queued inputs, through the `withdrawInputs`
 * hook, of every conversation they reach: the inbox belongs to the Harness.
 */
import { Context, Layer } from 'effect'
import type { Effect, Scope } from 'effect'
import type { Session, Tx } from '../session/Session.ts'
import type { CommitError, TxDocumentError } from '../session/SessionError.ts'
import type { Storage } from '../storage/Storage.ts'
import type { StorageError } from '../storage/StorageError.ts'
import type { ConversationId } from '../storage/schema/Ids.ts'
import type { TaskId } from '../storage/schema/Ids.ts'
import type { TaskRecord } from '../storage/schema/Task.ts'
import type { TaskOutcome } from '../storage/schema/Task.ts'
import * as Inspections from './Inspection.ts'
import * as internal from './internal/scheduler.ts'
import type { Registry } from './Registry.ts'
import type { TerminalTask } from './Task.ts'
import type { SchedulerClosed, TaskNotFound } from './TaskError.ts'

export const TaskInspectionState = Inspections.TaskInspectionState
export type TaskInspectionState = Inspections.TaskInspectionState
export const TaskInspection = Inspections.TaskInspection
export type TaskInspection = Inspections.TaskInspection
export const Inspection = Inspections.Inspection
export type Inspection = Inspections.Inspection

export interface Service {
  /**
   * Commit the task's abort mark, interrupt its run invocation and wait for it to end; the abort
   * handler then runs in a fresh invocation. A task no definition can take is settled as
   * `orphaned` instead. `terminal` when the task had already ended.
   */
  readonly abort: (
    id: TaskId,
  ) => Effect.Effect<'marked' | 'terminal', TaskNotFound | CommitError | StorageError>
  /** Wait for the task's terminal receipt. Interrupting the wait leaves the task running. */
  readonly waitForTask: (
    id: TaskId,
  ) => Effect.Effect<TerminalTask, TaskNotFound | SchedulerClosed | CommitError>
  /**
   * Wait until ordinary traversal from the conversation, or with none from every ownerless
   * conversation, reaches no live non-background task.
   */
  readonly waitForIdle: (
    conversationId?: ConversationId,
  ) => Effect.Effect<void, SchedulerClosed | CommitError>
  /**
   * Abort-mark, in one commit, every live non-background task ordinary traversal from the
   * conversation reaches, then wait until it is idle. With `background`, traversal crosses
   * background tasks, and the wait also covers every task it reached.
   */
  readonly abortConversation: (
    conversationId: ConversationId,
    options?: { readonly background?: boolean },
  ) => Effect.Effect<void, TaskNotFound | SchedulerClosed | CommitError | StorageError>
  /** Scheduling state and every live task with its derived state. Runs no task code. */
  readonly inspect: Effect.Effect<Inspection, CommitError | StorageError>
}

/** Terminal outcomes the scheduler writes without running task code. */
export type SchedulerOutcome = Extract<TaskOutcome, { readonly status: 'faulted' | 'orphaned' }>

/** Harness hooks a Scheduler stages in its own commits. */
export interface Options {
  /**
   * Cleanup for an outcome only the scheduler writes, `faulted` or `orphaned`, staged in the
   * commit that makes it terminal. A failure fails that commit.
   */
  readonly settleOutcome?: (
    tx: Tx,
    record: TaskRecord,
    outcome: SchedulerOutcome,
  ) => Effect.Effect<void, TxDocumentError>
  /**
   * Withdraw a conversation's queued inputs, in the commit of a conversation abort or an abort
   * cascade that reaches it.
   */
  readonly withdrawInputs?: (
    tx: Tx,
    conversationId: ConversationId,
  ) => Effect.Effect<void, TxDocumentError>
}

export class Scheduler extends Context.Service<Scheduler, Service>()(
  'effect-harness/task/Scheduler',
) {}

/**
 * Open a Scheduler over the Session and Registry in context and start scheduling. It reads
 * committed records from the Session's `Storage` on the line. Closing the scope stops it.
 */
export const makeWith: (
  options: Options,
) => Effect.Effect<Service, CommitError, Session | Storage | Registry | Scope.Scope> = internal.make

/** `makeWith` without hooks. */
export const make: Effect.Effect<Service, CommitError, Session | Storage | Registry | Scope.Scope> =
  makeWith({})

/** A `Scheduler` over the provided Session, its Storage, and a Registry. */
export const layerWith = (
  options: Options,
): Layer.Layer<Scheduler, CommitError, Session | Storage | Registry> =>
  Layer.effect(Scheduler, makeWith(options))

/** `layerWith` without hooks. */
export const layer: Layer.Layer<Scheduler, CommitError, Session | Storage | Registry> = layerWith(
  {},
)
