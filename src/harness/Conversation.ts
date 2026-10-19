/**
 * Conversation handles, ported from pi's `Conversation`. A handle is stateless and bound to the
 * Harness that returned it; compare handles by `id`.
 */
import type { Effect, Scope } from 'effect'
import type { ConversationOwnership, TransactionScope, Tx } from '../session/Session.ts'
import type { CommitError, SessionFailed, SnapshotError } from '../session/SessionError.ts'
import type { EntryRecord } from '../storage/schema/Entry.ts'
import type { ConversationId, EntryId, TaskId } from '../storage/schema/Ids.ts'
import type { Cursor, EntryQuery, Page } from '../storage/schema/Scan.ts'
import type { StorageError } from '../storage/StorageError.ts'
import type { SchedulerClosed, TaskNotFound } from '../task/TaskError.ts'
import type * as AgentEvent from './AgentEvent.ts'
import type * as AgentState from './AgentState.ts'
import type { ConversationBusy, InvalidSubmission } from './HarnessError.ts'
import type { Draft, Submission } from './Submission.ts'
import type { ContextView, ConversationView, Watch } from './ViewState.ts'

/**
 * Runs inside the creating commit, after the built-in documents. The conversation record is
 * already a table write, so table reads fail with `ReadAfterWrite`; documents stay available.
 */
export type Init<E> = (tx: Tx, conversationId: ConversationId) => Effect.Effect<void, E>

/** How to create a conversation. */
export interface CreateOptions<E = never> {
  readonly ownership: ConversationOwnership
  readonly init?: Init<E>
}

/** Options of `Conversation.abort`. */
export interface AbortOptions {
  /**
   * Cross background boundaries: mark every live task reached whether it is background or not,
   * withdraw the queued inputs of every conversation reached, and wait until those tasks are
   * terminal and the conversation is idle.
   */
  readonly background?: boolean
}

/** A conversation of one Harness. */
export interface Conversation {
  readonly id: ConversationId
  /**
   * Durably admit user input or a passive entry write, in one commit. A known `requestId` returns
   * its existing submission without writing.
   *
   * An idle conversation places input as its entry and starts a run, and appends a write at once.
   * A busy one, or one with queued items, queues them in `$inbox` for the next boundary; input
   * with `whenBusy: 'reject'` fails with `ConversationBusy` instead and writes nothing. A write of
   * only `kind` and `data`, which changes nothing the model sees, is appended at once even mid-run,
   * unless a reset is queued: then it lands after the reset.
   */
  readonly submit: (
    draft: Draft,
  ) => Effect.Effect<Submission, ConversationBusy | InvalidSubmission | CommitError>
  /**
   * Admit a `$reset` write that starts a new context, carrying `handoff` as a user message when
   * given; while busy, it is placed at the next boundary.
   */
  readonly reset: (handoff?: string) => Effect.Effect<Submission, CommitError>
  /**
   * Start a manual compaction and return its task. It summarizes while the conversation keeps
   * working, and places its summary through a write submission: at once when idle, otherwise at
   * the next boundary.
   */
  readonly compact: (instructions?: string) => Effect.Effect<TaskId, CommitError>
  /**
   * Change what the conversation asks with, in its `$agent`: a given field replaces the stored
   * one, `null` clears it. Its next request resolves the change.
   */
  readonly configure: (change: AgentState.Change) => Effect.Effect<void, CommitError>
  /** The agent the conversation's next request would resolve, by name. */
  readonly agent: Effect.Effect<AgentState.View, SnapshotError>
  /** A Session commit whose `tx.createTask()` defaults to this conversation. */
  readonly commit: <A, E, R>(
    change: (tx: Tx) => Effect.Effect<A, E, R>,
    scope?: Omit<TransactionScope, 'conversationId'>,
  ) => Effect.Effect<A, E | CommitError, R>
  /** Fork-aware history of this conversation, newest first unless `query.order` is `ascending`. */
  readonly entries: (
    query: Omit<EntryQuery, 'conversationId'>,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<EntryRecord>, CommitError>
  /**
   * What the model sees: the messages the next request would send and the active entries they
   * came from, now or as of the visible entry `at`, which is what a fork at `at` starts with.
   * Writes nothing.
   */
  readonly context: (options?: { readonly at?: EntryId }) => Effect.Effect<ContextView, CommitError>
  /**
   * Watch the active transcript and built-in documents, from their committed state now, while
   * the scope is open. Changes fail with `SessionFailed` when the Session fails.
   */
  readonly view: Effect.Effect<Watch<ConversationView, SessionFailed>, CommitError, Scope.Scope>
  /** Watch the conversation's agent events, from a snapshot now, while the scope is open. */
  readonly events: Effect.Effect<AgentEvent.Events<SessionFailed>, CommitError, Scope.Scope>
  /** Fork this conversation at a visible entry, in one commit. */
  readonly fork: <E = never>(
    at: EntryId,
    options: CreateOptions<E>,
  ) => Effect.Effect<Conversation, E | CommitError>
  /**
   * Withdraw queued inputs, which settle `unanswered` with reason `aborted` (queued writes stay),
   * abort-mark every live non-background task ordinary traversal from the conversation reaches,
   * and wait until it is idle.
   */
  readonly abort: (
    options?: AbortOptions,
  ) => Effect.Effect<void, TaskNotFound | SchedulerClosed | CommitError | StorageError>
  /**
   * Wait until ordinary traversal from the conversation, through the conversations its
   * non-background tasks own, reaches no live non-background task.
   */
  readonly waitForIdle: Effect.Effect<void, SchedulerClosed | CommitError>
}
