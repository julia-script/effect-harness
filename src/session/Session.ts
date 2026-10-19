/**
 * The `Session` service: one mutation line over a `Storage`, its transactions, and the
 * publication of what each commit changed. Ported from pi's `packages/durable/src/session`.
 */
import { Context, Layer, Schema } from 'effect'
import type { Duration, Effect, PubSub, Scope } from 'effect'
import type { Storage } from '../storage/Storage.ts'
import { StorageError } from '../storage/StorageError.ts'
import * as Conversation from '../storage/schema/Conversation.ts'
import { ConversationRecord } from '../storage/schema/Conversation.ts'
import * as Entry from '../storage/schema/Entry.ts'
import { EntryRecord } from '../storage/schema/Entry.ts'
import { ConversationId, EntryId, Seq, TaskId } from '../storage/schema/Ids.ts'
import type { SubmissionId } from '../storage/schema/Ids.ts'
import type {
  ConversationQuery,
  Cursor,
  EntryQuery,
  Page,
  TaskQuery,
} from '../storage/schema/Scan.ts'
import { SubmissionRecord } from '../storage/schema/Submission.ts'
import type { SubmissionCreate } from '../storage/schema/Submission.ts'
import { TaskRecord } from '../storage/schema/Task.ts'
import * as Document from './Document.ts'
import * as internal from './internal/session.ts'
import type * as Observation from './Observation.ts'
import type {
  CommitError,
  DocumentError,
  SessionClosed,
  SessionFailed,
  TxDocumentError,
  TxError,
  TxReadError,
} from './SessionError.ts'

/** Ownership selected explicitly whenever a conversation is created. */
export const ConversationOwnership = Conversation.ConversationOwnership
export type ConversationOwnership = Conversation.ConversationOwnership

/** Who owns a task: its conversation, or another task of the same conversation. */
export const TaskOwnership = Schema.Union([
  Schema.Struct({ kind: Schema.Literal('conversation') }),
  Schema.Struct({ kind: Schema.Literal('task'), taskId: TaskId }),
])
export type TaskOwnership = typeof TaskOwnership.Type

/** Creation options for a durable task. */
export const TaskOptions = Schema.Struct({
  /** Required: a task always names its owner. */
  ownership: TaskOwnership,
  /**
   * Default: the owner task's conversation, or the transaction's bound conversation; required
   * for conversation-owned tasks created by commits that are not bound to a conversation.
   */
  conversationId: Schema.optionalKey(ConversationId),
  /** Conversation-owned tasks only: excluded from idle waits, conversation aborts, and cascades. */
  background: Schema.optionalKey(Schema.Boolean),
  /** Its creator awaits it only in memory, so a later Harness abort-marks it with `restart`. */
  abandonOnRestart: Schema.optionalKey(Schema.Boolean),
})
export type TaskOptions = typeof TaskOptions.Type

/**
 * What `Tx.createTask()` needs of a task definition: its persisted kind and version, and the
 * checkpoint a new task starts from. The task runtime's definitions satisfy it.
 */
export interface TaskKind<I extends Schema.Json> {
  /** Registered task definition name, stored as the record's `kind`. */
  readonly name: string
  /** Positive integer version of the stored input and checkpoint shapes. */
  readonly version: number
  readonly initial: (input: I) => Schema.Json
}

/** Entry content supplied before the Session assigns identity and task attribution. */
export const EntryDraft = Entry.EntryDraft
export type EntryDraft = Entry.EntryDraft

/** Terminal status staged for a submission; identity, type, and entry come from its record. */
export const SubmissionSettlement = Schema.Union([
  Schema.Struct({ status: Schema.Literal('done'), answer: EntryId }),
  Schema.Struct({
    status: Schema.Literal('unanswered'),
    reason: Schema.String,
    detail: Schema.optionalKey(Schema.Json),
  }),
])
export type SubmissionSettlement = typeof SubmissionSettlement.Type

/** Defaults a commit binds to, as the task runtime's own commits do. */
export const TransactionScope = Schema.Struct({
  /** Default `Tx.createTask()` conversation. */
  conversationId: Schema.optionalKey(ConversationId),
  /** Task whose runtime commit this is; stamped as `byTaskId` on appended entries. */
  taskId: Schema.optionalKey(TaskId),
})
export type TransactionScope = typeof TransactionScope.Type

/** Committed change of one document incarnation. */
export const DocumentCommitChange = Document.CommitChange
export type DocumentCommitChange = Document.CommitChange

/** A complete table record, published as the write that committed it. */
export const TableCommitChange = Schema.Union([
  Schema.Struct({ type: Schema.Literal('conversation'), value: ConversationRecord }),
  Schema.Struct({ type: Schema.Literal('entry'), value: EntryRecord }),
  Schema.Struct({ type: Schema.Literal('task'), value: TaskRecord }),
  Schema.Struct({ type: Schema.Literal('submission'), value: SubmissionRecord }),
])
export type TableCommitChange = typeof TableCommitChange.Type

export const CommitChange = Schema.Union([TableCommitChange, DocumentCommitChange])
export type CommitChange = typeof CommitChange.Type

/** Every change of one successful commit. Change order is unspecified. */
export const CommitPublication = Schema.Struct({ seq: Seq, changes: Schema.Array(CommitChange) })
export type CommitPublication = typeof CommitPublication.Type

/** Why a Session ended: its scope closed, or Storage failed it with `error`. */
export const SessionEnd = Schema.Union([
  Schema.Struct({ reason: Schema.Literal('closed') }),
  Schema.Struct({ reason: Schema.Literal('failed'), error: StorageError }),
])
export type SessionEnd = typeof SessionEnd.Type

/**
 * The transaction of one commit callback. Reads see committed state only, and only before the
 * transaction's first table write (`ReadAfterWrite`). Records it returns are immutable and are
 * shared with commit subscribers. Every operation fails once the callback has ended.
 */
export interface Tx {
  readonly conversation: (
    id: ConversationId,
  ) => Effect.Effect<ConversationRecord | undefined, TxReadError>
  readonly entry: (id: EntryId) => Effect.Effect<EntryRecord | undefined, TxReadError>
  readonly task: (id: TaskId) => Effect.Effect<TaskRecord | undefined, TxReadError>
  readonly submission: (
    id: SubmissionId,
  ) => Effect.Effect<SubmissionRecord | undefined, TxReadError>
  /** Committed submission with a conversation-scoped request ID. */
  readonly submissionByRequest: (
    conversationId: ConversationId,
    requestId: string,
  ) => Effect.Effect<SubmissionRecord | undefined, TxReadError>
  readonly scanConversations: (
    query: ConversationQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<ConversationRecord>, TxReadError>
  readonly scanEntries: (
    query: EntryQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<EntryRecord>, TxReadError>
  readonly scanTasks: (
    query: TaskQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<TaskRecord>, TxReadError>
  /** Newest visible entry of the conversation that carries a `head`. */
  readonly latestHeadMarker: (
    conversationId: ConversationId,
  ) => Effect.Effect<(EntryRecord & { readonly head: EntryId }) | undefined, TxReadError>
  /**
   * A document's committed value, as `Session.snapshot` reads it; never creates one, and ignores
   * this transaction's drafts. A table read: it fails after the first table write.
   */
  readonly snapshot: Document.Reads<TxReadError | DocumentError>
  /** Create a conversation with explicitly selected ownership. */
  readonly createConversation: (options: {
    readonly ownership: ConversationOwnership
  }) => Effect.Effect<ConversationRecord, TxDocumentError>
  /** Create the root conversation at its reserved ID, as a Harness does once. */
  readonly createRootConversation: Effect.Effect<ConversationRecord, TxDocumentError>
  /** Fork history at one visible entry; `asOf` documents copy as of it, `current` ones as now. */
  readonly forkConversation: (
    parentConversationId: ConversationId,
    at: EntryId,
    options: { readonly ownership: ConversationOwnership },
  ) => Effect.Effect<ConversationRecord, TxDocumentError>
  readonly appendEntry: (
    conversationId: ConversationId,
    draft: EntryDraft,
  ) => Effect.Effect<EntryRecord, TxError>
  readonly createTask: <I extends Schema.Json>(
    kind: TaskKind<I>,
    input: I,
    options: TaskOptions,
  ) => Effect.Effect<TaskId, TxError>
  /** Create a raw submission with a fresh ID. No admission rules apply. */
  readonly createSubmission: (create: SubmissionCreate) => Effect.Effect<SubmissionRecord, TxError>
  /**
   * Settle a queued or placed submission; only a placed input can be answered, and a settled
   * submission stays unchanged. Resolved at the end of the callback against this transaction's
   * latest record of it, so it is not a read and works after table writes.
   */
  readonly settleSubmission: (
    id: SubmissionId,
    settlement: SubmissionSettlement,
  ) => Effect.Effect<void, TxError>
  /** Place a queued submission at `entry`: an input becomes `placed`, a write `done`. */
  readonly placeSubmission: (id: SubmissionId, entry: EntryId) => Effect.Effect<void, TxError>
  /**
   * Replace one task record completely, as the task runtime does. Stamps `startedAt` on the first
   * change to `running` and `endedAt` on the change to `terminal`; a terminal task's documents
   * retire in the same commit.
   */
  readonly setTask: (record: TaskRecord) => Effect.Effect<TaskRecord, TxError>
  /** Records created or replaced so far, as the batch would commit them; not reads. */
  readonly stagedTasks: Effect.Effect<ReadonlyArray<TaskRecord>, TxError>
  readonly stagedConversations: Effect.Effect<ReadonlyArray<ConversationRecord>, TxError>
  readonly doc: Document.Acquire
  readonly retireDoc: Document.Retire
}

/**
 * Owner of one mutation line over a `Storage`.
 *
 * Commits are serialized: each runs its callback, validates the staged batch, persists it in one
 * `Storage.commit`, and publishes it, while it holds the line. Inside a callback read only through
 * `tx`; a nested `commit` would wait for the line the callback holds.
 *
 * The first `StorageError` fails the Session: that call and every later one fail with
 * `SessionFailed`. A `StorageRequestError` fails only the read that asked for it. Closing the
 * Session's scope lets admitted commits finish and refuses new ones with `SessionClosed`.
 */
export interface Service {
  /**
   * Run one atomic transaction. When the callback fails or is interrupted nothing is committed;
   * once the batch reaches Storage, interruption waits for it to settle.
   */
  readonly commit: <A, E, R>(
    change: (tx: Tx) => Effect.Effect<A, E, R>,
    scope?: TransactionScope,
  ) => Effect.Effect<A, E | CommitError, R>
  /** Receive every later commit's publication, in commit order, while the scope is open. */
  readonly subscribe: Effect.Effect<PubSub.Subscription<CommitPublication>, never, Scope.Scope>
  /**
   * Run `listener` with every later commit's publication, on the line, while the scope is open:
   * an index it keeps is exact for whatever runs on the line next. It must not wait for the line.
   */
  readonly observeCommits: (
    listener: (publication: CommitPublication) => Effect.Effect<void>,
  ) => Effect.Effect<void, never, Scope.Scope>
  /** Succeeds once the Session ended: its scope closed, or Storage failed it. */
  readonly ended: Effect.Effect<SessionEnd>
  readonly snapshot: Document.Snapshot
  readonly snapshotAsOf: Document.SnapshotAsOf
  readonly watchDoc: Observation.WatchDoc
  readonly documentState: Observation.DocumentState
  /** Drop every loaded document tracker; later access loads from Storage. */
  readonly unloadDocuments: Effect.Effect<void, SessionClosed | SessionFailed>
}

/**
 * Runs in every transaction that creates or forks a conversation, right after the conversation
 * record is staged, so a Harness can stage its built-in documents there. The record is already a
 * table write: table reads fail with `ReadAfterWrite`. A failure fails the creating operation.
 */
export type ConversationCreated = (
  tx: Tx,
  record: ConversationRecord,
) => Effect.Effect<void, TxDocumentError>

/** How a Session runs. */
export interface Options {
  readonly conversationCreated?: ConversationCreated
  /**
   * How long a loaded document stays cached after its last use. Default: ten minutes. `0` evicts
   * right after use; `Infinity` keeps documents for the Session's lifetime.
   */
  readonly documentRetention?: Duration.Input
}

/** The Session kernel of a durable harness. */
export class Session extends Context.Service<Session, Service>()(
  'effect-harness/session/Session',
) {}

/** Open a Session over the `Storage` in context. Closing the scope closes the Session. */
export const makeWith: (options: Options) => Effect.Effect<Service, never, Storage | Scope.Scope> =
  internal.make

/** `makeWith` with the default options. */
export const make: Effect.Effect<Service, never, Storage | Scope.Scope> = makeWith({})

/** A `Session` over the `Storage` provided to the layer. */
export const layerWith = (options: Options): Layer.Layer<Session, never, Storage> =>
  Layer.effect(Session, makeWith(options))

/** `layerWith` with the default options. */
export const layer: Layer.Layer<Session, never, Storage> = layerWith({})
