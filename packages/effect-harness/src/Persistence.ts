/** Atomic record persistence for one embedded harness owner. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
import * as Record from './Record.ts'
import * as Schema from 'effect/Schema'
import type * as Document from './Document.ts'
import type * as Identity from './Identity.ts'
import type { StorageError } from './StorageError.ts'

export const Metadata = Schema.Struct({
  revision: Record.JournalCursor,
  nextId: Schema.Int.check(Schema.isBetween({ minimum: 2, maximum: Number.MAX_SAFE_INTEGER + 1 })),
})
export type Metadata = typeof Metadata.Type
export const ConversationQuery = Schema.Struct({
  ownerConversationId: Schema.optionalKey(Record.ConversationId),
  ownerTaskId: Schema.optionalKey(Record.TaskId),
})
export interface ConversationQuery {
  readonly ownerConversationId?: Record.ConversationId | undefined
  readonly ownerTaskId?: Record.TaskId | undefined
}
export const EntryQuery = Schema.Struct({
  conversationId: Record.ConversationId,
  minEntryId: Schema.optionalKey(Record.EntryId),
  maxEntryId: Schema.optionalKey(Record.EntryId),
})
export interface EntryQuery {
  readonly conversationId: Record.ConversationId
  readonly minEntryId?: Record.EntryId | undefined
  readonly maxEntryId?: Record.EntryId | undefined
}
export const TaskQuery = Schema.Struct({
  conversationId: Schema.optionalKey(Record.ConversationId),
  kind: Schema.optionalKey(Schema.String),
  abortRequested: Schema.optionalKey(Schema.Boolean),
  background: Schema.optionalKey(Schema.Boolean),
  owner: Schema.optionalKey(Record.TaskId),
  status: Schema.optionalKey(
    Schema.Literals(['pending', 'running', 'waiting', 'completing', 'terminal']),
  ),
})
export type TaskQuery = Partial<
  Pick<Record.Task, 'conversationId' | 'kind' | 'abortRequested' | 'background' | 'owner'>
> & {
  readonly status?: Record.Task['state']['status'] | undefined
}
export const SubmissionQuery = Schema.Struct({
  conversationId: Schema.optionalKey(Record.ConversationId),
  status: Schema.optionalKey(Schema.Literals(['queued', 'placed', 'done', 'unanswered'])),
  type: Schema.optionalKey(Schema.Literals(['input', 'write'])),
})
export type SubmissionQuery = Partial<Pick<Record.Submission, 'conversationId' | 'status' | 'type'>>
export const DocumentQuery = Schema.Struct({
  scope: Record.Scope,
  at: Schema.optionalKey(Schema.Union([Record.Seq, Schema.Literal('current')])),
  kind: Schema.optionalKey(Schema.String),
})
export interface DocumentQuery {
  readonly scope: Record.Scope
  readonly at?: Record.Point | undefined
  readonly kind?: string | undefined
}
export const EntryRecord = Schema.Struct({ entry: Record.Entry, commitSeq: Record.Seq })
export type EntryRecord = typeof EntryRecord.Type

/** Storage adapters supply this service; the runtime never requires a physical database. */
export class Persistence extends Context.Service<Persistence, Service>()(
  'effect-harness/Persistence',
) {}
export interface Service {
  readonly metadata: Effect.Effect<Metadata, StorageError>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Option.Option<Record.Conversation>, StorageError>
  readonly entry: (id: Record.EntryId) => Effect.Effect<Option.Option<Record.Entry>, StorageError>
  readonly entryRecord: (
    id: Record.EntryId,
  ) => Effect.Effect<Option.Option<EntryRecord>, StorageError>
  readonly task: (id: Record.TaskId) => Effect.Effect<Option.Option<Record.Task>, StorageError>
  readonly submission: (
    id: Record.SubmissionId,
  ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
  readonly submissionByRequest: (
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
  ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
  readonly document: (
    id: Record.DocumentId,
    at?: Record.Point,
  ) => Effect.Effect<Option.Option<Document.Document.Snapshot>, StorageError>
  readonly findDocument: (
    address: Record.Address,
    at?: Record.Point,
  ) => Effect.Effect<Option.Option<Document.Document.Snapshot>, StorageError>
  readonly scanConversations: (
    query?: ConversationQuery,
  ) => Stream.Stream<Record.Conversation, StorageError>
  readonly scanEntries: (query: EntryQuery) => Stream.Stream<Record.Entry, StorageError>
  readonly scanTasks: (query?: TaskQuery) => Stream.Stream<Record.Task, StorageError>
  readonly scanSubmissions: (
    query?: SubmissionQuery,
  ) => Stream.Stream<Record.Submission, StorageError>
  readonly scanDocuments: (query: DocumentQuery) => Stream.Stream<Record.Document, StorageError>
  /** The optional allocator value commits with the batch, including IDs minted without records. */
  readonly commit: (
    writes: ReadonlyArray<Record.Write>,
    nextId?: number,
  ) => Effect.Effect<Record.Frame, StorageError>
  readonly seal: Effect.Effect<void>
  readonly isClosed: Effect.Effect<boolean>
}
