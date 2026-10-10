/** Persistence capabilities and backend layers; dependencies are captured during construction. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
import type * as SqlClient from 'effect/sql/SqlClient'
import type * as Record from './Record.js'
import type * as Identity from './Identity.js'
import type * as Sequence from './Sequence.js'
import type {
  StorageWrite,
  StorageId,
  StoredEntry,
  HeadMarker,
  DocumentAddress,
  DocumentPoint,
  DocumentRecord,
  StoredDocument,
  ConversationQuery,
  EntryQuery,
  TaskQuery,
  SubmissionQuery,
  DocumentQuery,
} from './Record.js'
import type { StorageError } from './StorageError.js'
import * as Memory from './internal/Memory.js'
import * as Sql from './internal/Sql.js'
import * as Jsonl from './internal/Jsonl.js'

export * from './Record.js'
export { StorageError } from './StorageError.js'

export class Storage extends Context.Service<
  Storage,
  {
    readonly commit: (
      storageWrites: Iterable<StorageWrite>,
    ) => Effect.Effect<Sequence.Sequence, StorageError>
    readonly mintId: <I extends StorageId>() => Effect.Effect<I, StorageError>
    readonly conversation: (
      id: Record.ConversationId,
    ) => Effect.Effect<Option.Option<Record.Conversation>, StorageError>
    /** Supplying a conversation restricts the lookup to its visible ancestry. */
    readonly entry: (
      id: Record.EntryId,
      options?: { readonly conversationId: Record.ConversationId },
    ) => Effect.Effect<Option.Option<StoredEntry>, StorageError>
    readonly findLatestHeadMarker: (
      conversationId: Record.ConversationId,
      atOrBeforeEntryId?: Record.EntryId,
    ) => Effect.Effect<Option.Option<HeadMarker>, StorageError>
    readonly task: (id: Record.TaskId) => Effect.Effect<Option.Option<Record.Task>, StorageError>
    readonly submission: (
      id: Record.SubmissionId,
    ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
    readonly submissionByRequest: (
      conversationId: Record.ConversationId,
      requestId: Identity.RequestId,
    ) => Effect.Effect<Option.Option<Record.Submission>, StorageError>
    /** Document reads default to the current point. */
    readonly findDocument: (
      address: DocumentAddress,
      at?: DocumentPoint,
    ) => Effect.Effect<Option.Option<DocumentRecord>, StorageError>
    readonly document: (
      id: Record.DocumentId,
      at?: DocumentPoint,
    ) => Effect.Effect<Option.Option<StoredDocument>, StorageError>
    /** Scans hide pagination and default to ascending ID order, except entries. */
    readonly scanConversations: (
      query?: ConversationQuery,
    ) => Stream.Stream<Record.Conversation, StorageError>
    /** Visible history, including inherited entries; defaults to descending ID order. */
    readonly scanEntries: (query: EntryQuery) => Stream.Stream<Record.Entry, StorageError>
    readonly scanTasks: (query?: TaskQuery) => Stream.Stream<Record.Task, StorageError>
    readonly scanSubmissions: (
      query?: SubmissionQuery,
    ) => Stream.Stream<Record.Submission, StorageError>
    readonly scanDocuments: (query: DocumentQuery) => Stream.Stream<DocumentRecord, StorageError>
  }
>()('effect-harness/Storage') {}

export const layerMemory: Layer.Layer<Storage> = Layer.effect(Storage, Memory.make)

/**
 * Uses the supplied SQL client with an independent durable transaction per write.
 * Initialization, commit and ID allocation reject that client's ambient transactions
 * with an invalid StorageError; invoke them outside SqlClient.withTransaction.
 */
export const layerSql: Layer.Layer<Storage, StorageError, SqlClient.SqlClient> = Layer.effect(
  Storage,
  Sql.make,
)

export const layerJsonl = (options: {
  readonly filePath: string
}): Layer.Layer<Storage, StorageError, FileSystem.FileSystem> =>
  Layer.effect(Storage, Jsonl.make(options))
