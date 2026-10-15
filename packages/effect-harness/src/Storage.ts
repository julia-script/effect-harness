/** Persistence capabilities and backend layers; dependencies are captured during construction. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
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
import { StorageFormat } from './StorageFormat.js'

export * from './Record.js'
export { StorageError } from './StorageError.js'

/** Current document identity and stored schema version, without materializing its content. */
export interface DocumentMetadata {
  readonly record: DocumentRecord
  readonly version: number
}

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
    /** Optional for structural adapters; consumers must reject unavailable metadata support. */
    readonly documentMetadata?: (
      id: Record.DocumentId,
    ) => Effect.Effect<Option.Option<DocumentMetadata>, StorageError>
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

const services = (storage: Effect.Success<typeof Memory.make>) =>
  Context.make(Storage, storage).pipe(Context.add(StorageFormat, storage.storageFormat))

export const layerMemory: Layer.Layer<Storage | StorageFormat, StorageError> = Layer.effectContext(
  Memory.make.pipe(Effect.map(services)),
)

/**
 * Uses the supplied SQL client with an independent durable transaction per write.
 * Initialization, commit and ID allocation reject that client's ambient transactions
 * with an invalid StorageError; invoke them outside SqlClient.withTransaction.
 * Document headers, revisions, binary image objects and owner manifests use separate tables; commits append new revisions and
 * reclaim superseded current-only content in the same transaction. Schema version 3
 * rejects older physical layouts without migration. StorageFormat feature activation
 * remains independent and shares the metadata-row lock with domain writes.
 */
export const layerSql: Layer.Layer<Storage | StorageFormat, StorageError, SqlClient.SqlClient> =
  Layer.effectContext(Sql.make.pipe(Effect.map(services)))

/**
 * Scoped single-writer journal. Own one writer per file, including across processes.
 * Keep the file and its adjacent `<filePath>.parts` and `<filePath>.media` directories together when copying or
 * backing up a closed store. The sidecars-v3 layout rejects older journals.
 * New image objects, sidecars and directory entries are synced before each journal marker. Normal
 * commits reclaim superseded task rows and current-only document content after the
 * marker is durable; reopening removes orphan sidecars and images left by interrupted writes.
 * Format upgrades and domain writes share that writer's semaphore and synced frames.
 * The marker rejects old readers on reopen; it cannot stop an already-open foreign
 * writer appending a legacy frame. Such downgrade frames are corruption on reopen.
 */
export const layerJsonl = (options: {
  readonly filePath: string
}): Layer.Layer<Storage | StorageFormat, StorageError, FileSystem.FileSystem> =>
  Layer.effectContext(Jsonl.make(options).pipe(Effect.map(services)))
