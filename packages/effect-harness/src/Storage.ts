/** Persistence capabilities and backend layers; dependencies are captured during construction. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
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
import * as Store from './internal/Store.js'
import * as Memory from './internal/Memory.js'
import * as Jsonl from './internal/Jsonl.js'
import * as Sql from './internal/Sql.js'
import type * as SqlClient from 'effect/sql/SqlClient'
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
    /**
     * Reads validated identity, policy and version without decoding the document body.
     * Built-in adapters supply this optional capability. Custom adapters must supply it
     * for ordinary unstaged document retirement and terminal task document cleanup,
     * as well as explicit metadata retirement. Without it those operations fail with
     * SessionError(invalid); they never fall back to reading the body.
     */
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

/**
 * Volatile storage using the same record engine and commit format as JSONL and SQL.
 * Requires no platform services. Each fresh Layer construction starts empty;
 * share its built context to keep state across runtime restarts in the same process.
 * Closing the storage scope ends access; a new construction cannot recover its data.
 */
export const layerMemory: Layer.Layer<Storage | StorageFormat, StorageError> = Layer.effectContext(
  Memory.make.pipe(
    Effect.flatMap(Store.make),
    Effect.map((storage) =>
      Context.make(Storage, storage).pipe(Context.add(StorageFormat, storage.storageFormat)),
    ),
  ),
)

/**
 * Readable JSONL commit log with named document and active-task payload streams.
 * The shared state engine owns record validation, replay and retention. Own one
 * writer per path across processes. Keep `<filePath>.parts` with the main log.
 * This spike uses a new format; sidecars-v1 files must be kept separately.
 * IDs allocated without a committed record may be reused after reopening.
 */
export const layerJsonl = (options: {
  readonly filePath: string
}): Layer.Layer<Storage | StorageFormat, StorageError, FileSystem.FileSystem> =>
  Layer.effectContext(
    Jsonl.make(options).pipe(
      Effect.flatMap(Store.make),
      Effect.map((storage) =>
        Context.make(Storage, storage).pipe(Context.add(StorageFormat, storage.storageFormat)),
      ),
    ),
  )

/**
 * SQLite commit and payload tables using the same state engine and format as JSONL.
 * Provide an Effect SqlClient from the host. Each storeId selects an independent store.
 * Own one writer per store and call it outside application SQL transactions.
 * Existing EventJournal / legacy SQL tables are left untouched; no migration is performed.
 */
export const layerSql = (options?: {
  readonly storeId?: string
}): Layer.Layer<Storage | StorageFormat, StorageError, SqlClient.SqlClient> =>
  Layer.effectContext(
    Sql.make(options).pipe(
      Effect.flatMap(Store.make),
      Effect.map((storage) =>
        Context.make(Storage, storage).pipe(Context.add(StorageFormat, storage.storageFormat)),
      ),
    ),
  )
