/** Persistence capabilities and backend layers; dependencies are captured during construction. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import type * as Stream from 'effect/Stream'
import type * as EventJournal from 'effect/eventlog/EventJournal'
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
import * as Journal from './internal/Journal.js'
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

const services = (storage: Effect.Success<typeof Memory.make>) =>
  Context.make(Storage, storage).pipe(Context.add(StorageFormat, storage.storageFormat))

export const layerMemory: Layer.Layer<Storage | StorageFormat, StorageError> = Layer.effectContext(
  Memory.make.pipe(Effect.map(services)),
)

/**
 * Replays complete commits from an application-supplied EventJournal into local state.
 * Own one runtime per storeId, including across processes and journal instances.
 * Each batch, ID reservation and format upgrade is one event. Reads use the local
 * projection; close and reopen to reconcile an uncertain append. The journal must
 * acknowledge durable writes independently of application transactions. Construct
 * and use its backend outside ambient transactions, with a dedicated client.
 * Effect 4.0.1's IndexedDB journal does not wait for transaction completion and
 * cannot satisfy this contract; use an implementation that awaits commit or abort.
 * Existing SQL-table and JSONL containers are not imported into this journal format.
 */
export const layerJournal = (
  options: {
    readonly storeId?: string
  } = {},
): Layer.Layer<Storage | StorageFormat, StorageError, EventJournal.EventJournal> =>
  Layer.effectContext(Journal.make(options).pipe(Effect.map(services)))

/**
 * Scoped single-writer journal. Own one writer per file, including across processes.
 * Keep the file and its adjacent `<filePath>.parts` directory together when copying or
 * backing up a closed store. This physical layout rejects older inline-row journals.
 * New sidecars and directory entries are synced before each journal marker. Normal
 * commits reclaim superseded task rows and current-only document content after the
 * marker is durable; reopening removes orphan sidecars left by interrupted writes.
 * Format upgrades and domain writes share that writer's semaphore and synced frames.
 * The marker rejects old readers on reopen; it cannot stop an already-open foreign
 * writer appending a legacy frame. Such downgrade frames are corruption on reopen.
 */
export const layerJsonl = (options: {
  readonly filePath: string
}): Layer.Layer<Storage | StorageFormat, StorageError, FileSystem.FileSystem> =>
  Layer.effectContext(Jsonl.make(options).pipe(Effect.map(services)))
