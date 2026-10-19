/**
 * The `Storage` service: the Session's atomic persistence boundary, and its in-memory, JSONL, and
 * SQL layers.
 */
import { Context, Layer } from 'effect'
import type { Effect, FileSystem, Path, Scope } from 'effect'
import type { SqlClient } from 'effect/sql'
import type { StorageRequestError } from './StorageError.ts'
import type { ConversationRecord } from './schema/Conversation.ts'
import type {
  DocumentAddress,
  DocumentPoint,
  DocumentRecord,
  StoredDocument,
} from './schema/Document.ts'
import type { EntryRecord, StoredEntry } from './schema/Entry.ts'
import type {
  ConversationId,
  DocumentId,
  EntryId,
  Seq,
  SubmissionId,
  TaskId,
} from './schema/Ids.ts'
import type {
  ConversationQuery,
  Cursor,
  DocumentQuery,
  EntryQuery,
  Page,
  SubmissionQuery,
  TaskQuery,
} from './schema/Scan.ts'
import type { SubmissionRecord } from './schema/Submission.ts'
import type { TaskRecord } from './schema/Task.ts'
import type { StorageWrite } from './schema/Write.ts'
import * as jsonl from './internal/jsonlStorage.ts'
import * as internal from './internal/memoryStorage.ts'
import * as sql from './internal/sqlStorage.ts'
import type { StorageError } from './StorageError.ts'

/** Any ID from the Session-global numeric namespace. */
export type StorageId = ConversationId | EntryId | TaskId | SubmissionId | DocumentId

/** An ID schema, such as `EntryId`, that brands a minted number. */
export interface IdSchema<I extends StorageId> {
  readonly make: (value: number) => I
}

/** The entry a head marker is, with the head it selects. */
export type HeadMarker = EntryRecord & { readonly head: EntryId }

/**
 * Atomic persistence boundary for Session records, modeled on pi's durable `Storage`.
 *
 * Storage trusts the owning Session to supply semantically valid records, references, ancestry,
 * and transitions. Implementations enforce atomicity, global ID ownership, immutable
 * conversation and entry creation, document record consistency, and detached values; the Session
 * serializes commits.
 *
 * A `StorageError` is final: it fails the Session. A `StorageRequestError` fails only the read
 * that asked for it (an unknown conversation, a foreign or malformed cursor, history a document
 * does not keep) and has no durable effect. Interruption replaces pi's abort context, and closing
 * the Layer's scope replaces `close()`.
 */
export interface Service {
  /**
   * Atomically persist one batch and return its sequence. Once it succeeds, later reads observe
   * it; when it fails, none of the batch is applied.
   */
  readonly commit: (writes: ReadonlyArray<StorageWrite>) => Effect.Effect<Seq, StorageError>

  /**
   * A fresh ID greater than every ID minted or stored before, also across reopen: scans order by
   * ID as creation order. ID 1 is reserved for the root conversation. Pass the ID schema to brand
   * it, as in `mintId(EntryId)`.
   */
  readonly mintId: <I extends StorageId>(id: IdSchema<I>) => Effect.Effect<I, StorageError>

  /** Look up one conversation by exact ID. */
  readonly conversation: (
    id: ConversationId,
  ) => Effect.Effect<ConversationRecord | undefined, StorageError>

  /** Scan conversations in `query.order` (default ascending) by ID. */
  readonly scanConversations: (
    query: ConversationQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<ConversationRecord>, StorageRequestError | StorageError>

  /** Look up one global entry and the sequence of the commit that persisted it. */
  readonly entry: (id: EntryId) => Effect.Effect<StoredEntry | undefined, StorageError>

  /** Look up one entry only when it is visible through the conversation's fork ancestry. */
  readonly visibleEntry: (
    conversationId: ConversationId,
    id: EntryId,
  ) => Effect.Effect<StoredEntry | undefined, StorageRequestError | StorageError>

  /**
   * The newest visible entry with a `head` at or below the optional inclusive cutoff. The entry is
   * the marker; its `head` is the active range's lower bound.
   */
  readonly findLatestHeadMarker: (
    conversationId: ConversationId,
    atOrBeforeEntryId?: EntryId,
  ) => Effect.Effect<HeadMarker | undefined, StorageRequestError | StorageError>

  /** Scan the inclusive visible range in `query.order` (default descending). */
  readonly scanEntries: (
    query: EntryQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<EntryRecord>, StorageRequestError | StorageError>

  /** Look up the latest complete record for one task. */
  readonly task: (id: TaskId) => Effect.Effect<TaskRecord | undefined, StorageError>

  /** Scan tasks matching every supplied filter in `query.order` (default ascending) by ID. */
  readonly scanTasks: (
    query: TaskQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<TaskRecord>, StorageRequestError | StorageError>

  /** Look up the latest complete record for one admitted submission. */
  readonly submission: (
    id: SubmissionId,
  ) => Effect.Effect<SubmissionRecord | undefined, StorageError>

  /** Scan submissions matching every supplied filter in `query.order` (default ascending) by ID. */
  readonly scanSubmissions: (
    query: SubmissionQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<SubmissionRecord>, StorageRequestError | StorageError>

  /** Find a submission by its conversation-scoped host deduplication key. */
  readonly submissionByRequest: (
    conversationId: ConversationId,
    requestId: string,
  ) => Effect.Effect<SubmissionRecord | undefined, StorageError>

  /** Resolve the incarnation occupying one exact logical address at the selected point. */
  readonly findDocument: (
    address: DocumentAddress,
    at: DocumentPoint,
  ) => Effect.Effect<DocumentRecord | undefined, StorageError>

  /**
   * Materialize one incarnation by ID at the selected point, without following a replacement at
   * its address.
   */
  readonly document: (
    id: DocumentId,
    at: DocumentPoint,
  ) => Effect.Effect<StoredDocument | undefined, StorageRequestError | StorageError>

  /** Scan incarnations alive in one exact scope at the selected point, ascending by ID. */
  readonly scanDocuments: (
    query: DocumentQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<DocumentRecord>, StorageRequestError | StorageError>
}

/** The Session's storage backend. */
export class Storage extends Context.Service<Storage, Service>()(
  'effect-harness/storage/Storage',
) {}

/**
 * Build an empty in-memory store, ported from pi's `storage/memory.ts`. Closing the scope closes
 * the store: every later operation fails with a `StorageError`.
 */
export const makeMemory: Effect.Effect<Service, never, Scope.Scope> = internal.makeMemory

/** A fresh, empty in-memory `Storage` for each build of the layer. */
export const layerMemory: Layer.Layer<Storage> = Layer.effect(Storage, makeMemory)

/** Options for a JSONL store. */
export interface JsonlOptions {
  /**
   * Flush every appended sidecar to stable storage before the marker that publishes it, and the
   * main file before reclaiming obsolete sidecars. Defaults to `false`.
   */
  readonly fsync?: boolean
}

/**
 * Open or create a JSONL storage directory, ported from pi's `storage/jsonl/storage.ts`, and
 * recover every commit whose marker is complete. Fails with a `StorageError` when the files are
 * corrupt. Closing the scope closes the store.
 *
 * A failed write may leave a partial commit on disk, so it poisons the store: every later
 * operation fails until the directory is reopened.
 */
export const makeJsonl: (
  directory: string,
  options?: JsonlOptions,
) => Effect.Effect<Service, StorageError, FileSystem.FileSystem | Path.Path | Scope.Scope> =
  jsonl.makeJsonl

/** A `Storage` persisted as JSONL files in `directory`. */
export const layerJsonl = (
  directory: string,
  options?: JsonlOptions,
): Layer.Layer<Storage, StorageError, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(Storage, makeJsonl(directory, options))

/** Options for a SQL store. */
export interface SqlOptions {
  /**
   * Prefix for every table and index the store creates, so several stores, or other tables, can
   * share one database. It must be a plain SQL identifier and must not start with `sqlite_` or
   * `pg_`. Existing data is not migrated when it changes. Defaults to none, as in pi.
   */
  readonly prefix?: string
}

/**
 * Open a SQL store, ported from pi's `storage/sqlite/storage.ts`, on the `SqlClient` in context:
 * any Effect SQL driver whose dialect is SQLite or PostgreSQL, such as `@effect/sql-sqlite-node`,
 * `@effect/sql-sqlite-bun`, `@effect/sql-pg`, or `@effect/sql-pglite`. Creates or migrates the
 * tables with Effect's `Migrator`, and fails with a `StorageError` for another dialect.
 *
 * Each commit is one transaction. The owning Session must be the only writer of the store's
 * tables. Closing the scope closes the store, not the client.
 */
export const makeSql: (
  options?: SqlOptions,
) => Effect.Effect<Service, StorageError, SqlClient.SqlClient | Scope.Scope> = sql.makeSql

/** A `Storage` kept in the tables of the `SqlClient` provided to the layer. */
export const layerSql = (
  options?: SqlOptions,
): Layer.Layer<Storage, StorageError, SqlClient.SqlClient> =>
  Layer.effect(Storage, makeSql(options))
