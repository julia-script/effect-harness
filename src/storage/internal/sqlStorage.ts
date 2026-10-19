import { Effect, Semaphore } from 'effect'
import type { Scope } from 'effect'
import { SqlClient, SqlError } from 'effect/sql'
import type { IdSchema, Service, SqlOptions, StorageId } from '../Storage.ts'
import type { StorageWrite } from '../schema/Write.ts'
import { StorageError } from '../StorageError.ts'
import type { StorageRequestError } from '../StorageError.ts'
import { commit, metadata } from './sql/commit.ts'
import * as Documents from './sql/documents.ts'
import * as Entries from './sql/entries.ts'
import * as Reads from './sql/reads.ts'
import { makeDb } from './sql/rows.ts'
import type { Db } from './sql/rows.ts'
import { checkDialect, makeSchema, migrate } from './sql/tables.ts'

/**
 * SQL implementation of `Storage` over Effect's `SqlClient`, ported from pi's
 * `packages/durable/src/storage/sqlite/storage.ts`.
 *
 * The store depends only on the `SqlClient` service, so any driver whose dialect is SQLite or
 * PostgreSQL serves it. Every commit is one transaction; the database serves every read. The
 * owning Session is the only writer of its tables, so the next free ID is kept in memory between
 * commits, as pi does.
 */

interface SqlState {
  readonly db: Db
  nextId: number
  closed: boolean
}

/** A driver failure is a backend I/O failure: fatal to the Session. */
const ioFailure = (cause: SqlError.SqlError) =>
  new StorageError({ message: 'SQL storage query failed', cause })

/** Run an operation while the store is open, reporting driver failures as `StorageError`. */
const guard = <A, E extends StorageError | StorageRequestError>(
  state: SqlState,
  effect: Effect.Effect<A, E | SqlError.SqlError>,
): Effect.Effect<A, E | StorageError> =>
  Effect.suspend(() =>
    state.closed
      ? Effect.fail(new StorageError({ message: 'SqlStorage is closed' }))
      : Effect.mapError(effect, (error) => (SqlError.isSqlError(error) ? ioFailure(error) : error)),
  )

const mint = <I extends StorageId>(state: SqlState, id: IdSchema<I>) =>
  guard(
    state,
    Effect.suspend(() => {
      if (!Number.isSafeInteger(state.nextId)) {
        return Effect.fail(new StorageError({ message: 'ID space is exhausted' }))
      }
      const value = id.make(state.nextId)
      state.nextId++
      return Effect.succeed(value)
    }),
  )

const makeCommit = (state: SqlState) => {
  const lock = Semaphore.makeUnsafe(1)
  return (writes: ReadonlyArray<StorageWrite>) =>
    lock.withPermit(
      guard(
        state,
        Effect.map(
          Effect.suspend(() => commit(state.db, writes, state.nextId)),
          (committed) => {
            state.nextId = Math.max(state.nextId, committed.nextId)
            return committed.seq
          },
        ),
      ),
    )
}

const service = (state: SqlState): Service => {
  const { db } = state
  return {
    commit: makeCommit(state),
    mintId: (id) => mint(state, id),
    conversation: (id) => guard(state, Reads.conversation(db, id)),
    scanConversations: (query, limit, cursor) =>
      guard(state, Reads.scanConversations(db, query, limit, cursor)),
    entry: (id) => guard(state, Entries.storedEntry(db, id)),
    visibleEntry: (conversationId, id) =>
      guard(state, Entries.visibleEntry(db, conversationId, id)),
    findLatestHeadMarker: (conversationId, atOrBefore) =>
      guard(state, Entries.findLatestHeadMarker(db, conversationId, atOrBefore)),
    scanEntries: (query, limit, cursor) =>
      guard(state, Entries.scanEntries(db, query, limit, cursor)),
    task: (id) => guard(state, Reads.task(db, id)),
    scanTasks: (query, limit, cursor) => guard(state, Reads.scanTasks(db, query, limit, cursor)),
    submission: (id) => guard(state, Reads.submission(db, id)),
    scanSubmissions: (query, limit, cursor) =>
      guard(state, Reads.scanSubmissions(db, query, limit, cursor)),
    submissionByRequest: (conversationId, requestId) =>
      guard(state, Reads.submissionByRequest(db, conversationId, requestId)),
    findDocument: (address, at) => guard(state, Documents.findDocument(db, address, at)),
    document: (id, at) => guard(state, Documents.materializeDocument(db, id, at)),
    scanDocuments: (query, limit, cursor) =>
      guard(state, Documents.scanDocuments(db, query, limit, cursor)),
  }
}

/**
 * Open the store on the `SqlClient` in context, creating or migrating its tables. Closing the
 * scope closes the store; the client's own lifetime is the caller's.
 */
export const makeSql = Effect.fnUntraced(function* (
  options: SqlOptions = {},
): Effect.fn.Return<Service, StorageError, SqlClient.SqlClient | Scope.Scope> {
  // Column names are written as stored, whatever name transforms the client applies.
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()
  yield* checkDialect(sql)
  const schema = yield* makeSchema(options.prefix ?? '')
  const db = makeDb(sql, schema)
  const nextId = yield* migrate(sql, schema).pipe(
    Effect.andThen(metadata(db)),
    Effect.map((row) => row.next_id),
    Effect.catchTag('SqlError', (cause) => Effect.fail(ioFailure(cause))),
  )
  const state: SqlState = { db, nextId, closed: false }
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      state.closed = true
    }),
  )
  return service(state)
})
