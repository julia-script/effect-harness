/** The physical SQLite client is confined to this storage adapter. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import type * as Scope from 'effect/Scope'
import type { Service } from '../../Persistence.ts'
import {
  CorruptError,
  IoError,
  rejected,
  uncertain,
  type StorageError,
} from '../../StorageError.ts'
import * as records from './records.ts'
import * as Record from '../../Record.ts'

const Metadata = Schema.Struct({
  format: Schema.Literal(2),
  revision: Record.JournalCursor,
  nextId: Schema.Int.check(Schema.isBetween({ minimum: 2, maximum: Number.MAX_SAFE_INTEGER + 1 })),
})
const RowFromString = Schema.fromJsonString(records.Row)
const decode = (payload: string) =>
  Schema.decodeEffect(RowFromString)(payload).pipe(
    Effect.mapError((cause) => rejected('Stored record is malformed', CorruptError, cause)),
  )

export const make: Effect.Effect<Service, StorageError, SqlClient.SqlClient | Scope.Scope> =
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const io = <A>(
      effect: Effect.Effect<A, import('effect/sql/SqlError').SqlError>,
    ): Effect.Effect<A, StorageError> =>
      effect.pipe(Effect.mapError((cause) => rejected('SQLite operation failed', IoError, cause)))
    // EXCLUSIVE mode retains SQLite's lock until this scoped connection closes. Process death
    // releases it automatically; another harness cannot concurrently own the same database.
    yield* io(sql`PRAGMA locking_mode = EXCLUSIVE`)
    yield* io(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`CREATE TABLE IF NOT EXISTS harness_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), format INTEGER NOT NULL, revision INTEGER NOT NULL, nextId INTEGER NOT NULL)`
          yield* sql`INSERT OR IGNORE INTO harness_metadata VALUES (1, 2, 0, 2)`
          yield* sql`CREATE TABLE IF NOT EXISTS harness_records (id INTEGER PRIMARY KEY, category TEXT NOT NULL, payload TEXT NOT NULL, conversationId INTEGER, kind TEXT, ownerTaskId INTEGER, ownerConversationId INTEGER, status TEXT, background INTEGER, abortRequested INTEGER, requestId TEXT, address TEXT, scope TEXT, createdAt INTEGER, retiredAt INTEGER, type TEXT)`
          yield* sql`CREATE INDEX IF NOT EXISTS harness_category_id ON harness_records(category, id)`
          yield* sql`CREATE INDEX IF NOT EXISTS harness_conversation_id ON harness_records(category, conversationId, id)`
          yield* sql`CREATE INDEX IF NOT EXISTS harness_task_status ON harness_records(category, status, id)`
          yield* sql`CREATE INDEX IF NOT EXISTS harness_address ON harness_records(address, createdAt, retiredAt)`
          yield* sql`CREATE INDEX IF NOT EXISTS harness_scope ON harness_records(scope, id)`
          yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS harness_request ON harness_records(conversationId, requestId) WHERE requestId IS NOT NULL`
        }),
      ),
    )
    const metadata = Effect.gen(function* () {
      const rows = yield* io(
        sql`SELECT format, revision, nextId FROM harness_metadata WHERE singleton = 1`,
      )
      const value = yield* Schema.decodeUnknownEffect(Metadata)(rows[0]).pipe(
        Effect.mapError((cause) =>
          rejected('Unsupported or corrupt persistence format', CorruptError, cause),
        ),
      )
      return { revision: value.revision, nextId: value.nextId }
    })
    yield* metadata
    return yield* records.make({
      metadata,
      get: (id) =>
        io(sql<{ payload: string }>`SELECT payload FROM harness_records WHERE id = ${id}`).pipe(
          Effect.flatMap((rows) =>
            rows[0] === undefined
              ? Effect.succeedNone
              : decode(rows[0].payload).pipe(Effect.asSome),
          ),
        ),
      page: Effect.fn('SQLite.page')(function* (kind, after, limit, filter) {
        const clauses = [sql`category = ${kind}`, sql`id > ${after}`]
        for (const key of [
          'conversationId',
          'kind',
          'ownerTaskId',
          'ownerConversationId',
          'status',
          'background',
          'abortRequested',
          'requestId',
          'address',
          'scope',
          'type',
        ] as const) {
          const value = filter[key]
          if (value !== undefined)
            clauses.push(sql`${sql(key)} = ${typeof value === 'boolean' ? Number(value) : value}`)
        }
        if (filter.minId !== undefined) clauses.push(sql`id >= ${filter.minId}`)
        if (filter.maxId !== undefined) clauses.push(sql`id <= ${filter.maxId}`)
        if (filter.at === 'current') clauses.push(sql`retiredAt IS NULL`)
        else if (filter.at !== undefined)
          clauses.push(
            sql`createdAt <= ${filter.at} AND (retiredAt IS NULL OR retiredAt > ${filter.at})`,
          )
        const rows = yield* io(
          sql<{
            payload: string
          }>`SELECT payload FROM harness_records WHERE ${sql.and(clauses)} ORDER BY id ASC LIMIT ${limit}`,
        )
        return yield* Effect.forEach(rows, (row) => decode(row.payload))
      }),
      save: Effect.fn('SQLite.save')(function* (rows, next) {
        const encoded = yield* Effect.forEach(rows, (row) =>
          Schema.encodeEffect(RowFromString)(row).pipe(
            Effect.map((payload) => ({ row, payload })),
            Effect.mapError((cause) => rejected('Record cannot be encoded', undefined, cause)),
          ),
        )
        // Adapter failures after entering the transaction are conservatively uncertain.
        // Reopening reads the authoritative revision rather than attempting a blind retry.
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              for (const { row, payload } of encoded) {
                const index = records.indexOf(row)
                const column = (name: string): string | number | null => {
                  const value: unknown = Reflect.get(index, name)
                  if (typeof value === 'boolean') return Number(value)
                  if (typeof value === 'string' || typeof value === 'number') return value
                  return null
                }
                yield* sql`INSERT INTO harness_records (id, category, payload, conversationId, kind, ownerTaskId, ownerConversationId, status, background, abortRequested, requestId, address, scope, createdAt, retiredAt, type) VALUES (${records.idOf(row)}, ${row._tag}, ${payload}, ${column('conversationId')}, ${column('kind')}, ${column('ownerTaskId')}, ${column('ownerConversationId')}, ${column('status')}, ${column('background')}, ${column('abortRequested')}, ${column('requestId')}, ${column('address')}, ${column('scope')}, ${column('createdAt')}, ${column('retiredAt')}, ${column('type')}) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, conversationId=excluded.conversationId, kind=excluded.kind, ownerTaskId=excluded.ownerTaskId, ownerConversationId=excluded.ownerConversationId, status=excluded.status, background=excluded.background, abortRequested=excluded.abortRequested, requestId=excluded.requestId, address=excluded.address, scope=excluded.scope, createdAt=excluded.createdAt, retiredAt=excluded.retiredAt, type=excluded.type`
              }
              yield* sql`UPDATE harness_metadata SET revision = ${next.revision}, nextId = ${next.nextId} WHERE singleton = 1`
            }),
          )
          .pipe(Effect.mapError((cause) => uncertain('SQLite commit outcome is uncertain', cause)))
      }),
    })
  })
