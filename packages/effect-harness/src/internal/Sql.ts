/** Indexed SQL storage using only the supplied Effect SqlClient. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Errors from '../StorageError.js'
import type { Access } from './Kernel.js'
import * as Kernel from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'

const metadataName = 'effect_harness_metadata'
const recordsName = 'effect_harness_records'
// Drivers return BIGINT columns as numbers, decimal strings, or bigints.
const SqlBigIntCounter = Schema.BigInt.check(
  Schema.isBetweenBigInt({ minimum: 1n, maximum: BigInt(Number.MAX_SAFE_INTEGER) + 1n }),
).pipe(
  Schema.decodeTo(
    Metadata.Counter,
    SchemaTransformation.transform({ decode: Number, encode: BigInt }),
  ),
)
export const SqlCounter = Schema.Union([
  Metadata.Counter,
  Schema.String.check(Schema.isPattern(/^[0-9]+$/)).pipe(
    Schema.decodeTo(Schema.BigIntFromString),
    Schema.decodeTo(SqlBigIntCounter),
  ),
  SqlBigIntCounter,
])
export const SqlMetadata = Metadata.Metadata.mapFields(() => ({
  format: SqlCounter.pipe(Schema.decodeTo(Metadata.Metadata.fields.format)),
  nextId: SqlCounter.pipe(Schema.decodeTo(Metadata.Metadata.fields.nextId)),
  nextSeq: SqlCounter,
})).pipe(Schema.encodeKeys({ nextId: 'next_id', nextSeq: 'next_seq' }))
const PayloadRows = Schema.Array(
  Schema.Struct({
    id: SqlCounter,
    category: Schema.String,
    payload: Schema.fromJsonString(Row.Row),
  }).check(
    Schema.makeFilter(
      (row) => Row.idOf(row.payload) === row.id && row.payload._tag === row.category,
    ),
  ),
)
const RowJson = Schema.fromJsonString(Row.Row)
const io = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError((cause) => Errors.make('io', operation, `SQL ${operation} failed`, cause)),
  )

export const make = Effect.gen(function* () {
  const sql = (yield* SqlClient.SqlClient).withoutTransforms()
  // A savepoint cannot settle durability before its owning transaction commits.
  const independent = (operation: string) =>
    Effect.serviceOption(sql.transactionService).pipe(
      Effect.flatMap((transaction) =>
        Option.isSome(transaction)
          ? Effect.fail(
              Errors.make(
                'invalid',
                operation,
                'SQL storage cannot initialize or write inside the supplied client transaction',
              ),
            )
          : Effect.void,
      ),
    )
  yield* independent('initialize')
  const metadataTable = sql(metadataName)
  const recordsTable = sql(recordsName)
  const text = sql.onDialectOrElse({
    mssql: () => sql.literal('NVARCHAR(MAX)'),
    orElse: () => sql.literal('TEXT'),
  })
  const metadataDdl = sql`CREATE TABLE ${metadataTable} (
    singleton INTEGER PRIMARY KEY, format INTEGER NOT NULL,
    next_id BIGINT NOT NULL, next_seq BIGINT NOT NULL
  )`
  const recordsDdl = sql`CREATE TABLE ${recordsTable} (
    id BIGINT PRIMARY KEY, category VARCHAR(32) NOT NULL, payload ${text} NOT NULL,
    conversation_id BIGINT, kind ${text}, owner_conversation_id BIGINT, owner_task_id BIGINT,
    status VARCHAR(32), background INTEGER, abort_requested INTEGER, request_id ${text},
    address ${text}, scope_key ${text}, created_at BIGINT, retired_at BIGINT
  )`
  yield* io(
    'initialize',
    sql.onDialectOrElse({
      mssql: () =>
        Effect.gen(function* () {
          if (
            (yield* sql`SELECT 1 AS present FROM sys.tables WHERE name = ${metadataName}`)
              .length === 0
          )
            yield* metadataDdl
          if (
            (yield* sql`SELECT 1 AS present FROM sys.tables WHERE name = ${recordsName}`).length ===
            0
          )
            yield* recordsDdl
        }),
      orElse: () =>
        Effect.gen(function* () {
          yield* sql`CREATE TABLE IF NOT EXISTS ${metadataTable} (
        singleton INTEGER PRIMARY KEY, format INTEGER NOT NULL, next_id BIGINT NOT NULL, next_seq BIGINT NOT NULL
      )`
          yield* sql`CREATE TABLE IF NOT EXISTS ${recordsTable} (
        id BIGINT PRIMARY KEY, category VARCHAR(32) NOT NULL, payload ${text} NOT NULL,
        conversation_id BIGINT, kind ${text}, owner_conversation_id BIGINT, owner_task_id BIGINT,
        status VARCHAR(32), background INTEGER, abort_requested INTEGER, request_id ${text},
        address ${text}, scope_key ${text}, created_at BIGINT, retired_at BIGINT
      )`
        }),
    }),
  )
  const existingMetadata = yield* io(
    'initialize',
    sql`SELECT singleton FROM ${metadataTable} WHERE singleton = 1`,
  )
  if (
    existingMetadata.length === 0 &&
    (yield* io('initialize', sql`SELECT id FROM ${recordsTable}`)).length > 0
  )
    return yield* Errors.make('corrupt', 'initialize', 'SQL records have no allocation metadata')
  yield* io(
    'initialize',
    sql.onDialectOrElse({
      mysql:
        () => sql`INSERT INTO ${metadataTable} (singleton, format, next_id, next_seq) VALUES (1, 1, 2, 1)
      ON DUPLICATE KEY UPDATE singleton = singleton`,
      mssql: () => sql`IF NOT EXISTS (SELECT 1 FROM ${metadataTable} WHERE singleton = 1)
      INSERT INTO ${metadataTable} (singleton, format, next_id, next_seq) VALUES (1, 1, 2, 1)`,
      orElse:
        () => sql`INSERT INTO ${metadataTable} (singleton, format, next_id, next_seq) VALUES (1, 1, 2, 1)
      ON CONFLICT (singleton) DO NOTHING`,
    }),
  )
  for (const [name, columns] of [
    ['effect_harness_category', ['category', 'id']],
    ['effect_harness_conversation', ['category', 'conversation_id', 'id']],
    ['effect_harness_status', ['category', 'status', 'id']],
  ] as const) {
    const ddl = sql`CREATE INDEX ${sql(name)} ON ${recordsTable} (${sql.csv(columns.map((column) => sql`${sql(column)}`))})`
    yield* io(
      'initialize',
      sql.onDialectOrElse({
        mysql: () =>
          Effect.gen(function* () {
            if (
              (yield* sql`SELECT 1 AS present FROM information_schema.statistics
          WHERE table_schema = DATABASE() AND table_name = ${recordsName} AND index_name = ${name}`)
                .length === 0
            )
              yield* ddl
          }),
        mssql: () =>
          Effect.gen(function* () {
            if (
              (yield* sql`SELECT 1 AS present FROM sys.indexes
          WHERE object_id = OBJECT_ID(${recordsName}) AND name = ${name}`).length === 0
            )
              yield* ddl
          }),
        orElse: () =>
          sql`CREATE INDEX IF NOT EXISTS ${sql(name)} ON ${recordsTable} (${sql.csv(columns.map((column) => sql`${sql(column)}`))})`.pipe(
            Effect.asVoid,
          ),
      }),
    )
  }
  const metadata = Effect.gen(function* () {
    const rows = yield* io(
      'metadata',
      sql`SELECT format, next_id, next_seq FROM ${metadataTable} WHERE singleton = 1`,
    )
    return yield* Schema.decodeUnknownEffect(SqlMetadata)(rows[0]).pipe(
      Effect.mapError(Errors.corrupt('metadata')),
    )
  })
  yield* metadata
  const payloads = Effect.fnUntraced(function* (
    query: Effect.Effect<ReadonlyArray<unknown>, import('effect/sql/SqlError').SqlError>,
  ) {
    const rows = yield* Schema.decodeUnknownEffect(PayloadRows)(yield* io('read', query)).pipe(
      Effect.mapError(Errors.corrupt('record')),
    )
    return rows.map((row) => row.payload)
  })
  const access: Access = {
    metadata,
    get: Effect.fnUntraced(function* (id) {
      const rows = yield* payloads(
        sql`SELECT id, category, payload FROM ${recordsTable} WHERE id = ${id}`,
      )
      return Option.fromUndefinedOr(rows[0])
    }),
    page: Effect.fnUntraced(function* (kind, after, limit, filter, order) {
      const results: Array<Row.Row> = []
      let cursor = after
      while (results.length < limit) {
        const clauses = [sql`category = ${kind}`]
        if (cursor !== undefined)
          clauses.push(order === 'ascending' ? sql`id > ${cursor}` : sql`id < ${cursor}`)
        if (filter.minId !== undefined) clauses.push(sql`id >= ${filter.minId}`)
        if (filter.maxId !== undefined) clauses.push(sql`id <= ${filter.maxId}`)
        const indexFields = {
          conversationId: 'conversation_id',
          kind: 'kind',
          ownerConversationId: 'owner_conversation_id',
          ownerTaskId: 'owner_task_id',
          status: 'status',
          background: 'background',
          abortRequested: 'abort_requested',
          requestId: 'request_id',
          address: 'address',
          scope: 'scope_key',
        }
        for (const key of [
          'conversationId',
          'kind',
          'ownerConversationId',
          'ownerTaskId',
          'status',
          'background',
          'abortRequested',
          'requestId',
          'address',
          'scope',
        ] as const) {
          const value = filter[key]
          if (value !== undefined)
            clauses.push(
              sql`${sql(indexFields[key])} = ${typeof value === 'boolean' ? Number(value) : value}`,
            )
        }
        if (filter.at === 'current') clauses.push(sql`retired_at IS NULL`)
        else if (filter.at !== undefined)
          clauses.push(
            sql`created_at <= ${filter.at} AND (retired_at IS NULL OR retired_at > ${filter.at})`,
          )
        const direction = sql.literal(order === 'ascending' ? 'ASC' : 'DESC')
        const rows = yield* payloads(
          sql.onDialectOrElse({
            mssql: () =>
              sql`SELECT TOP (64) id, category, payload FROM ${recordsTable} WHERE ${sql.and(clauses)} ORDER BY id ${direction}`,
            orElse: () =>
              sql`SELECT id, category, payload FROM ${recordsTable} WHERE ${sql.and(clauses)} ORDER BY id ${direction} LIMIT 64`,
          }),
        )
        // SQL collations can match extra strings; enforce the same exact comparison as memory.
        results.push(...rows.filter((row) => Row.matches(row, filter)))
        const last = rows.at(-1)
        if (rows.length < 64 || last === undefined) break
        cursor = Row.idOf(last)
      }
      return results.slice(0, limit)
    }),
    save: Effect.fnUntraced(function* (rows, next) {
      const counters = yield* Schema.encodeEffect(SqlMetadata)(next).pipe(
        Effect.mapError(Errors.invalid('commit')),
      )
      for (const row of rows) {
        const payload = yield* Schema.encodeEffect(RowJson)(row).pipe(
          Effect.mapError((cause) =>
            Errors.make('invalid', 'commit', 'Cannot encode SQL record', cause),
          ),
        )
        const index = Row.indexOf(row)
        // The metadata row serializes writers, so delete/insert is an atomic portable upsert.
        yield* io('write', sql`DELETE FROM ${recordsTable} WHERE id = ${Row.idOf(row)}`)
        yield* io(
          'write',
          sql`INSERT INTO ${recordsTable}
          (id, category, payload, conversation_id, kind, owner_conversation_id, owner_task_id,
            status, background, abort_requested, request_id, address, scope_key, created_at, retired_at)
          VALUES (${Row.idOf(row)}, ${row._tag}, ${payload}, ${index.conversationId}, ${index.kind},
            ${index.ownerConversationId}, ${index.ownerTaskId}, ${index.status},
            ${index.background === null ? null : Number(index.background)},
            ${index.abortRequested === null ? null : Number(index.abortRequested)}, ${index.requestId},
            ${index.address}, ${index.scope}, ${index.createdAt}, ${index.retiredAt})`,
        )
      }
      yield* io(
        'write',
        sql`UPDATE ${metadataTable} SET next_id = ${counters.next_id}, next_seq = ${counters.next_seq} WHERE singleton = 1`,
      )
    }),
    exclusive: <A>(effect: Effect.Effect<A, Errors.StorageError>) =>
      independent('commit').pipe(
        Effect.andThen(
          sql.withTransaction(
            io('lock', sql`UPDATE ${metadataTable} SET next_id = next_id WHERE singleton = 1`).pipe(
              Effect.andThen(effect),
            ),
          ),
        ),
        Effect.mapError((cause) => {
          if (cause._tag === 'StorageError' && cause.reason !== 'io') return cause
          return Errors.make('uncertain', 'commit', 'SQL write outcome is uncertain', cause)
        }),
      ),
  }
  return yield* Kernel.make(access)
})
