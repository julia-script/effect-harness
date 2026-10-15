/** Indexed SQL storage using only the supplied Effect SqlClient. */
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as SqlError from 'effect/sql/SqlError'
import * as SqlClient from 'effect/sql/SqlClient'
import type * as Statement from 'effect/sql/Statement'
import * as Errors from '../StorageError.js'
import type { Access } from './Kernel.js'
import * as Kernel from './Kernel.js'
import * as Metadata from './Metadata.js'
import * as Row from './Row.js'
import * as MediaRow from './MediaRow.js'
import * as Media from '../StorageMedia.js'

const metadataName = 'effect_harness_metadata'
const recordsName = 'effect_harness_records'
const revisionsName = 'effect_harness_revisions'
const objectsName = 'effect_harness_media'
const ownersName = 'effect_harness_media_owners'
const persistenceVersion = 3
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
  minimumReader: Schema.optionalKey(SqlCounter),
  nextId: SqlCounter.pipe(Schema.decodeTo(Metadata.Metadata.fields.nextId)),
  nextSeq: SqlCounter,
})).pipe(
  Schema.encodeKeys({ nextId: 'next_id', nextSeq: 'next_seq', minimumReader: 'minimum_reader' }),
)
const PayloadRows = Schema.Array(
  Schema.Struct({
    id: SqlCounter,
    category: Schema.String,
    payload: Schema.fromJsonString(MediaRow.PackedRow),
    revision_seq: Schema.NullOr(SqlCounter),
    revision_content: Schema.NullOr(Schema.fromJsonString(Media.PackedBodySchema)),
    owner_seq: Schema.NullOr(Schema.Union([Schema.Finite, Schema.String, Schema.BigInt])),
    digest: Schema.NullOr(Schema.String),
    bytes: Schema.NullOr(Schema.Uint8Array),
  }),
)
const RowJson = Schema.fromJsonString(MediaRow.PackedRow)
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
  const revisionsTable = sql(revisionsName)
  const objectsTable = sql(objectsName)
  const ownersTable = sql(ownersName)
  const text = sql.onDialectOrElse({
    mssql: () => sql.literal('NVARCHAR(MAX)'),
    orElse: () => sql.literal('TEXT'),
  })
  const tableExists = (name: string) =>
    io(
      'initialize',
      sql.onDialectOrElse({
        sqlite: () => sql`SELECT 1 FROM sqlite_master WHERE type='table' AND name=${name}`,
        mssql: () => sql`SELECT 1 FROM sys.tables WHERE name=${name}`,
        mysql: () =>
          sql`SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=${name}`,
        orElse: () =>
          sql`SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=${name}`,
      }),
    )
  const hasMetadata = yield* tableExists(metadataName)
  if (hasMetadata.length > 0) {
    // Probe the schema before DDL: opening an incompatible container must not migrate it.
    const marker =
      yield* sql`SELECT persistence_version, format, minimum_reader FROM ${metadataTable}`.pipe(
        Effect.mapError((cause) =>
          Errors.make(
            'unsupportedFormat',
            'initialize',
            'Unsupported SQL persistence schema',
            cause,
          ),
        ),
      )
    if (
      marker.some(
        (row) =>
          row.persistence_version !== persistenceVersion ||
          Number(row.format) !== 12 ||
          Number(row.minimum_reader) !== 12,
      )
    )
      return yield* Errors.make(
        'unsupportedFormat',
        'initialize',
        'Unsupported SQL persistence schema',
      )
  }
  if (hasMetadata.length === 0 && (yield* tableExists(recordsName)).length > 0) {
    const existing = yield* io('initialize', sql`SELECT COUNT(*) AS n FROM ${recordsTable}`)
    if (Number(existing[0]?.n) > 0)
      return yield* Errors.make('corrupt', 'initialize', 'SQL records have no allocation metadata')
  }
  const metadataDdl = sql`CREATE TABLE ${metadataTable} (
    singleton INTEGER PRIMARY KEY, format INTEGER NOT NULL, minimum_reader BIGINT,
    persistence_version INTEGER NOT NULL, next_id BIGINT NOT NULL, next_seq BIGINT NOT NULL
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
        singleton INTEGER PRIMARY KEY, format INTEGER NOT NULL, minimum_reader BIGINT, persistence_version INTEGER NOT NULL, next_id BIGINT NOT NULL, next_seq BIGINT NOT NULL
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
  const binary = sql.onDialectOrElse({
    pg: () => sql.literal('BYTEA'),
    mysql: () => sql.literal('LONGBLOB'),
    mssql: () => sql.literal('VARBINARY(MAX)'),
    orElse: () => sql.literal('BLOB'),
  })
  const mediaTables = [
    { name: objectsName, columns: sql`digest VARCHAR(66) PRIMARY KEY, bytes ${binary} NOT NULL` },
    {
      name: ownersName,
      columns: sql`record_id BIGINT NOT NULL, revision_seq BIGINT NOT NULL,
      digest VARCHAR(66) NOT NULL, PRIMARY KEY (record_id, revision_seq, digest),
      FOREIGN KEY (record_id) REFERENCES ${recordsTable}(id),
      FOREIGN KEY (digest) REFERENCES ${objectsTable}(digest)`,
    },
  ]
  for (const table of mediaTables)
    yield* io(
      'initialize',
      sql.onDialectOrElse({
        mssql: () =>
          Effect.gen(function* () {
            if (
              (yield* sql`SELECT 1 AS present FROM sys.tables WHERE name = ${table.name}`)
                .length === 0
            )
              yield* sql`CREATE TABLE ${sql(table.name)} (${table.columns})`
          }),
        orElse: () =>
          sql`CREATE TABLE IF NOT EXISTS ${sql(table.name)} (${table.columns})`.pipe(Effect.asVoid),
      }),
    )
  const existingMetadata = yield* io(
    'initialize',
    sql`SELECT singleton, persistence_version FROM ${metadataTable} WHERE singleton = 1`,
  )
  if (
    existingMetadata.length > 0 &&
    existingMetadata[0]?.persistence_version !== persistenceVersion
  )
    return yield* Errors.make(
      'unsupportedFormat',
      'initialize',
      'Unsupported SQL persistence schema',
    )
  const revisionsDdl = sql`CREATE TABLE ${revisionsTable} (
    document_id BIGINT NOT NULL,
    seq BIGINT NOT NULL CHECK (seq > 0), content ${text} NOT NULL,
    PRIMARY KEY (document_id, seq), FOREIGN KEY (document_id) REFERENCES ${recordsTable}(id)
  )`
  yield* io(
    'initialize',
    sql.onDialectOrElse({
      mssql: () =>
        Effect.gen(function* () {
          if (
            (yield* sql`SELECT 1 AS present FROM sys.tables WHERE name = ${revisionsName}`)
              .length === 0
          )
            yield* revisionsDdl
        }),
      orElse: () =>
        sql`CREATE TABLE IF NOT EXISTS ${revisionsTable} (
      document_id BIGINT NOT NULL,
      seq BIGINT NOT NULL CHECK (seq > 0), content ${text} NOT NULL,
      PRIMARY KEY (document_id, seq), FOREIGN KEY (document_id) REFERENCES ${recordsTable}(id)
    )`.pipe(Effect.asVoid),
    }),
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
        () => sql`INSERT INTO ${metadataTable} (singleton, format, minimum_reader, persistence_version, next_id, next_seq) VALUES (1, 12, 12, 3, 2, 1)
      ON DUPLICATE KEY UPDATE singleton = singleton`,
      mssql: () => sql`IF NOT EXISTS (SELECT 1 FROM ${metadataTable} WHERE singleton = 1)
      INSERT INTO ${metadataTable} (singleton, format, minimum_reader, persistence_version, next_id, next_seq) VALUES (1, 12, 12, 3, 2, 1)`,
      orElse:
        () => sql`INSERT INTO ${metadataTable} (singleton, format, minimum_reader, persistence_version, next_id, next_seq) VALUES (1, 12, 12, 3, 2, 1)
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
      sql.onDialectOrElse({
        // Node SQLite refuses numeric reads above MAX_SAFE_INTEGER. The valid
        // exhausted sentinel is decoded losslessly by SqlCounter from decimal text.
        sqlite: () => sql`SELECT format, minimum_reader, persistence_version,
          CAST(next_id AS TEXT) AS next_id, CAST(next_seq AS TEXT) AS next_seq
          FROM ${metadataTable} WHERE singleton = 1`,
        orElse: () => sql`SELECT format, minimum_reader, persistence_version, next_id, next_seq
          FROM ${metadataTable} WHERE singleton = 1`,
      }),
    )
    const row = rows[0]
    if (row !== undefined && row.persistence_version !== persistenceVersion)
      return yield* Errors.make(
        'unsupportedFormat',
        'metadata',
        'Unsupported SQL persistence schema',
      )
    // SQL NULL means the original format-1 marker, not an automatically upgraded reader floor.
    const raw =
      row === undefined
        ? row
        : Object.fromEntries(
            Object.entries(row).filter(
              ([key, value]) => key !== 'minimum_reader' || value !== null,
            ),
          )
    return yield* Schema.decodeUnknownEffect(SqlMetadata)(raw).pipe(
      Effect.mapError(Errors.corrupt('metadata')),
      Effect.flatMap(Metadata.requireSupported),
    )
  })
  yield* metadata
  const physical = Effect.fnUntraced(function* (
    query: Statement.Statement<unknown>,
    order: 'ascending' | 'descending' = 'ascending',
  ) {
    // Headers, revisions, ownership and bytes come from one statement snapshot.
    // UNION avoids multiplying image objects by the number of retained revisions.
    const direction = sql.literal(order === 'ascending' ? 'ASC' : 'DESC')
    const seq = sql.onDialectOrElse({
      sqlite: () => sql`CAST(v.seq AS TEXT)`,
      orElse: () => sql`v.seq`,
    })
    const snapshot = sql`WITH selected AS (${query})
      SELECT r.id, r.category, r.payload, ${seq} AS revision_seq, v.content AS revision_content,
        NULL AS owner_seq, NULL AS digest, NULL AS bytes
      FROM selected r LEFT JOIN ${revisionsTable} v ON v.document_id = r.id
      UNION ALL
      SELECT r.id, r.category, r.payload, NULL AS revision_seq, NULL AS revision_content,
        o.revision_seq AS owner_seq, o.digest, m.bytes
      FROM selected r JOIN ${ownersTable} o ON o.record_id = r.id
      LEFT JOIN ${objectsTable} m ON m.digest = o.digest
      ORDER BY id ${direction}, revision_seq ASC`
    const joined = yield* io(
      'read',
      sql.onDialectOrElse({
        // SQL Server's locking READ COMMITTED can release row locks within a statement.
        // Retain the shared metadata lock until the entire object closure has been copied.
        mssql: () =>
          sql.withTransaction(
            sql`SELECT singleton FROM ${metadataTable} WITH (HOLDLOCK) WHERE singleton = 1`.pipe(
              Effect.andThen(snapshot),
            ),
          ),
        orElse: () => snapshot,
      }),
    )
    const rows = yield* Schema.decodeUnknownEffect(PayloadRows)(joined).pipe(
      Effect.mapError(Errors.corrupt('record')),
    )
    const results = new Map<
      number,
      { packed: MediaRow.PackedRow; objects: Map<string, Uint8Array>; owners: Set<string> }
    >()
    for (const row of rows) {
      let result = results.get(row.id)
      if (result === undefined) {
        yield* MediaRow.validate(row.payload)
        const header = yield* MediaRow.skeleton(row.payload)
        if (
          Row.idOf(header) !== row.id ||
          header._tag !== row.category ||
          row.payload.revisions.length !== 0
        )
          return yield* Errors.make(
            'corrupt',
            'record',
            'SQL record identity or inline revisions are invalid',
          )
        result = { packed: row.payload, objects: new Map(), owners: new Set() }
        results.set(row.id, result)
      }
      if (row.revision_seq !== null) {
        if (row.category !== 'document' || row.revision_content === null)
          return yield* Errors.make('corrupt', 'record', 'Invalid document revision')
        yield* Media.manifest(row.revision_content)
        result.packed = {
          ...result.packed,
          revisions: [
            ...result.packed.revisions,
            { seq: row.revision_seq as import('../Record.js').Seq, content: row.revision_content },
          ],
        }
      }
      if (row.digest !== null) {
        if (row.bytes === null || row.owner_seq === null)
          return yield* Errors.make('corrupt', 'media.read', 'Required media object is absent')
        result.objects.set(row.digest, row.bytes)
        result.owners.add(`${String(row.owner_seq)}:${row.digest}`)
      }
    }
    for (const result of results.values()) {
      result.packed = {
        ...result.packed,
        revisions: [...result.packed.revisions].sort((a, b) => a.seq - b.seq),
      }
      const expected = new Set([
        ...result.packed.value.media.map((ref) => `0:${ref.digest}`),
        ...result.packed.revisions.flatMap((revision) =>
          revision.content.media.map((ref) => `${revision.seq}:${ref.digest}`),
        ),
      ])
      if (
        expected.size !== result.owners.size ||
        [...expected].some((key) => !result.owners.has(key))
      )
        return yield* Errors.make(
          'corrupt',
          'media.manifest',
          'SQL media owners disagree with retained manifests',
        )
      for (const ref of MediaRow.references(result.packed)) {
        const bytes = result.objects.get(ref.digest)
        if (bytes === undefined)
          return yield* Errors.make('corrupt', 'media.read', 'Required media object is absent')
        yield* Media.verify(ref, bytes)
      }
    }
    return [...results.values()]
  })
  const payloads = (
    query: Statement.Statement<unknown>,
    order: 'ascending' | 'descending' = 'ascending',
    metadataOnly = false,
  ) =>
    physical(query, order).pipe(
      Effect.flatMap((rows) =>
        Effect.forEach(rows, ({ packed, objects }) =>
          MediaRow.hydrate(
            packed,
            (digest) => {
              const bytes = objects.get(digest)
              return bytes === undefined
                ? Effect.fail(
                    Errors.make('corrupt', 'media.read', 'Required media object is absent'),
                  )
                : Effect.succeed(bytes)
            },
            metadataOnly,
          ),
        ),
      ),
    )
  const access: Access = {
    metadata,
    get: Effect.fnUntraced(function* (id, metadataOnly = false) {
      const rows = yield* payloads(
        sql`SELECT id, category, payload FROM ${recordsTable} WHERE id = ${id}`,
        'ascending',
        metadataOnly,
      )
      return Option.fromUndefinedOr(rows[0])
    }),
    page: Effect.fnUntraced(function* (kind, after, limit, filter, order, metadataOnly = false) {
      const results: Array<Row.Stored> = []
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
          order,
          metadataOnly,
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
        const id = Row.idOf(row)
        const old = (yield* physical(
          sql`SELECT id, category, payload FROM ${recordsTable} WHERE id = ${id}`,
        ))[0]
        const packed = yield* MediaRow.pack(row, old?.packed)
        for (const object of packed.objects) {
          const found = yield* io(
            'media.read',
            sql`SELECT bytes FROM ${objectsTable} WHERE digest = ${object.digest}`,
          )
          if (found.length > 0) {
            const bytes = yield* Schema.decodeUnknownEffect(Schema.Uint8Array)(
              found[0]?.bytes,
            ).pipe(Effect.mapError(Errors.corrupt('media.integrity')))
            if (!Media.sameObject(bytes, object.bytes))
              return yield* Errors.make('corrupt', 'media.integrity', 'Media hash collision')
          } else
            yield* io(
              'media.write',
              sql`INSERT INTO ${objectsTable} (digest, bytes) VALUES (${object.digest}, ${object.bytes})`,
            )
        }
        const payload = yield* Schema.encodeEffect(RowJson)({ ...packed.row, revisions: [] }).pipe(
          Effect.mapError(Errors.invalid('commit')),
        )
        const index = Row.indexOf(row)
        const values = {
          category: row._tag,
          payload,
          conversation_id: index.conversationId,
          kind: index.kind,
          owner_conversation_id: index.ownerConversationId,
          owner_task_id: index.ownerTaskId,
          status: index.status,
          background: index.background === null ? null : Number(index.background),
          abort_requested: index.abortRequested === null ? null : Number(index.abortRequested),
          request_id: index.requestId,
          address: index.address,
          scope_key: index.scope,
          created_at: index.createdAt,
          retired_at: index.retiredAt,
        }
        // The metadata lock serializes insert/update, revision pruning and allocation counters.
        const exists =
          (yield* io('read', sql`SELECT id FROM ${recordsTable} WHERE id = ${id}`)).length > 0
        if (exists)
          yield* io('write', sql`UPDATE ${recordsTable} SET ${sql.update(values)} WHERE id = ${id}`)
        else yield* io('write', sql`INSERT INTO ${recordsTable} ${sql.insert({ id, ...values })}`)
        yield* io('media.owners', sql`DELETE FROM ${ownersTable} WHERE record_id = ${id}`)
        for (const [seq, body] of [
          [0, packed.row.value],
          ...packed.row.revisions.map((revision) => [revision.seq, revision.content] as const),
        ] as const) {
          for (const digest of new Set(body.media.map((ref) => ref.digest)))
            yield* io(
              'media.owners',
              sql`INSERT INTO ${ownersTable} (record_id, revision_seq, digest) VALUES (${id}, ${seq}, ${digest})`,
            )
        }
        if (row._tag === 'document') {
          const first = row.value.revisions[0]
          if (first === undefined)
            yield* io('write', sql`DELETE FROM ${revisionsTable} WHERE document_id = ${id}`)
          else
            yield* io(
              'write',
              sql`DELETE FROM ${revisionsTable} WHERE document_id = ${id} AND seq < ${first.seq}`,
            )
          for (const revision of packed.row.revisions) {
            if (revision.seq !== next.nextSeq - 1) continue
            const content = yield* Schema.encodeEffect(
              Schema.fromJsonString(Media.PackedBodySchema),
            )(revision.content).pipe(Effect.mapError(Errors.invalid('commit')))
            yield* io(
              'write',
              sql`INSERT INTO ${revisionsTable} (document_id, seq, content)
              VALUES (${id}, ${revision.seq}, ${content})`,
            )
          }
        }
      }
      yield* io(
        'media.collect',
        sql`DELETE FROM ${objectsTable} WHERE NOT EXISTS (SELECT 1 FROM ${ownersTable} WHERE ${ownersTable}.digest = ${objectsTable}.digest)`,
      )
      yield* io(
        'write',
        sql`UPDATE ${metadataTable} SET format = ${counters.format}, minimum_reader = ${counters.minimum_reader ?? null}, next_id = ${counters.next_id}, next_seq = ${counters.next_seq} WHERE singleton = 1`,
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
        Effect.catchCause((cause) => {
          // SqlClient converts COMMIT/ROLLBACK failures to defects in its finalizer.
          if (
            !cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && SqlError.isSqlError(reason.defect),
            )
          )
            return Effect.failCause(
              Cause.map(cause, (error) => {
                if (error._tag === 'StorageError' && error.reason !== 'io') return error
                return Errors.make('uncertain', 'commit', 'SQL write outcome is uncertain', error)
              }),
            )
          const error = Errors.make('uncertain', 'commit', 'SQL write outcome is uncertain', cause)
          // Retain cancellation and unrelated defects, and keep the full SQL cause on the error.
          const remaining = Cause.fromReasons<never>(
            cause.reasons.filter(
              (reason): reason is Cause.Die | Cause.Interrupt =>
                Cause.isInterruptReason(reason) ||
                (Cause.isDieReason(reason) && !SqlError.isSqlError(reason.defect)),
            ),
          )
          return Effect.failCause(Cause.combine(Cause.fail(error), remaining))
        }),
      ),
  }
  return yield* Kernel.make(access)
})
