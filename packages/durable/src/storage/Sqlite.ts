import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as SqlError from 'effect/sql/SqlError'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Record from '../Record.ts'
import { rejected, StorageError, uncertain, Corrupt, Io, Invalid } from '../StorageError.ts'
import { Store } from '../Store.ts'
import * as Backend from './Backend.ts'
import { validateState } from './State.ts'

export const MetadataRow = Schema.Struct({ name: Schema.String })
export const VersionRow = Schema.Struct({
  version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
})
export const StateRow = Schema.Struct({
  state: Schema.String,
  next_seq: Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/)),
  format: Schema.Literal(1),
})
export const JournalRow = Schema.Struct({ seq: Record.Seq, frame: Schema.String })
export const ReceiptRow = Schema.Struct({
  key: Schema.String,
  fingerprint: Schema.String,
  result: Schema.String,
  seq: Record.Seq,
  is_void: Schema.Literals([0, 1]),
})
const rows = <S extends Schema.Constraint>(schema: S, input: unknown) =>
  Schema.decodeUnknownEffect(Schema.Array(schema))(input).pipe(
    Effect.mapError((cause) => rejected('Invalid SQL driver rows', Corrupt, cause)),
  )
const decode = <S extends Schema.Constraint>(schema: S, input: unknown) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(input).pipe(
    Effect.mapError((cause) => rejected('Invalid persisted JSON', Corrupt, cause)),
  )
const encode = <S extends Schema.Constraint>(schema: S, input: S['Type']) =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(input).pipe(
    Effect.mapError((cause) => rejected('Cannot encode durable SQL data', Invalid, cause)),
  )
const encodeReceipt = Effect.fnUntraced(function* (receipt: Record.Receipt) {
  return {
    key: yield* encode(Schema.String, receipt.key),
    fingerprint: yield* encode(Schema.String, receipt.fingerprint),
    result: yield* encode(Schema.Json, receipt.result),
    seq: receipt.seq,
    is_void: receipt.resultIsVoid === true ? 1 : 0,
  }
})
export const CURRENT_SCHEMA_VERSION = 2
export interface Migration {
  readonly version: number
  readonly statements: ReadonlyArray<string>
}
export const MIGRATIONS: ReadonlyArray<Migration> = [
  {
    version: 1,
    statements: [
      'CREATE TABLE durable_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), format INTEGER NOT NULL CHECK(format=1), next_seq INTEGER NOT NULL CHECK(next_seq>0), state TEXT NOT NULL CHECK(json_valid(state))) STRICT',
      'CREATE TABLE durable_journal (seq INTEGER PRIMARY KEY CHECK(seq>0), frame TEXT NOT NULL CHECK(json_valid(frame))) STRICT',
      'CREATE TABLE durable_receipt (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL CHECK(json_valid(result)), seq INTEGER NOT NULL CHECK(seq>0)) STRICT',
    ],
  },
  {
    version: 2,
    statements: [
      'ALTER TABLE durable_receipt ADD COLUMN is_void INTEGER NOT NULL DEFAULT 0 CHECK(is_void IN (0,1))',
    ],
  },
]
export const migrate = Effect.fnUntraced(function* (
  migrations: ReadonlyArray<Migration> = MIGRATIONS,
) {
  const sql = yield* SqlClient.SqlClient
  if (migrations.some((migration, index) => migration.version !== index + 1))
    return yield* rejected('Migrations must have contiguous versions starting at 1')
  return yield* sql
    .withTransaction(
      Effect.gen(function* () {
        const existing = yield* rows(
          MetadataRow,
          yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name='durable_schema'`,
        )
        yield* sql.unsafe(
          'CREATE TABLE IF NOT EXISTS durable_schema(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version>=0)) STRICT',
        )
        if (existing.length === 0) yield* sql.unsafe('INSERT INTO durable_schema VALUES(1,0)')
        const metadata = yield* rows(
          VersionRow,
          yield* sql`SELECT version FROM durable_schema WHERE singleton=1`,
        )
        const version = metadata[0]?.version
        if (version === undefined) return yield* rejected('Schema metadata is missing', Corrupt)
        if (version > (migrations.at(-1)?.version ?? 0))
          return yield* rejected('Schema is newer than supported', Corrupt)
        for (const migration of migrations) {
          if (migration.version <= version) continue
          for (const statement of migration.statements) yield* sql.unsafe(statement)
          yield* sql`UPDATE durable_schema SET version=${migration.version} WHERE singleton=1`
          if (migration.version === 1 && migrations === MIGRATIONS)
            yield* sql`INSERT INTO durable_state VALUES(1,1,1,${yield* encode(Record.State, Record.emptyState())})`
        }
      }),
    )
    .pipe(
      Effect.mapError((cause) =>
        cause instanceof StorageError ? cause : rejected('Schema migration failed', Io, cause),
      ),
    )
})
export const make: Effect.Effect<
  Store['Service'],
  StorageError,
  Scope.Scope | SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  yield* migrate()
  const load = Effect.gen(function* () {
    const metadata = yield* rows(
      StateRow,
      yield* sql`SELECT state,CAST(next_seq AS TEXT) AS next_seq,format FROM durable_state WHERE singleton=1`,
    )
    const row = metadata[0]
    if (row === undefined) return yield* rejected('Durable metadata is missing', Corrupt)
    const state = yield* validateState(yield* decode(Record.State, row.state))
    if (row.format !== 1 || Number(row.next_seq) !== state.nextSeq)
      return yield* rejected('Durable metadata is corrupt', Corrupt)
    const journals = yield* rows(
      JournalRow,
      yield* sql`SELECT seq,frame FROM durable_journal ORDER BY seq`,
    )
    const frames: Array<Record.Frame> = []
    for (const journal of journals) {
      const frame = yield* decode(Record.Frame, journal.frame)
      if (frame.seq !== journal.seq || frame.seq >= state.nextSeq)
        return yield* rejected('Journal sequence is corrupt', Corrupt)
      frames.push(frame)
    }
    const receipts = yield* rows(
      ReceiptRow,
      yield* sql`SELECT key,fingerprint,result,seq,is_void FROM durable_receipt`,
    )
    const expected = yield* Effect.forEach(state.receipts, encodeReceipt)
    if (
      receipts.length !== state.receipts.length ||
      receipts.some(
        (row) =>
          !expected.some(
            (receipt) =>
              row.key === receipt.key &&
              row.fingerprint === receipt.fingerprint &&
              row.result === receipt.result &&
              row.seq === receipt.seq &&
              row.is_void === receipt.is_void,
          ),
      )
    )
      return yield* rejected('Receipt index differs from authoritative state', Corrupt)
    return { state, frames }
  }).pipe(
    sql.withTransaction,
    Effect.mapError((cause) =>
      cause instanceof StorageError ? cause : rejected('Cannot read durable storage', Io, cause),
    ),
  )
  // Removing the native dynamic transaction service makes this read lease the physical connection.
  // The client cannot lease it until an outer Activity transaction commits or rolls back.
  const committed = Effect.contextWith((context: Context.Context<never>) =>
    sql.withTransaction(load).pipe(
      Effect.provideContext(Context.omit(sql.transactionService)(context)),
      Effect.mapError((cause) =>
        cause instanceof StorageError
          ? cause
          : rejected('Cannot read committed SQL state', Io, cause),
      ),
    ),
  )
  const save = Effect.fnUntraced(
    function* (snapshot: Backend.Snapshot) {
      // Validate and encode the complete write set before the first driver mutation.
      const state = yield* encode(Record.State, snapshot.state)
      const frame = snapshot.frames.at(-1)
      const encodedFrame = frame === undefined ? undefined : yield* encode(Record.Frame, frame)
      const receipts = yield* Effect.forEach(snapshot.state.receipts, encodeReceipt)
      yield* sql`UPDATE durable_state SET next_seq=${snapshot.state.nextSeq},state=${state} WHERE singleton=1`
      if (frame !== undefined && encodedFrame !== undefined)
        yield* sql`INSERT INTO durable_journal(seq,frame) VALUES(${frame.seq},${encodedFrame})`
      yield* sql.unsafe(
        `DELETE FROM durable_journal WHERE seq NOT IN (${snapshot.frames.map(() => '?').join(',')})`,
        snapshot.frames.map((frame) => frame.seq),
      )
      for (const receipt of receipts)
        yield* sql`INSERT OR IGNORE INTO durable_receipt(key,fingerprint,result,seq,is_void) VALUES(${receipt.key},${receipt.fingerprint},${receipt.result},${receipt.seq},${receipt.is_void})`
    },
    Effect.mapError((cause) =>
      cause instanceof StorageError
        ? cause
        : rejected('Domain SQL write was rejected before transaction settlement', Io, cause),
    ),
  )
  yield* load
  const committedReadContext = {}
  return yield* Backend.make({
    load,
    readContext: Effect.serviceOption(sql.transactionService).pipe(
      Effect.map((lease) => (lease._tag === 'Some' ? lease.value : committedReadContext)),
    ),
    committed,
    save,
    atomic: (effect) =>
      sql
        .withTransaction(effect)
        .pipe(
          Effect.mapError((cause) =>
            cause instanceof SqlError.SqlError
              ? uncertain('SQL transaction settlement is uncertain', cause)
              : cause,
          ),
        ),
  })
})
export const layer = Layer.effect(Store, make)
