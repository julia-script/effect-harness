import { assert, it } from '@effect/vitest'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Reactivity from 'effect/reactivity/Reactivity'
import * as Statement from 'effect/sql/Statement'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID

it.live('rejects SQL storage initialization in an ambient client transaction before DDL', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const error = yield* sql.withTransaction(Layer.build(Storage.layerSql)).pipe(Effect.flip)
      assert.strictEqual(error._tag, 'StorageError')
      if (error._tag === 'StorageError') {
        assert.strictEqual(error.reason, 'invalid')
        assert.strictEqual(error.operation, 'initialize')
      }
      const tables = yield* sql`SELECT name FROM sqlite_master WHERE name LIKE 'effect_harness_%'`
      assert.lengthOf(tables, 0)
      const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
      assert.strictEqual(yield* storage.commit([{ _tag: 'conversation', value: { id: root } }]), 1)
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

it.live('rejects SQL storage commit and ID allocation in an ambient client transaction', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const commit = yield* storage
              .commit([{ _tag: 'conversation', value: { id: root } }])
              .pipe(Effect.flip)
            const allocation = yield* storage.mintId().pipe(Effect.flip)
            assert.strictEqual(commit.reason, 'invalid')
            assert.strictEqual(allocation.reason, 'invalid')
            return yield* Effect.fail('application rollback')
          }),
        )
        .pipe(Effect.flip)
      assert.isTrue(Option.isNone(yield* storage.conversation(root)))
      assert.strictEqual(yield* storage.mintId(), 2)
      assert.strictEqual(yield* storage.commit([{ _tag: 'conversation', value: { id: root } }]), 1)
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

it.live('does not adopt a Session write rejected by an outer SQL transaction and can retry', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
      const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, storage))
      const counter = Document.define({
        kind: 'counter',
        scope: 'conversation',
        version: 1,
        schema: Schema.Struct({ count: Schema.Natural }),
        initial: () => ({ count: 0 }),
        history: 'rewindable',
        fork: 'asOf',
      })
      const target = { scope: { _tag: 'conversation' as const, conversationId: root } }
      yield* Session.commit(session, (tx) =>
        Effect.gen(function* () {
          yield* Transaction.ensureRoot(tx)
          yield* Transaction.ensureDocument(tx, counter, target)
        }),
      )
      const documentId = Option.getOrThrow(yield* Session.snapshot(session, counter, target)).record
        .id
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const rejected = yield* Session.commit(session, (tx) =>
              Transaction.setDocument(tx, counter, target, { count: 42 }),
            ).pipe(Effect.flip)
            assert.strictEqual(rejected._tag, 'StorageError')
            if (rejected._tag === 'StorageError') assert.strictEqual(rejected.reason, 'invalid')
            return yield* Effect.fail('application rollback')
          }),
        )
        .pipe(Effect.flip)
      assert.deepEqual(Option.getOrThrow(yield* Session.snapshot(session, counter, target)).value, {
        count: 0,
      })
      assert.deepEqual(Option.getOrThrow(yield* storage.document(documentId)).value, { count: 0 })
      yield* Session.commit(session, (tx) =>
        Transaction.setDocument(tx, counter, target, { count: 42 }),
      )
      assert.deepEqual(Option.getOrThrow(yield* Session.snapshot(session, counter, target)).value, {
        count: 42,
      })
      assert.deepEqual(Option.getOrThrow(yield* storage.document(documentId)).value, { count: 42 })
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

for (const control of ['COMMIT', 'ROLLBACK', 'ROLLBACK after conflict'] as const)
  it.live(`normalizes a failed SQL ${control} and requires direct Storage to reopen`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const native = yield* SqlClient.SqlClient
        // Configure the real client transaction wrapper to issue a failing rollback.
        // The retained SQLite connection is exclusively reserved for this test scope.
        const sql =
          control !== 'COMMIT'
            ? yield* SqlClient.make({
                acquirer: Effect.succeed(yield* native.reserve),
                compiler: Statement.makeCompilerSqlite(),
                rollback: 'ROLLBACK BROKEN',
                spanAttributes: [],
              }).pipe(Effect.provide(Reactivity.layer))
            : native
        const open = Layer.build(Storage.layerSql).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        )
        const storage = Context.get(yield* open, Storage.Storage)
        yield* storage.commit([{ _tag: 'conversation', value: { id: root } }])
        if (control === 'COMMIT') {
          yield* sql`PRAGMA foreign_keys = ON`
          yield* sql`CREATE TABLE parent(id INTEGER PRIMARY KEY)`
          yield* sql`CREATE TABLE child(id INTEGER, FOREIGN KEY(id) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`
          yield* sql`CREATE TRIGGER fail_transaction AFTER INSERT ON effect_harness_records WHEN NEW.id = 2 BEGIN INSERT INTO child(id) VALUES(999); END`
        } else if (control === 'ROLLBACK') {
          yield* sql`CREATE TRIGGER fail_transaction AFTER INSERT ON effect_harness_records WHEN NEW.id = 2 BEGIN SELECT RAISE(ABORT, 'write fails before rollback'); END`
        }
        const entryWrite: Storage.StorageWrite = {
          _tag: 'entry',
          value: { id: Record.EntryId.make(2), conversationId: root, kind: 'user', data: {} },
        }
        const write: Storage.StorageWrite =
          control === 'ROLLBACK after conflict'
            ? { _tag: 'conversation', value: { id: root } }
            : entryWrite
        const failed = yield* storage.commit([write]).pipe(Effect.exit)
        assert.strictEqual(failed._tag, 'Failure')
        if (failed._tag === 'Failure') {
          assert.isFalse(Cause.hasDies(failed.cause))
          const error = Option.getOrThrow(Cause.findErrorOption(failed.cause))
          assert.strictEqual(error.reason, 'uncertain')
          assert.strictEqual(error.operation, 'commit')
          assert.isTrue(Cause.isCause(error.cause))
          if (Cause.isCause(error.cause)) assert.isTrue(Cause.hasDies(error.cause))
        }
        if (control !== 'COMMIT') yield* sql`ROLLBACK`
        if (control !== 'ROLLBACK after conflict') yield* sql`DROP TRIGGER fail_transaction`
        const subsequent = yield* storage.commit([write]).pipe(Effect.flip)
        assert.strictEqual(subsequent.reason, 'uncertain')
        assert.strictEqual(subsequent.operation, 'access')
        assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
        assert.strictEqual(
          (yield* storage.entry(Record.EntryId.make(2)).pipe(Effect.flip)).reason,
          'uncertain',
        )
        // Reopen on the same healthy SQL connection after reconciling the transaction.
        const reopened = Context.get(yield* open, Storage.Storage)
        assert.isTrue(Option.isNone(yield* reopened.entry(Record.EntryId.make(2))))
        assert.strictEqual(yield* reopened.commit([entryWrite]), 2)
        assert.isTrue(Option.isSome(yield* reopened.entry(Record.EntryId.make(2))))
      }),
    ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

it.live('keeps Session sealed after a normalized SQL COMMIT error', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
      const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, storage))
      const counter = Document.define({
        kind: 'counter',
        scope: 'conversation',
        version: 1,
        schema: Schema.Struct({ count: Schema.Natural }),
        initial: () => ({ count: 0 }),
        history: 'rewindable',
        fork: 'asOf',
      })
      const target = { scope: { _tag: 'conversation' as const, conversationId: root } }
      yield* Session.commit(session, (tx) =>
        Effect.gen(function* () {
          yield* Transaction.ensureRoot(tx)
          yield* Transaction.ensureDocument(tx, counter, target)
        }),
      )
      const documentId = Option.getOrThrow(yield* Session.snapshot(session, counter, target)).record
        .id
      yield* sql`PRAGMA foreign_keys = ON`
      yield* sql`CREATE TABLE parent(id INTEGER PRIMARY KEY)`
      yield* sql`CREATE TABLE child(id INTEGER, FOREIGN KEY(id) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`
      yield* sql`CREATE TRIGGER fail_transaction AFTER INSERT ON effect_harness_records BEGIN INSERT INTO child(id) VALUES(999); END`
      const error = yield* Session.commit(session, (tx) =>
        Transaction.setDocument(tx, counter, target, { count: 42 }),
      ).pipe(Effect.flip)
      assert.strictEqual(error._tag, 'StorageError')
      if (error._tag === 'StorageError') assert.strictEqual(error.reason, 'uncertain')
      yield* sql`DROP TRIGGER fail_transaction`
      assert.deepEqual(yield* Session.snapshot(session, counter, target).pipe(Effect.flip), error)
      assert.deepEqual(
        yield* Session.commit(session, (tx) =>
          Transaction.setDocument(tx, counter, target, { count: 43 }),
        ).pipe(Effect.flip),
        error,
      )
      const reopened = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
      assert.deepEqual(Option.getOrThrow(yield* reopened.document(documentId)).value, { count: 0 })
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)
