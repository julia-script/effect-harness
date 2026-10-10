import { assert, it } from '@effect/vitest'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
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
