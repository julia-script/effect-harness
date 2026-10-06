import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Document from '../../src/Document.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import { Store } from '../../src/Store.ts'
import { rejected, StorageError } from '../../src/StorageError.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Sqlite from '../../src/storage/Sqlite.ts'
import { sessionLayer } from '../../src/testing/Storage.ts'
const token = Document.define({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite.pipe(Schema.mutableKey) }),
  initial: () => ({ count: 0 }),
})
const initialize = Effect.gen(function* () {
  const session = yield* Session.Session
  yield* session.transaction((tx) => tx.doc(token).pipe(Effect.as(null)))
  return session
})
const update = (session: Session.Service, count: number) =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const d = yield* tx.doc(token)
      d.count = count
      return null
    }),
  )
const sqliteClient = SqliteClient.layer({ filename: ':memory:' })
const sqlLayers = sessionLayer(Sqlite.layer).pipe(Layer.provideMerge(sqliteClient))
describe('committed observations', () => {
  it.effect('delivers exact ordinary structural noops and skips unrelated commits', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* initialize
        const watch = yield* session.watchDoc(token)
        assert.ok(watch)
        yield* session.root()
        yield* update(session, 0)
        yield* update(session, 2)
        const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(2)))
        assert.deepStrictEqual(
          changes.map((change) => change.ops),
          [[['set', ['count'], 0]], [['set', ['count'], 2]]],
        )
        assert.deepStrictEqual(
          changes.map((change) => change.reset),
          [false, false],
        )
        assert.strictEqual(watch.value?.count, 2)
        yield* watch.stop
        yield* watch.stop
        assert.strictEqual(yield* watch.closed, 'stopped')
        assert.ok(yield* watch.changes.pipe(Stream.runCollect, Effect.flip))
      }).pipe(Effect.provide(sessionLayer(Memory.layer))),
    ),
  )
  it.effect('collapses more than 100 pending commits to one root replacement', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* initialize
        const watch = yield* session.watchDoc(token)
        assert.ok(watch)
        for (let count = 1; count <= 101; count++) yield* update(session, count)
        const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(1)))
        assert.strictEqual(changes.length, 1)
        assert.deepStrictEqual(changes[0]?.ops, [['replace', { count: 101 }]])
        assert.strictEqual(changes[0]?.reset, true)
      }).pipe(Effect.provide(sessionLayer(Memory.layer))),
    ),
  )
  it.effect('delivers retirement once and never follows a replacement incarnation', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* initialize
        const watch = yield* session.watchDoc(token)
        assert.ok(watch)
        yield* session.transaction((tx) => tx.retire(token).pipe(Effect.as(null)))
        yield* update(session, 9)
        const changes = yield* Stream.runCollect(watch.changes)
        assert.deepStrictEqual(
          changes.map((change) => change.value),
          [null],
        )
        assert.strictEqual(yield* watch.closed, 'retired')
      }).pipe(Effect.provide(sessionLayer(Memory.layer))),
    ),
  )
  it.live('keeps live values current without consuming and closes on session shutdown', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* initialize
        const watch = yield* session.state(token)
        assert.ok(watch)
        yield* update(session, 7)
        yield* Effect.sleep('50 millis')
        assert.strictEqual(watch.value?.count, 7)
        yield* session.close
        assert.strictEqual(yield* watch.closed, 'session_closed')
      }).pipe(Effect.provide(sessionLayer(Memory.layer))),
    ),
  )
  it.effect(
    "does not collapse a document's backlog because unrelated commits filled the global tail",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* initialize
          const watch = yield* session.watchDoc(token)
          assert.ok(watch)
          yield* update(session, 1)
          yield* session.root()
          for (let index = 0; index < 110; index++)
            yield* session.transaction((tx) =>
              tx
                .appendEntry(Record.ROOT_CONVERSATION_ID, { kind: 'unrelated' })
                .pipe(Effect.asVoid),
            )
          const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(1)))
          assert.deepStrictEqual(changes[0]?.ops, [['set', ['count'], 1]])
          assert.strictEqual(changes[0]?.reset, false)
        }).pipe(Effect.provide(sessionLayer(Memory.layer))),
      ),
  )
  it.live(
    'collapses pending-only frames while retaining an in-flight callback and classifies interruption',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* initialize
          const watch = yield* session.watchDoc(token)
          assert.ok(watch)
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const received: Array<number | undefined> = []
          const listening = yield* watch
            .listen((change) =>
              Effect.gen(function* () {
                received.push(change.value?.count)
                if (received.length === 1) {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }
              }),
            )
            .pipe(Effect.forkScoped)
          yield* update(session, 1)
          yield* Deferred.await(entered)
          for (let count = 2; count <= 102; count++) yield* update(session, count)
          assert.strictEqual(watch.value?.count, 1)
          yield* Deferred.succeed(release, undefined)
          yield* Effect.sleep('40 millis')
          assert.deepStrictEqual(received, [1, 102])
          yield* Fiber.interrupt(listening)
          assert.strictEqual(yield* watch.closed, 'cancelled')
        }).pipe(Effect.provide(sessionLayer(Memory.layer))),
      ),
  )
  it.live('does not publish nested writes before outer physical commit', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* initialize
        const store = yield* Store
        const sql = yield* SqlClient.SqlClient
        const watch = yield* session.watchDoc(token)
        assert.ok(watch)
        const staged = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const writer = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* update(session, 5)
              yield* Deferred.succeed(staged, undefined)
              yield* Deferred.await(release)
            }),
          )
          .pipe(Effect.forkScoped)
        yield* Deferred.await(staged)
        let published = false
        const reader = yield* store.journal(0).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              published = true
            }),
          ),
          Effect.forkScoped,
        )
        yield* Effect.sleep('40 millis')
        assert.strictEqual(published, false)
        assert.strictEqual(watch.value?.count, 0)
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(writer)
        yield* Fiber.join(reader)
        const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(1)))
        assert.strictEqual(changes[0]?.value?.count, 5)
        assert.strictEqual((yield* store.committed).receipts.length, 0)
      }).pipe(Effect.provide(sqlLayers)),
    ),
  )
  it.live('outer rollback leaves no receipt, state cache or observer publication', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* initialize
        const store = yield* Store
        const sql = yield* SqlClient.SqlClient
        const watch = yield* session.watchDoc(token)
        assert.ok(watch)
        const result = yield* sql
          .withTransaction(
            session
              .transaction(
                Effect.fnUntraced(function* (tx) {
                  const d = yield* tx.doc(token)
                  d.count = 9
                  return 9
                }),
                { key: 'receipt' },
              )
              .pipe(Effect.andThen(rejected('rollback'))),
          )
          .pipe(Effect.flip)
        assert.ok(result instanceof StorageError)
        assert.strictEqual(result.certainty, 'rejected')
        assert.strictEqual((yield* session.snapshot(token))?.value.count, 0)
        assert.strictEqual((yield* store.committed).receipts.length, 0)
        yield* Effect.sleep('40 millis')
        assert.strictEqual(watch.value?.count, 0)
        const seq = (yield* store.read).nextSeq - 1
        assert.deepStrictEqual((yield* store.journal(seq as Record.Seq)).frames, [])
      }).pipe(Effect.provide(sqlLayers)),
    ),
  )
})
