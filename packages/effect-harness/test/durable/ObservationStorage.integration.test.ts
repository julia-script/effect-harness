import * as Clock from 'effect/Clock'
import * as ReadAdmissionFixture from './storage/ReadAdmissionFixture.ts'
import * as TestClock from 'effect/testing/TestClock'
import * as Option from 'effect/Option'
import { ResourceScope } from 'effect-harness/durable/testing/Storage'
import { withLayer } from './StorageFixture.ts'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Document from 'effect-harness/durable/Document'
import * as Record from 'effect-harness/durable/Record'
import * as Session from 'effect-harness/durable/Session'
import { Store } from 'effect-harness/durable/Store'
import { rejected, StorageError } from 'effect-harness/durable/StorageError'
import * as StoreModule from 'effect-harness/durable/Store'
import { sessionLayer } from 'effect-harness/durable/testing/Storage'
const token = Document.defineUnsafe({
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
const update = (session: Session.Session.Service, count: number) =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const d = yield* tx.doc(token)
      d.count = count
      return null
    }),
  )
const sqliteClient = SqliteClient.layer({ filename: ':memory:' })
const sqlLayers = sessionLayer(ReadAdmissionFixture.layer).pipe(
  Layer.provideMerge(Layer.merge(sqliteClient, ReadAdmissionFixture.controls)),
)
describe('ObservationStorage', () => {
  it.effect('delivers exact ordinary structural noops and skips unrelated commits', () =>
    Effect.gen(function* () {
      const session = yield* initialize
      const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
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
    }).pipe(Effect.provide(sessionLayer(StoreModule.layerMemory))),
  )
  it.effect('collapses more than 100 pending commits to one root replacement', () =>
    Effect.gen(function* () {
      const session = yield* initialize
      const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
      assert.ok(watch)
      for (let count = 1; count <= 101; count++) yield* update(session, count)
      const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(1)))
      assert.strictEqual(changes.length, 1)
      assert.deepStrictEqual(changes[0]?.ops, [['replace', { count: 101 }]])
      assert.strictEqual(changes[0]?.reset, true)
    }).pipe(Effect.provide(sessionLayer(StoreModule.layerMemory))),
  )
  it.effect('delivers retirement once and never follows a replacement incarnation', () =>
    Effect.gen(function* () {
      const session = yield* initialize
      const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
      assert.ok(watch)
      yield* session.transaction((tx) => tx.retire(token).pipe(Effect.as(null)))
      yield* update(session, 9)
      const changes = yield* Stream.runCollect(watch.changes)
      assert.deepStrictEqual(
        changes.map((change) => change.value),
        [null],
      )
      assert.strictEqual(yield* watch.closed, 'retired')
    }).pipe(Effect.provide(sessionLayer(StoreModule.layerMemory))),
  )
  it.effect('keeps live values current without consuming and closes on session shutdown', () =>
    Effect.gen(function* () {
      const session = yield* initialize
      const watch = yield* session.state(token).pipe(Effect.map(Option.getOrUndefined))
      assert.ok(watch)
      yield* update(session, 7)
      yield* TestClock.adjust('50 millis')
      assert.strictEqual(watch.value?.count, 7)
      yield* Scope.close(yield* ResourceScope, Exit.void)
      yield* TestClock.adjust('50 millis')
      assert.strictEqual(yield* watch.closed, 'session_closed')
    }).pipe((effect) => withLayer(effect, sessionLayer(StoreModule.layerMemory))),
  )
  it.effect(
    "does not collapse a document's backlog because unrelated commits filled the global tail",
    () =>
      Effect.gen(function* () {
        const session = yield* initialize
        const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
        assert.ok(watch)
        yield* update(session, 1)
        yield* session.root()
        for (let index = 0; index < 110; index++)
          yield* session.transaction((tx) =>
            tx.appendEntry(Record.ROOT_CONVERSATION_ID, { kind: 'unrelated' }).pipe(Effect.asVoid),
          )
        const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(1)))
        assert.deepStrictEqual(changes[0]?.ops, [['set', ['count'], 1]])
        assert.strictEqual(changes[0]?.reset, false)
      }).pipe(Effect.provide(sessionLayer(StoreModule.layerMemory))),
  )
  it.effect(
    'collapses pending-only frames while retaining an in-flight callback and classifies interruption',
    () =>
      Effect.gen(function* () {
        const session = yield* initialize
        const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
        assert.ok(watch)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const collapsed = yield* Deferred.make<void>()
        const received: Array<number | undefined> = []
        const listening = yield* watch
          .listen((change) =>
            Effect.gen(function* () {
              received.push(change.value?.count)
              if (received.length === 1) {
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
              } else if (change.value?.count === 102) {
                yield* Deferred.succeed(collapsed, undefined)
              }
            }),
          )
          .pipe(Effect.forkScoped)
        yield* update(session, 1)
        yield* Deferred.await(entered)
        for (let count = 2; count <= 102; count++) yield* update(session, count)
        assert.strictEqual(watch.value?.count, 1)
        yield* Deferred.succeed(release, undefined)
        yield* Deferred.await(collapsed)
        assert.deepStrictEqual(received, [1, 102])
        yield* Fiber.interrupt(listening)
        assert.strictEqual(yield* watch.closed, 'cancelled')
      }).pipe(Effect.provide(sessionLayer(StoreModule.layerMemory))),
  )
  // Native SQL worker acquisition and transaction notifications progress outside TestClock.
  it.live('does not publish nested writes before outer physical commit', () =>
    Effect.gen(function* () {
      const session = yield* initialize
      const store = yield* Store
      const sql = yield* SqlClient.SqlClient
      const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
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
      const startRead = yield* Deferred.make<void>()
      const reader = yield* Deferred.await(startRead).pipe(
        Effect.andThen(store.journal(0)),
        Effect.tap(() =>
          Effect.sync(() => {
            published = true
          }),
        ),
        Effect.forkScoped,
      )
      const admission = yield* ReadAdmissionFixture.ReadAdmission
      const read = yield* admission.track(reader.id)
      yield* Deferred.succeed(startRead, undefined)
      yield* read.entered
      assert.strictEqual(published, false)
      assert.strictEqual(watch.value?.count, 0)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(writer)
      yield* Fiber.join(reader)
      const changes = yield* Stream.runCollect(watch.changes.pipe(Stream.take(1)))
      assert.strictEqual(changes[0]?.value?.count, 5)
      assert.strictEqual((yield* store.committed).receipts.length, 0)
    }).pipe(Effect.provide(sqlLayers)),
  )
  // Native SQL worker acquisition and transaction notifications progress outside TestClock.
  it.live('outer rollback leaves no receipt, state cache or observer publication', () =>
    Effect.gen(function* () {
      const session = yield* initialize
      const store = yield* Store
      const sql = yield* SqlClient.SqlClient
      const watch = yield* session.watchDoc(token).pipe(Effect.map(Option.getOrUndefined))
      assert.ok(watch)
      const nativeClock = yield* Clock.Clock
      const listeningStarted = yield* Deferred.make<void>()
      const pollCompleted = yield* Deferred.make<void>()
      let afterRollback = false
      const received: Array<number | undefined> = []
      // This clock delegates actual host time. Its sleep call acknowledges that the observer finished processing an empty journal poll; no elapsed duration is used as evidence.
      const observedClock = Clock.Clock.of({
        currentTimeMillisUnsafe: () => nativeClock.currentTimeMillisUnsafe(),
        currentTimeMillis: nativeClock.currentTimeMillis,
        currentTimeNanosUnsafe: () => nativeClock.currentTimeNanosUnsafe(),
        currentTimeNanos: nativeClock.currentTimeNanos,
        monotonicTimeNanosUnsafe: () => nativeClock.monotonicTimeNanosUnsafe(),
        monotonicTimeNanos: nativeClock.monotonicTimeNanos,
        sleep: (duration) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(listeningStarted, undefined)
            if (afterRollback) yield* Deferred.succeed(pollCompleted, undefined)
            yield* nativeClock.sleep(duration)
          }),
      })
      const listening = yield* watch
        .listen((change) =>
          Effect.sync(() => {
            received.push(change.value?.count)
          }),
        )
        .pipe(Effect.provideService(Clock.Clock, observedClock), Effect.forkScoped)
      yield* Deferred.await(listeningStarted)
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
      assert.strictEqual(
        (yield* session.snapshot(token).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
        0,
      )
      assert.strictEqual((yield* store.committed).receipts.length, 0)
      yield* Effect.sync(() => {
        afterRollback = true
      })
      yield* Deferred.await(pollCompleted)
      assert.deepStrictEqual(received, [])
      assert.strictEqual(watch.value?.count, 0)
      const seq = (yield* store.read).nextSeq - 1
      assert.deepStrictEqual((yield* store.journal(seq as Record.Seq)).frames, [])
      yield* Fiber.interrupt(listening)
    }).pipe(Effect.provide(sqlLayers)),
  )
})
