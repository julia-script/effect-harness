import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Document from 'effect-harness/Document'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Live from 'effect-harness/LiveState'
import * as Identity from 'effect-harness/Identity'
import * as Usage from 'effect-harness/Usage'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import { StorageError } from 'effect-harness/StorageError'
import * as Transaction from 'effect-harness/Transaction'
import * as Documents from '../src/internal/DocumentBackend.js'
import * as Observations from '../src/internal/ObservationRuntime.js'
import * as Runtime from '../src/internal/SessionRuntime.js'

const target = { scope: { _tag: 'session' } } as const
const document = Document.family({
  kind: 'cache.counter',
  scope: 'session',
  version: 1,
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
const selected = { ...target, key: 'one' }
const address = { ...selected, kind: document.definition.kind }
const some = Option.getOrThrow
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(Storage.layerMemory))
const counted = Effect.gen(function* () {
  const storage = yield* Storage.Storage
  const counts = { finds: 0, loads: 0 }
  const service: Storage.Storage['Service'] = {
    ...storage,
    findDocument: (address, at) =>
      Effect.suspend(() => {
        counts.finds++
        return storage.findDocument(address, at)
      }),
    document: (id, at) =>
      Effect.suspend(() => {
        counts.loads++
        return storage.document(id, at)
      }),
  }
  return { counts, service }
})
const setup = Effect.fnUntraced(function* (
  options: Session.Options = { documentRetentionMs: 100 },
) {
  const { counts, service } = yield* counted
  const session = yield* Session.make(options).pipe(Effect.provideService(Storage.Storage, service))
  yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, document, selected))
  counts.finds = 0
  counts.loads = 0
  return { session, counts }
})
const read = (session: Session.Session) => Session.snapshot(session, document, selected)
const write = (session: Session.Session, count: number) =>
  Session.commit(session, (tx) => Transaction.setDocument(tx, document, selected, { count }))

// Count real storage accesses: cache eviction has no public introspection API.
describe('Session idle materialization retention', () => {
  it.effect('defaults to ten minutes and reloads the same durable incarnation after eviction', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup({})
        const initial = some(yield* read(session))
        yield* TestClock.adjust(600_000)
        const reloaded = some(yield* read(session))
        assert.deepEqual(reloaded, initial)
        assert.strictEqual(counts.loads, 1)
        yield* read(session)
        assert.strictEqual(counts.loads, 1)
        yield* TestClock.adjust(600_000)
        yield* read(session)
        assert.strictEqual(counts.loads, 2)
      }),
    ),
  )

  it.effect('expires negative lookups and refreshes reads and committed updates', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup()
        const missing = { ...target, key: 'missing' }
        assert.isTrue(Option.isNone(yield* Session.snapshot(session, document, missing)))
        yield* Session.snapshot(session, document, missing)
        assert.strictEqual(counts.finds, 1)
        yield* TestClock.adjust(60)
        yield* read(session)
        yield* TestClock.adjust(60)
        yield* read(session)
        assert.strictEqual(counts.loads, 0)
        yield* Session.snapshot(session, document, missing)
        assert.strictEqual(counts.finds, 2)
        yield* TestClock.adjust(60)
        yield* write(session, 7)
        yield* TestClock.adjust(99)
        assert.strictEqual(some(yield* read(session)).value.count, 7)
        assert.strictEqual(counts.loads, 0)
        yield* TestClock.adjust(100)
        assert.strictEqual(some(yield* read(session)).value.count, 7)
        assert.strictEqual(counts.loads, 1)
      }),
    ),
  )

  for (const kind of ['snapshot', 'materialized', 'remote'] as const)
    it.effect(`pins ${kind} watchers until the last consumer releases`, () =>
      run(
        Effect.gen(function* () {
          const { session, counts } = yield* setup()
          const first = yield* Scope.make()
          const second = yield* Scope.make()
          const watch = () => {
            if (kind === 'snapshot')
              return Session.watch(session, document, selected).pipe(Stream.map(() => undefined))
            if (kind === 'materialized')
              return Session.watchMaterialized(session, document, selected).pipe(
                Stream.map(() => undefined),
              )
            return Documents.make(session).watch!({
              address,
              metadata: document.definition,
              family: true,
            }).pipe(Stream.map(() => undefined))
          }
          const a = yield* Stream.toPull(watch()).pipe(Effect.provideService(Scope.Scope, first))
          const b = yield* Stream.toPull(watch()).pipe(Effect.provideService(Scope.Scope, second))
          yield* a
          yield* b
          yield* TestClock.adjust(200)
          yield* read(session)
          assert.strictEqual(counts.loads, 0)
          yield* Scope.close(first, Exit.void)
          yield* TestClock.adjust(200)
          yield* write(session, 8)
          yield* b
          assert.strictEqual(some(yield* read(session)).value.count, 8)
          assert.strictEqual(counts.loads, 0)
          yield* Scope.close(second, Exit.void)
          yield* TestClock.adjust(100)
          assert.strictEqual(some(yield* read(session)).value.count, 8)
          assert.strictEqual(counts.loads, 1)
        }),
      ),
    )

  it.effect('old watcher release cannot pin or refresh a replacement incarnation', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup()
        const consumer = yield* Scope.make()
        const pull = yield* Session.watch(session, document, selected).pipe(
          Stream.rechunk(1),
          Stream.toPull,
          Effect.provideService(Scope.Scope, consumer),
        )
        const initial = (yield* pull)[0]
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.retireDocument(tx, document, selected)
            yield* Transaction.ensureDocument(tx, document, selected)
            yield* Transaction.setDocument(tx, document, selected, { count: 9 })
          }),
        )
        yield* TestClock.adjust(60)
        yield* Scope.close(consumer, Exit.void)
        yield* TestClock.adjust(40)
        const replacement = some(yield* read(session))
        assert.notStrictEqual(replacement.record.id, initial.record.id)
        assert.strictEqual(replacement.value.count, 9)
        assert.strictEqual(counts.loads, 1)
      }),
    ),
  )

  it.effect('releases overflowed watchers even while their consumer scope remains open', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup()
        const pull = yield* Session.watch(session, document, selected).pipe(Stream.toPull)
        yield* pull
        for (let count = 1; count <= 65; count++) yield* write(session, count)
        yield* TestClock.adjust(100)
        assert.strictEqual(some(yield* read(session)).value.count, 65)
        assert.strictEqual(counts.loads, 1)
      }),
    ),
  )

  it.effect('serializes expiry with held-line hydration and transaction adoption/publication', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup()
        const entered = yield* Deferred.make<void>()
        const resume = yield* Deferred.make<void>()
        const stream = yield* Session.commits(session).pipe(Stream.toPull)
        const notification = yield* stream.pipe(Effect.forkChild)
        yield* Effect.yieldNow
        const transaction = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const runtime = yield* Runtime.get(session)
            const before = some(yield* Runtime.load(runtime, address))
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(resume)
            // Scheduler/request hydration already owns the line: load must never acquire it again.
            assert.strictEqual(some(yield* Runtime.load(runtime, address)), before)
            yield* Transaction.setDocument(tx, document, selected, { count: 4 })
            assert.strictEqual(
              some(yield* Transaction.snapshot(tx, document, selected)).value.count,
              4,
            )
          }),
        ).pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        yield* TestClock.adjust(500)
        yield* Deferred.succeed(resume, undefined)
        yield* Fiber.join(transaction)
        const commit = (yield* Fiber.join(notification))[0]
        assert.isTrue(commit.changes.some((write) => write._tag === 'document.change'))
        assert.strictEqual(some(yield* read(session)).value.count, 4)
        assert.strictEqual(counts.loads, 0)
        yield* TestClock.adjust(100)
        assert.strictEqual(some(yield* read(session)).value.count, 4)
        assert.strictEqual(counts.loads, 1)
      }),
    ),
  )

  it.effect('zero retention evicts at the next available line while watchers stay attached', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup({ documentRetentionMs: 0 })
        yield* TestClock.adjust(0)
        yield* read(session)
        assert.strictEqual(counts.loads, 1)
        yield* TestClock.adjust(0)
        yield* read(session)
        assert.strictEqual(counts.loads, 2)
        const consumer = yield* Scope.make()
        const pull = yield* Session.watch(session, document, selected).pipe(
          Stream.toPull,
          Effect.provideService(Scope.Scope, consumer),
        )
        yield* pull
        const attachedLoads = counts.loads
        yield* TestClock.adjust(1_000)
        yield* read(session)
        assert.strictEqual(counts.loads, attachedLoads)
        yield* Scope.close(consumer, Exit.void)
        yield* TestClock.adjust(0)
        yield* read(session)
        assert.strictEqual(counts.loads, attachedLoads + 1)
      }),
    ),
  )

  it.effect('preserves rewindable history and forks after idle materializations expire', () =>
    run(
      Effect.gen(function* () {
        const session = yield* Session.make({ documentRetentionMs: 100 })
        const history = Document.define({
          ...document.definition,
          kind: 'cache.history',
          scope: 'conversation',
          history: 'rewindable',
          fork: 'asOf',
        })
        const root = Record.ROOT_CONVERSATION_ID
        const parent = { scope: { _tag: 'conversation', conversationId: root } } as const
        const cutoff = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            yield* Transaction.ensureDocument(tx, history, parent)
            yield* Transaction.setDocument(tx, history, parent, { count: 1 })
            return yield* Transaction.appendEntry(tx, root, { kind: 'cutoff' })
          }),
        )
        yield* Session.commit(session, (tx) =>
          Transaction.setDocument(tx, history, parent, { count: 2 }),
        )
        yield* TestClock.adjust(100)
        assert.strictEqual(
          some(yield* Session.snapshotAsOf(session, history, parent, cutoff.id)).value.count,
          1,
        )
        const fork = yield* Session.commit(session, (tx) =>
          Transaction.forkConversation(tx, root, cutoff.id, {
            ownership: { _tag: 'ownerless' },
          }),
        )
        yield* TestClock.adjust(100)
        assert.strictEqual(some(yield* Session.snapshot(session, history, parent)).value.count, 2)
        assert.strictEqual(
          some(
            yield* Session.snapshot(session, history, {
              scope: { _tag: 'conversation', conversationId: fork.id },
            }),
          ).value.count,
          1,
        )
      }),
    ),
  )

  it.effect('supports explicit indefinite retention', () =>
    run(
      Effect.gen(function* () {
        const { session, counts } = yield* setup({ documentRetentionMs: Infinity })
        yield* TestClock.adjust('365 days')
        yield* read(session)
        assert.strictEqual(counts.loads, 0)
      }),
    ),
  )

  for (const documentRetentionMs of [-1, NaN, -Infinity])
    it.effect(
      `rejects invalid retention ${documentRetentionMs} before acquiring storage ownership`,
      () =>
        run(
          Effect.gen(function* () {
            const error = yield* Session.make({ documentRetentionMs }).pipe(Effect.flip)
            assert.strictEqual(error.reason, 'invalid')
            assert.strictEqual(error.operation, 'session.make')
            yield* Session.make()
          }),
        ),
    )

  for (const ending of ['close', 'poison'] as const)
    it.effect(`cancels the single pending eviction sleep on ${ending}`, () =>
      run(
        Effect.gen(function* () {
          const clock = yield* Clock.Clock
          let sleeping = 0
          let started = 0
          const tracked: Clock.Clock = {
            currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
            currentTimeMillis: clock.currentTimeMillis,
            currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
            currentTimeNanos: clock.currentTimeNanos,
            monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
            monotonicTimeNanos: clock.monotonicTimeNanos,
            sleep: (duration) =>
              Effect.gen(function* () {
                sleeping++
                started++
                yield* clock.sleep(duration).pipe(
                  Effect.ensuring(
                    Effect.sync(() => {
                      sleeping--
                    }),
                  ),
                )
              }),
          }
          const owner = yield* Scope.make()
          const storage = yield* Storage.Storage
          let fail = false
          const uncertain = new StorageError({
            reason: 'uncertain',
            operation: 'test',
            message: 'injected',
          })
          const service: Storage.Storage['Service'] = {
            ...storage,
            commit: (writes) =>
              Effect.suspend(() => (fail ? Effect.fail(uncertain) : storage.commit(writes))),
          }
          const session = yield* Session.make({ documentRetentionMs: 100 }).pipe(
            Effect.provideService(Scope.Scope, owner),
            Effect.provideService(Storage.Storage, service),
            Effect.provideService(Clock.Clock, tracked),
          )
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              for (let i = 0; i < 20; i++)
                yield* Transaction.ensureDocument(tx, document, { ...target, key: String(i) })
            }),
          )
          yield* TestClock.adjust(1)
          assert.strictEqual(sleeping, 1)
          assert.strictEqual(started, 1)
          if (ending === 'close') yield* Scope.close(owner, Exit.void)
          else {
            fail = true
            assert.strictEqual(
              yield* Session.commit(session, (tx) =>
                Transaction.setDocument(tx, document, { ...target, key: '0' }, { count: 1 }),
              ).pipe(Effect.flip),
              uncertain,
            )
          }
          assert.strictEqual(sleeping, 0)
          yield* TestClock.adjust(1_000)
          assert.strictEqual(started, 1)
          const denied = yield* read(session).pipe(Effect.flip)
          assert.isTrue(
            'reason' in denied && (denied.reason === 'closed' || denied.reason === 'uncertain'),
          )
          yield* Scope.close(owner, Exit.void)
        }),
      ),
    )

  it.effect(
    'forwards Harness retention and keeps conversation observations consistent across eviction',
    () =>
      run(
        Effect.gen(function* () {
          const { service, counts } = yield* counted
          const format = yield* StorageFormat.StorageFormat
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No provider'),
            streamText: () => Stream.die('No provider'),
          })
          const runtime = yield* HarnessRuntime.make({ documentRetentionMs: 100 }).pipe(
            Effect.provideService(Storage.Storage, service),
            Effect.provideService(StorageFormat.StorageFormat, { ...format, storage: service }),
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const session = runtime.session
          const root = Record.ROOT_CONVERSATION_ID
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              yield* Transaction.ensureDocument(tx, document, selected)
              yield* Transaction.ensureDocument(tx, Live.document, Live.target(root))
            }),
          )
          const consumer = yield* Scope.make()
          const pull = yield* Observations.make(session).watchView!(root).pipe(
            Stream.rechunk(1),
            Stream.toPull,
            Effect.provideService(Scope.Scope, consumer),
          )
          const initial = (yield* pull)[0]
          assert.strictEqual(initial._tag, 'Snapshot')
          const before = counts.loads
          yield* TestClock.adjust(100)
          yield* read(session)
          assert.strictEqual(counts.loads, before + 1)
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, Live.document, Live.target(root), {
              generation: {
                requestId: Identity.RequestId.make('update'),
                attempt: Usage.AttemptId.make(1),
                revision: 1,
              },
            }),
          )
          const update = (yield* pull)[0]
          assert.strictEqual(update._tag, 'Delta')
          if (update._tag === 'Delta') {
            assert.strictEqual(update.ops[0].value.live.generation?.revision, 1)
            assert.strictEqual(update.to, 1)
          }
          yield* Scope.close(consumer, Exit.void)
        }),
      ),
  )
})
