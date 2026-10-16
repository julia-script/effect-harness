import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Graph from 'effect-harness/TaskGraph'
import * as View from 'effect-harness/ConversationView'
import * as Events from 'effect-harness/AgentEvent'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Transaction from 'effect-harness/Transaction'
import { StorageError } from 'effect-harness/StorageError'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import * as Live from 'effect-harness/LiveState'

const bun = 'Bun' in globalThis
const services = bun ? (await import('@effect/platform-bun')).BunServices.layer : NodeServices.layer
const root = Record.ROOT_CONVERSATION_ID
for (const backend of ['temporary', 'jsonl'] as const)
  describe(`atomic public observations: ${backend}`, () => {
    it.live(
      'a failed projection fails its consumers without replacing the successful producer exit',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'atomic-projection-' })

            const layer = (() => {
              if (backend === 'temporary') return TemporaryStorage.layer

              return Storage.layerJsonl({ filePath: `${directory}/native.jsonl` })
            })()
            const native = yield* Layer.build(layer)
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const runtime = yield* HarnessRuntime.make().pipe(
              Effect.provideContext(native),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            yield* Session.commit(runtime.session, Transaction.ensureRoot)
            const consumer = yield* Scope.make()
            const stream = runtime.backend.observations!.watchView!(root)
            const pull = yield* Stream.toPull(stream).pipe(
              Effect.provideService(Scope.Scope, consumer),
            )
            const initial = (yield* pull)[0]
            assert.strictEqual(initial._tag, 'Snapshot')
            const raw = Document.define({
              ...Live.document.definition,
              schema: Schema.JsonObject,
              initial: () => ({}),
            })
            const result = yield* Session.commit(runtime.session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureDocument(tx, raw, Live.target(root))
                yield* Transaction.setDocument(tx, raw, Live.target(root), { generation: {} })
                return 'original-success'
              }),
            )
            assert.strictEqual(result, 'original-success')
            const failed = yield* pull.pipe(Effect.flip)
            assert.strictEqual(failed._tag, 'ObservationError')
            if (failed._tag === 'ObservationError') assert.strictEqual(failed.reason, 'projection')
            assert.isTrue(Option.isSome(yield* Session.conversation(runtime.session, root)))
            assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
            assert.isTrue(
              Exit.isFailure(
                yield* runtime.backend.observations!.conversationSnapshot!(root).pipe(Effect.exit),
              ),
            )
            assert.strictEqual(
              yield* Session.commit(runtime.session, () => Effect.succeed('still-open')),
              'still-open',
            )
            yield* Scope.close(consumer, Exit.void)
          }),
        ).pipe(Effect.provide(services)),
    )

    it.live(
      'rejected writes preserve the replica; uncertain durable writes poison it honestly',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'atomic-settlement-' })

            const layer = (() => {
              if (backend === 'temporary') return TemporaryStorage.layer

              return Storage.layerJsonl({ filePath: `${directory}/native.jsonl` })
            })()
            const native = yield* Layer.build(layer)
            const store = Context.get(native, Storage.Storage)
            const format = Context.get(native, StorageFormat.StorageFormat)
            let mode: 'normal' | 'reject' | 'uncertain' = 'normal'
            const rejected = new StorageError({
              reason: 'conflict',
              operation: 'probe.commit',
              message: 'Definitely rejected',
            })
            const uncertain = new StorageError({
              reason: 'uncertain',
              operation: 'probe.commit',
              message: 'Durable write without acknowledged settlement',
            })
            const wrapped: Storage.Storage['Service'] = {
              ...store,
              commit: (writes) =>
                Effect.suspend(() => {
                  if (mode === 'reject') return Effect.fail(rejected)
                  if (mode === 'uncertain')
                    return store.commit(writes).pipe(Effect.andThen(Effect.fail(uncertain)))
                  return store.commit(writes)
                }),
            }
            const context = Context.add(
              Context.add(native, Storage.Storage, wrapped),
              StorageFormat.StorageFormat,
              { ...format, storage: wrapped },
            )
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const runtime = yield* HarnessRuntime.make().pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const client = yield* Harness.make.pipe(
              Effect.provideService(HarnessBackend, runtime.backend),
            )
            yield* Session.commit(runtime.session, Transaction.ensureRoot)
            const consumer = yield* Scope.make()
            const pull = yield* Stream.toPull(Graph.watch(client)).pipe(
              Effect.provideService(Scope.Scope, consumer),
            )
            const initial = (yield* pull)[0]
            if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial required')
            mode = 'reject'
            assert.strictEqual(
              yield* Session.commit(runtime.session, (tx) =>
                Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
              ).pipe(Effect.flip),
              rejected,
            )
            assert.deepStrictEqual(yield* Graph.snapshot(client), initial)
            assert.strictEqual((yield* store.scanConversations().pipe(Stream.runCollect)).length, 1)
            mode = 'normal'
            yield* Session.commit(runtime.session, (tx) =>
              Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
            )
            const committed = yield* Graph.reduce(initial, (yield* pull)[0])
            assert.strictEqual(committed.value.conversations.length, 2)
            mode = 'uncertain'
            assert.strictEqual(
              yield* Session.commit(runtime.session, (tx) =>
                Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
              ).pipe(Effect.flip),
              uncertain,
            )
            assert.strictEqual(yield* pull.pipe(Effect.flip), uncertain)
            assert.strictEqual(yield* Graph.snapshot(client).pipe(Effect.flip), uncertain)
            assert.strictEqual(
              yield* Session.conversation(runtime.session, root).pipe(Effect.flip),
              uncertain,
            )
            assert.strictEqual((yield* store.scanConversations().pipe(Stream.runCollect)).length, 3)
            yield* Scope.close(consumer, Exit.void)
          }),
        ).pipe(Effect.provide(services)),
    )

    it.live('held native hydration registers before the queued commit, with exact late join', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'atomic-attach-race-' })

          const layer = (() => {
            if (backend === 'temporary') return TemporaryStorage.layer

            return Storage.layerJsonl({ filePath: `${directory}/native.jsonl` })
          })()
          const native = yield* Layer.build(layer)
          const store = Context.get(native, Storage.Storage)
          const format = Context.get(native, StorageFormat.StorageFormat)
          const reading = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const queued = yield* Deferred.make<void>()
          let block = false
          let intercepted = false
          let writerEntered = false
          const wrapped: Storage.Storage['Service'] = {
            ...store,
            scanTasks: (query) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  if (
                    block &&
                    query !== undefined &&
                    Object.keys(query).length === 0 &&
                    !intercepted
                  ) {
                    intercepted = true
                    yield* Deferred.succeed(reading, undefined)
                    yield* Deferred.await(release)
                  }
                  return store.scanTasks(query)
                }),
              ),
          }
          const context = Context.add(
            Context.add(native, Storage.Storage, wrapped),
            StorageFormat.StorageFormat,
            { ...format, storage: wrapped },
          )
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No provider'),
            streamText: () => Stream.die('No provider'),
          })
          const runtime = yield* HarnessRuntime.make().pipe(
            Effect.provideContext(context),
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const client = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, runtime.backend),
          )
          yield* Session.commit(runtime.session, Transaction.ensureRoot)
          assert.strictEqual((yield* store.scanConversations().pipe(Stream.runCollect)).length, 1)
          block = true
          const reader = yield* Graph.watch(client).pipe(
            Stream.tap((frame) =>
              Effect.sync(() => {
                if (frame._tag === 'Snapshot')
                  assert.strictEqual(frame.value.conversations.length, 1)
              }),
            ),
            Stream.take(2),
            Stream.runCollect,
            Effect.forkChild,
          )
          yield* Deferred.await(reading)
          const writer = yield* Deferred.succeed(queued, undefined).pipe(
            Effect.andThen(
              Session.commit(runtime.session, (tx) =>
                Effect.sync(() => {
                  writerEntered = true
                }).pipe(
                  Effect.andThen(
                    Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
                  ),
                ),
              ),
            ),
            Effect.forkChild,
          )
          yield* Deferred.await(queued)
          // Let the writer reach native admission; it must stop at the same held line.
          yield* Effect.forEach(Array.from({ length: 10 }), () => Effect.yieldNow, {
            discard: true,
          })
          assert.isFalse(writerEntered, 'Native transaction entered during held hydration')
          assert.isUndefined(yield* Effect.sync(() => writer.pollUnsafe()))
          yield* Deferred.succeed(release, undefined)
          const child = yield* Fiber.join(writer)
          const frames = yield* Fiber.join(reader)
          assert.strictEqual(frames.length, 2)
          const initial = frames[0]!
          if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial snapshot required')
          assert.strictEqual(initial.value.conversations.length, 1)
          const final = yield* Graph.reduce(initial, frames[1]!)
          assert.strictEqual(final.revision.cursor, 1)
          assert.deepStrictEqual(
            final.value.conversations.map((conversation) => conversation.id),
            [root, child.id],
          )
          const scope = yield* Scope.make()
          const pull = yield* Stream.toPull(Graph.watch(client)).pipe(
            Effect.provideService(Scope.Scope, scope),
          )
          const late = (yield* pull)[0]
          if (late._tag !== 'Snapshot') return yield* Effect.die('Late join starts with snapshot')
          assert.deepStrictEqual(late.value, final.value)
          const before = yield* Graph.snapshot(client)
          const rejected = yield* Session.commit(runtime.session, (tx) =>
            Transaction.createConversation(tx, {
              ownership: { _tag: 'task', taskId: Record.TaskId.make(999999) },
            }),
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(rejected))
          assert.deepStrictEqual(yield* Graph.snapshot(client), before)
          const invalid = yield* Graph.watch(client, { capacity: 0 }).pipe(
            Stream.runHead,
            Effect.exit,
          )
          assert.isTrue(Exit.isFailure(invalid))
          assert.deepStrictEqual(yield* Graph.snapshot(client), before)
          yield* Scope.close(scope, Exit.void)
        }),
      ).pipe(Effect.provide(services)),
    )

    it.live('two handles, repeated overflow, final drain, readonly remount and cold reopen', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'atomic-observation-' })

          const storageLayer = (() => {
            if (backend === 'temporary') return TemporaryStorage.layer

            return Storage.layerJsonl({ filePath: `${directory}/native.jsonl` })
          })()
          const storeScope = yield* Scope.make()
          const context = yield* Layer.buildWithScope(storageLayer, storeScope)
          const store = Context.get(context, Storage.Storage)
          const format = Context.get(context, StorageFormat.StorageFormat)
          let calls = 0
          const model = yield* LanguageModel.make({
            generateText: () =>
              Effect.sync(() => {
                calls++
                return []
              }),
            streamText: () => Stream.die('Observation must not start a provider'),
          })
          const runtimeScope = yield* Scope.make()
          const runtime = yield* HarnessRuntime.make().pipe(
            Effect.provideContext(context),
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideService(Scope.Scope, runtimeScope),
          )
          const client = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, runtime.backend),
          )
          assert.strictEqual(client.observations, runtime.backend.observations)
          const first = yield* client.root
          const second = Option.getOrThrow(yield* client.conversation(root))
          const consumer = yield* Scope.make()
          // Keep default-capacity overflow on the retained runtime; reopen cases exercise the same lifecycle
          // with smaller queues so repeated full snapshots require fewer durable writes.
          const capacity = backend === 'temporary' ? 64 : 2
          const watchOptions = backend === 'temporary' ? undefined : { capacity }
          const burst = capacity + 4
          const pull = yield* Stream.toPull(View.watch(first, watchOptions)).pipe(
            Effect.provideService(Scope.Scope, consumer),
          )
          const eventOptions = { capacity: 2 }
          const eventPull = yield* Stream.toPull(Events.watch(second, eventOptions)).pipe(
            Effect.provideService(Scope.Scope, consumer),
          )
          const graphPull = yield* Stream.toPull(Graph.watch(client, watchOptions)).pipe(
            Effect.provideService(Scope.Scope, consumer),
          )
          const initial = (yield* pull)[0]
          const eventInitial = (yield* eventPull)[0]
          const graphInitial = (yield* graphPull)[0]
          assert.strictEqual(initial._tag, 'Snapshot')
          assert.strictEqual(eventInitial._tag, 'Snapshot')
          assert.strictEqual(graphInitial._tag, 'Snapshot')
          if (
            initial._tag !== 'Snapshot' ||
            eventInitial._tag !== 'Snapshot' ||
            graphInitial._tag !== 'Snapshot'
          )
            return yield* Effect.die('Expected initial snapshots')
          assert.deepStrictEqual(initial.revision, eventInitial.revision)
          eventOptions.capacity = 4096
          const originalEpoch = initial.revision.epoch
          const healthyReady = yield* Deferred.make<void>()
          const healthy = yield* View.watch(second, { capacity: 4096 }).pipe(
            Stream.tap((frame) =>
              frame._tag === 'Snapshot' ? Deferred.succeed(healthyReady, undefined) : Effect.void,
            ),
            Stream.take(burst + 1),
            Stream.runCollect,
            Effect.forkChild,
          )
          yield* Deferred.await(healthyReady)
          const firstHeld = yield* Deferred.make<void>()
          const releaseInitial = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(releaseInitial, undefined))
          const stalled = yield* View.watchWith(
            second,
            (frame): Effect.Effect<View.Frame> =>
              frame._tag === 'Snapshot'
                ? Deferred.succeed(firstHeld, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseInitial)),
                    Effect.as(frame),
                  )
                : Effect.succeed(frame),
            watchOptions,
          ).pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
          yield* Deferred.await(firstHeld)
          for (let i = 0; i < burst; i++)
            yield* Session.commit(runtime.session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } })
                yield* Transaction.appendEntry(tx, root, {
                  kind: 'observation.note',
                  data: { index: i },
                })
              }),
            )
          const latest = yield* View.snapshot(second)
          const healthyFrames = yield* Fiber.join(healthy)
          const healthyInitial = healthyFrames[0]!
          if (healthyInitial._tag !== 'Snapshot')
            return yield* Effect.die('Healthy initial required')
          let healthyReplica = healthyInitial
          for (const frame of healthyFrames.slice(1)) {
            assert.strictEqual(frame._tag, 'Delta')
            healthyReplica = yield* View.reduce(healthyReplica, frame)
            assert.strictEqual(healthyReplica.value.entries.length, healthyReplica.revision.cursor)
          }
          assert.deepStrictEqual(healthyReplica, latest)
          yield* Deferred.succeed(releaseInitial, undefined)
          const stalledFrames = yield* Fiber.join(stalled)
          const firstAfterOverflow = stalledFrames[0]!
          assert.deepStrictEqual(firstAfterOverflow, initial)
          const newestRebase = stalledFrames[1]!
          assert.strictEqual(newestRebase._tag, 'Rebase')
          if (firstAfterOverflow._tag !== 'Snapshot')
            return yield* Effect.die('Initial remains first')
          assert.deepStrictEqual(yield* View.reduce(firstAfterOverflow, newestRebase), latest)
          assert.strictEqual(latest.value.entries.length, burst)
          assert.strictEqual(latest.revision.cursor, burst)
          let replica = initial
          const pending = yield* pull
          assert.isAtMost(pending.length, capacity)
          assert.strictEqual(pending[0]._tag, 'Rebase')
          for (const frame of pending) replica = yield* View.reduce(replica, frame)
          assert.deepStrictEqual(replica, latest)
          const unrelated = yield* Session.commit(runtime.session, (tx) =>
            Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
          )
          yield* Session.commit(runtime.session, (tx) =>
            Transaction.appendEntry(tx, unrelated.id, {
              kind: 'unrelated.note',
              data: { outside: true },
            }),
          )
          yield* Session.commit(runtime.session, () => Effect.void)
          assert.deepStrictEqual(yield* View.snapshot(second), latest)
          let eventReplica = eventInitial
          const eventPending = yield* eventPull
          assert.strictEqual(eventPending[0]._tag, 'Rebase')
          for (const frame of eventPending) eventReplica = yield* Events.reduce(eventReplica, frame)
          assert.deepStrictEqual(eventReplica, latest)
          let graphReplica = graphInitial
          for (const frame of yield* graphPull)
            graphReplica = yield* Graph.reduce(graphReplica, frame)
          assert.deepStrictEqual(graphReplica, yield* Graph.snapshot(client))
          const latePull = yield* Stream.toPull(View.watch(second)).pipe(
            Effect.provideService(Scope.Scope, consumer),
          )
          const late = (yield* latePull)[0]
          assert.deepStrictEqual(late, latest)
          const mutable = latest.value.entries[0]!.data
          if (mutable === null || typeof mutable !== 'object')
            return yield* Effect.die('Expected object entry data')
          Reflect.set(mutable, 'index', -1)
          assert.deepStrictEqual((yield* View.snapshot(second)).value.entries[0]!.data, {
            index: 0,
          })
          for (let i = burst; i < burst * 2; i++)
            yield* Session.commit(runtime.session, (tx) =>
              Transaction.appendEntry(tx, root, { kind: 'observation.note', data: { index: i } }),
            )
          const final = yield* View.snapshot(second)
          yield* Scope.close(runtimeScope, Exit.void)
          const rest: View.Frame[] = []
          const drained = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
            Stream.tap((frame) =>
              Effect.sync(() => {
                rest.push(frame)
              }),
            ),
            Stream.runDrain,
            Effect.exit,
          )
          for (const frame of rest) replica = yield* View.reduce(replica, frame)
          assert.strictEqual(
            replica.value.entries.length,
            burst * 2,
            'Normal close must drain the newest full final snapshot',
          )
          assert.isTrue(Exit.isSuccess(drained), 'Normal close must end the observer normally')
          assert.strictEqual(rest[0]?._tag, 'Rebase')
          assert.deepStrictEqual(replica, final)
          yield* Scope.close(consumer, Exit.void)
          const beforeFormat = yield* format.inspect
          const beforeEntries = yield* store
            .scanEntries({ conversationId: root, order: 'ascending' })
            .pipe(Stream.runCollect)
          const coldStoreScope = yield* Scope.make()
          if (backend !== 'temporary') yield* Scope.close(storeScope, Exit.void)
          const coldContext =
            backend === 'temporary'
              ? context
              : yield* Layer.buildWithScope(storageLayer, coldStoreScope)
          const coldStore = Context.get(coldContext, Storage.Storage)
          const coldFormat = Context.get(coldContext, StorageFormat.StorageFormat)
          const reopenScope = yield* Scope.make()
          const reopened = yield* HarnessRuntime.make().pipe(
            Effect.provideContext(coldContext),
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideService(Scope.Scope, reopenScope),
          )
          const cold = yield* reopened.backend.observations!.conversationSnapshot!(root)
          assert.notStrictEqual(cold.revision.epoch, originalEpoch)
          assert.notProperty(cold, 'commitSeq')
          assert.deepStrictEqual(cold.value, final.value)
          assert.deepStrictEqual(yield* coldFormat.inspect, beforeFormat)
          assert.deepStrictEqual(
            yield* coldStore
              .scanEntries({ conversationId: root, order: 'ascending' })
              .pipe(Stream.runCollect),
            beforeEntries,
          )
          const missing = yield* reopened.backend.observations!.conversationSnapshot!(
            Record.ConversationId.make(999999),
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(missing))
          assert.strictEqual(calls, 0)
          yield* Scope.close(reopenScope, Exit.void)
          yield* Scope.close(coldStoreScope, Exit.void)
          yield* Scope.close(storeScope, Exit.void)
        }),
      ).pipe(Effect.provide(services)),
    )
  })
