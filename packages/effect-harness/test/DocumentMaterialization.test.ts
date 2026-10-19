import * as TemporaryStorage from './fixtures/Storage.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Pull from 'effect/Pull'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Conversation from '../src/Conversation.js'
import * as Document from 'effect-harness/Document'
import * as DocumentMaterialization from '../src/DocumentMaterialization.js'
import * as Harness from '../src/Harness.js'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import { StorageFormat } from 'effect-harness/StorageFormat'
import * as Record from 'effect-harness/Record'
import * as Session from '../src/Session.js'
import { SessionError } from 'effect-harness/SessionError'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as SessionRuntime from '../src/internal/SessionRuntime.js'

const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const ownerless = { ownership: { _tag: 'ownerless' } } satisfies Transaction.ConversationOptions
const Legacy = Schema.Struct({ legacy: Schema.Natural })
const Current = Schema.Struct({ count: Schema.Natural })
const old = Document.define({
  kind: 'materialization.counter',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Legacy,
  initial: () => ({ legacy: 4 }),
})
const migration: Document.Migration = (value) =>
  Schema.decodeUnknownEffect(Legacy)(value).pipe(Effect.map(({ legacy }) => ({ count: legacy })))
const current = Document.define({
  kind: old.definition.kind,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  version: 3,
  schema: Current,
  initial: () => ({ count: 0 }),
  migrations: { 1: migration },
})
const some = Option.getOrThrow
const withMemory = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(TemporaryStorage.layer))
const initialize = (session: Session.Session) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      yield* Transaction.ensureDocument(tx, old, target)
      return yield* Transaction.appendEntry(tx, root, { kind: 'materialization.cutoff' })
    }),
  )
const trackedStorage = Effect.gen(function* () {
  const storage = yield* Storage.Storage
  let commits = 0
  const tracked = {
    ...storage,
    commit: (writes: Iterable<Record.StorageWrite>) =>
      Effect.sync(() => {
        commits++
      }).pipe(Effect.andThen(storage.commit(writes))),
  } satisfies Storage.Storage['Service']
  const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, tracked))
  return {
    storage,
    session,
    writes: () => commits,
    reset: () => {
      commits = 0
    },
  }
})

class Labels extends Context.Service<Labels, string>()('test/materialization/Labels') {}
const ServiceCodec = Current.pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect(({ count }) =>
      Labels.pipe(Effect.map((label) => ({ count, label }))),
    ),
    encode: SchemaGetter.transform(({ count }) => ({ count })),
  }),
)
const serviceDocument = Document.define({
  ...current.definition,
  schema: ServiceCodec,
  initial: () => ({ count: 0, label: 'initial' }),
})

describe('Document materialization', () => {
  it.effect('requires a direct migration from the actual stored version', () =>
    withMemory(
      Effect.gen(function* () {
        const { session, reset, writes } = yield* trackedStorage
        yield* initialize(session)
        const indirect = Document.define({
          ...current.definition,
          migrations: { 2: Effect.succeed },
        })
        reset()
        const error = yield* Session.materializedSnapshot(session, indirect, target).pipe(
          Effect.flip,
        )
        assert.strictEqual(error._tag, 'SessionError')
        if (error._tag === 'SessionError') assert.strictEqual(error.reason, 'conflict')
        assert.strictEqual(writes(), 0)
      }),
    ),
  )

  it.effect(
    'does not lose a durable upgrade queued while the initial migration is in progress',
    () =>
      withMemory(
        Effect.gen(function* () {
          const { session, reset, writes } = yield* trackedStorage
          yield* initialize(session)
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const observed = Document.define({
            ...current.definition,
            migrations: {
              1: (value) =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(migration(value)),
                ),
            },
          })
          reset()
          const watching = yield* Session.watchMaterialized(session, observed, target).pipe(
            Stream.take(2),
            Stream.runCollect,
            Effect.forkChild,
          )
          yield* Deferred.await(entered)
          const acquisition = yield* Session.commit(session, (tx) =>
            Transaction.ensureDocument(tx, current, target),
          ).pipe(Effect.forkChild)
          yield* Effect.yieldNow
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(acquisition)
          const frames = yield* Fiber.join(watching)
          assert.deepEqual(
            frames.map((frame) => frame.storedVersion),
            [1, 3],
          )
          assert.deepEqual(
            frames.map((frame) => frame.value),
            [{ count: 4 }, { count: 4 }],
          )
          assert.strictEqual((yield* SessionRuntime.get(session)).subscribers.size, 0)
          assert.strictEqual(writes(), 1)
        }),
      ),
  )

  it.effect(
    'failed later watch materialization releases the subscriber and leaves the raw revision readable',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* initialize(session)
          const entered = yield* Deferred.make<void>()
          const unstable = Document.define({
            ...current.definition,
            migrations: {
              1: (value) =>
                Schema.decodeUnknownEffect(Legacy)(value).pipe(
                  Effect.map(({ legacy }) => ({ count: legacy === 4 ? legacy : -1 })),
                ),
            },
          })
          const watching = yield* Session.watchMaterialized(session, unstable, target).pipe(
            Stream.tap(() => Deferred.succeed(entered, undefined)),
            Stream.runDrain,
            Effect.flip,
            Effect.forkChild,
          )
          yield* Deferred.await(entered)
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, old, target, { legacy: 8 }),
          )
          assert.strictEqual((yield* Fiber.join(watching))._tag, 'SchemaError')
          assert.strictEqual((yield* SessionRuntime.get(session)).subscribers.size, 0)
          assert.deepEqual(some(yield* Session.snapshot(session, old, target)).value, { legacy: 8 })
        }),
      ),
  )
  it.effect('retains actual encoded delta replay metadata after a cold load', () =>
    withMemory(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        const id = yield* storage.mintId<Record.DocumentId>()
        yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          {
            _tag: 'document.create',
            record: {
              id,
              kind: old.definition.kind,
              ...target,
              history: 'rewindable',
              fork: 'asOf',
            },
            content: { _tag: 'base', version: 1, value: { legacy: 4 } },
          },
        ])
        yield* storage.commit([
          {
            _tag: 'document.change',
            id,
            content: {
              _tag: 'delta',
              version: 1,
              ops: [['set', ['legacy'], 7]],
            },
          },
        ])
        const { session, writes, reset } = yield* trackedStorage
        reset()
        const result = some(yield* Session.materializedSnapshot(session, current, target))
        assert.deepEqual(result.value, { count: 7 })
        assert.strictEqual(result.storedVersion, 1)
        assert.strictEqual(result.deltasSinceBase, 1)
        assert.deepEqual(some(yield* Session.snapshot(session, old, target)).value, { legacy: 7 })
        assert.strictEqual(some(yield* storage.document(id)).version, 1)
        assert.strictEqual(writes(), 0)
      }),
    ),
  )
  it.effect(
    'materializes v1 to v3 without writes and preserves exact reads and detached metadata',
    () =>
      withMemory(
        Effect.gen(function* () {
          const tracked = yield* trackedStorage
          const cutoff = yield* initialize(tracked.session)
          tracked.reset()
          const exact = some(yield* Session.snapshot(tracked.session, old, target))
          const result = some(
            yield* tracked.session.pipe(Session.materializedSnapshot(current, target)),
          )
          const historical = some(
            yield* tracked.session.pipe(
              Session.materializedSnapshotAsOf(current, target, cutoff.id),
            ),
          )
          assert.deepEqual(result.value, { count: 4 })
          assert.deepEqual(result, historical)
          assert.strictEqual(result.storedVersion, 1)
          assert.strictEqual(result.effectiveVersion, 3)
          assert.deepEqual(result.record, exact.record)
          Object.assign(result.value, { count: 90 })
          Object.assign(result.record, { kind: 'caller-mutated' })
          assert.deepEqual(some(yield* Session.snapshot(tracked.session, old, target)).value, {
            legacy: 4,
          })
          assert.deepEqual(some(yield* tracked.storage.document(exact.record.id)).value, {
            legacy: 4,
          })
          assert.strictEqual(
            (yield* Session.snapshot(tracked.session, current, target).pipe(Effect.flip))._tag,
            'SessionError',
          )
          assert.strictEqual(tracked.writes(), 0)
        }),
      ),
  )

  it.effect('owns callback input and result, retaining the original replay count', () =>
    withMemory(
      Effect.gen(function* () {
        const { storage, session, reset, writes } = yield* trackedStorage
        yield* initialize(session)
        const record = some(yield* Session.snapshot(session, old, target)).record
        const detached = { record, version: 1, value: { legacy: 5 }, deltasSinceBase: 7 }
        let output: Schema.JsonObject = {}
        const mutating = Document.define({
          ...current.definition,
          migrations: {
            1: (value) =>
              Schema.decodeUnknownEffect(Legacy)(value).pipe(
                Effect.map(({ legacy }) => {
                  output = { count: legacy }
                  Object.assign(value, { legacy: 999 })
                  return output
                }),
              ),
          },
        })
        reset()
        const result = yield* DocumentMaterialization.materialize(mutating, detached)
        Object.assign(output, { count: 80 })
        assert.deepEqual(result.value, { count: 5 })
        assert.strictEqual(result.deltasSinceBase, 7)
        assert.deepEqual(detached.value, { legacy: 5 })
        assert.deepEqual(some(yield* storage.document(record.id)).value, { legacy: 4 })
        assert.strictEqual(writes(), 0)
      }),
    ),
  )

  for (const backend of ['JSONL', 'SQL'] as const) {
    it.live(
      `cold ${backend} reopen retains raw v1 history after materialized current and historical reads`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const dir = yield* fs.makeTempDirectoryScoped({ prefix: 'materialization-' })
            const layer =
              backend === 'JSONL'
                ? Storage.layerJsonl({ filePath: `${dir}/storage.jsonl` })
                : Storage.layerJsonl({ filePath: `${dir}/storage.sqlite` }).pipe(
                    Layer.provide(NodeServices.layer),
                  )
            const cutoff = yield* Effect.scoped(
              Effect.gen(function* () {
                return yield* initialize(yield* Session.make())
              }),
            ).pipe(Effect.provide(layer))
            yield* Effect.scoped(
              Effect.gen(function* () {
                const { session, writes, reset } = yield* trackedStorage
                reset()
                assert.strictEqual(
                  some(yield* Session.materializedSnapshot(session, current, target)).storedVersion,
                  1,
                )
                assert.deepEqual(
                  some(yield* Session.materializedSnapshotAsOf(session, current, target, cutoff.id))
                    .value,
                  { count: 4 },
                )
                assert.strictEqual(writes(), 0)
              }),
            ).pipe(Effect.provide(layer))
            yield* Effect.scoped(
              Effect.gen(function* () {
                const session = yield* Session.make()
                assert.deepEqual(some(yield* Session.snapshot(session, old, target)).value, {
                  legacy: 4,
                })
                assert.strictEqual(
                  some(yield* Session.snapshotAsOf(session, old, target, cutoff.id)).version,
                  1,
                )
              }),
            ).pipe(Effect.provide(layer))
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }

  for (const failure of [
    'missing',
    'metadata',
    'policy',
    'callback',
    'throw',
    'defect',
    'schema',
  ] as const) {
    it.effect(`semantic ${failure} failure leaves encoded cache and storage unchanged`, () =>
      withMemory(
        Effect.gen(function* () {
          const { session, storage, reset, writes } = yield* trackedStorage
          yield* initialize(session)
          const source = some(yield* Session.snapshot(session, old, target))
          let definition = current
          if (failure === 'missing')
            definition = Document.define({ ...current.definition, migrations: {} })
          if (failure === 'metadata')
            definition = Document.define({ ...current.definition, version: 0 })
          if (failure === 'policy')
            definition = Document.define({
              ...current.definition,
              scope: 'conversation',
              history: 'rewindable',
              fork: 'current',
            })
          if (['callback', 'throw', 'defect', 'schema'].includes(failure))
            definition = Document.define({
              ...current.definition,
              migrations: {
                1: (value) => {
                  Object.assign(value, { legacy: 999 })
                  if (failure === 'throw') throw new Error('callback throw')
                  if (failure === 'defect') return Effect.die('callback defect')
                  if (failure === 'callback')
                    return Effect.fail(
                      new SessionError({
                        reason: 'invalid',
                        operation: 'test',
                        message: 'callback failure',
                      }),
                    )
                  return Effect.succeed({ count: -1 })
                },
              },
            })
          reset()
          const error = yield* Session.materializedSnapshot(session, definition, target).pipe(
            Effect.flip,
          )
          assert.strictEqual(
            error._tag,
            failure === 'schema' || failure === 'metadata' ? 'SchemaError' : 'SessionError',
          )
          assert.deepEqual(some(yield* Session.snapshot(session, old, target)), source)
          assert.deepEqual(some(yield* storage.document(source.record.id)).value, { legacy: 4 })
          assert.strictEqual(writes(), 0)
        }),
      ),
    )
  }

  it.effect('rejects a valid future stored version', () =>
    withMemory(
      Effect.gen(function* () {
        const { session, reset, writes } = yield* trackedStorage
        yield* initialize(session)
        yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, current, target))
        const lower = Document.define({
          ...current.definition,
          version: 2,
          migrations: { 3: Effect.succeed },
        })
        reset()
        const error = yield* Session.materializedSnapshot(session, lower, target).pipe(Effect.flip)
        assert.strictEqual(error._tag, 'SessionError')
        if (error._tag === 'SessionError') assert.strictEqual(error.reason, 'conflict')
        assert.strictEqual(writes(), 0)
      }),
    ),
  )

  it.effect(
    'watches exact encoded versions through concurrent durable acquisition and later update',
    () =>
      withMemory(
        Effect.gen(function* () {
          const { session, storage, reset, writes } = yield* trackedStorage
          yield* initialize(session)
          let migrations = 0
          const observed = Document.define({
            ...current.definition,
            migrations: {
              1: (value) =>
                Effect.sync(() => {
                  migrations++
                }).pipe(Effect.andThen(migration(value))),
            },
          })
          const pull = yield* session.pipe(
            Session.watchMaterialized(observed, target),
            Stream.rechunk(1),
            Stream.toPull,
          )
          const initial = (yield* pull)[0]
          assert.strictEqual(initial.storedVersion, 1)
          assert.strictEqual(migrations, 1)
          reset()
          yield* Effect.all(
            Array.from({ length: 8 }, () =>
              Session.commit(session, (tx) => Transaction.ensureDocument(tx, observed, target)),
            ),
            { concurrency: 'unbounded' },
          )
          const equivalent = (yield* pull)[0]
          assert.deepEqual(equivalent.value, initial.value)
          assert.strictEqual(equivalent.storedVersion, 3)
          assert.strictEqual(equivalent.effectiveVersion, 3)
          assert.strictEqual(migrations, 2)
          assert.strictEqual(writes(), 1)
          yield* Session.commit(session, (tx) =>
            Transaction.updateDocument(tx, observed, target, ({ count }) => ({ count: count + 1 })),
          )
          const updated = (yield* pull)[0]
          assert.deepEqual(updated.value, { count: 5 })
          assert.strictEqual(migrations, 2)
          assert.deepEqual(some(yield* storage.document(initial.record.id)).value, { count: 5 })
          yield* Session.commit(session, (tx) => Transaction.retireDocument(tx, current, target))
          const ended = yield* pull.pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(ended))
          if (Exit.isFailure(ended)) assert.isTrue(Pull.isDoneCause(ended.cause))
        }),
      ),
  )

  for (const fork of ['asOf', 'current', 'initial'] as const) {
    it.effect(
      `materializes ${fork} fork selection and inherited cutoffs without changing versions`,
      () =>
        withMemory(
          Effect.gen(function* () {
            const { session, reset, writes } = yield* trackedStorage
            const source = Document.define({
              ...old.definition,
              scope: 'conversation',
              history: 'rewindable',
              fork,
            })
            const upgraded = Document.define({
              ...current.definition,
              scope: 'conversation',
              history: 'rewindable',
              fork,
            })
            const cutoff = yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureRoot(tx)
                yield* Transaction.ensureDocument(tx, source, target)
                return yield* Transaction.appendEntry(tx, root, { kind: 'fork.cutoff' })
              }),
            )
            yield* Session.commit(session, (tx) =>
              Transaction.updateDocument(tx, upgraded, target, () => ({ count: 9 })),
            )
            const child = yield* Session.commit(session, (tx) =>
              Transaction.forkConversation(tx, root, cutoff.id, ownerless),
            )
            const childTarget = {
              scope: { _tag: 'conversation', conversationId: child.id },
            } satisfies Document.Target
            reset()
            const now = yield* Session.materializedSnapshot(session, upgraded, childTarget)
            if (fork === 'initial') assert.isTrue(Option.isNone(now))
            else {
              assert.strictEqual(some(now).storedVersion, fork === 'asOf' ? 1 : 3)
              assert.deepEqual(some(now).value, { count: fork === 'asOf' ? 4 : 9 })
            }
            const past = some(
              yield* Session.materializedSnapshotAsOf(session, upgraded, childTarget, cutoff.id),
            )
            assert.strictEqual(past.storedVersion, 1)
            assert.deepEqual(past.value, { count: 4 })
            assert.strictEqual(writes(), 0)
          }),
        ),
    )
  }

  it.effect('missing and invalid historical reads fail without initialization or writes', () =>
    withMemory(
      Effect.gen(function* () {
        const { session, reset, writes } = yield* trackedStorage
        assert.isTrue(Option.isNone(yield* Session.materializedSnapshot(session, current, target)))
        const absent = yield* Session.watchMaterialized(session, current, target).pipe(
          Stream.runCollect,
          Effect.flip,
        )
        assert.strictEqual(absent._tag, 'SessionError')
        if (absent._tag === 'SessionError') assert.strictEqual(absent.reason, 'notFound')
        yield* initialize(session)
        reset()
        const invalid = yield* Session.materializedSnapshotAsOf(
          session,
          current,
          target,
          Record.EntryId.make(999),
        ).pipe(Effect.flip)
        assert.strictEqual(invalid._tag, 'SessionError')
        const latest = Document.define({
          ...current.definition,
          scope: 'conversation',
          history: 'latest',
          fork: 'current',
        })
        assert.strictEqual(
          (yield* Session.materializedSnapshotAsOf(
            session,
            latest,
            target,
            Record.EntryId.make(999),
          ).pipe(Effect.flip))._tag,
          'SessionError',
        )
        assert.strictEqual(writes(), 0)
      }),
    ),
  )

  it.effect(
    'interruption during watch initialization releases coordination without subscribing',
    () =>
      withMemory(
        Effect.gen(function* () {
          const { session, reset, writes } = yield* trackedStorage
          yield* initialize(session)
          const entered = yield* Deferred.make<void>()
          const pending = Document.define({
            ...current.definition,
            migrations: {
              1: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
            },
          })
          reset()
          const fiber = yield* Session.watchMaterialized(session, pending, target).pipe(
            Stream.runCollect,
            Effect.forkChild,
          )
          yield* Deferred.await(entered)
          yield* Fiber.interrupt(fiber)
          assert.strictEqual((yield* SessionRuntime.get(session)).subscribers.size, 0)
          assert.strictEqual(some(yield* Session.snapshot(session, old, target)).version, 1)
          assert.strictEqual(writes(), 0)
          yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, current, target))
        }),
      ),
  )

  it.effect('cancellation and Session close release an attached materialized watch', () =>
    withMemory(
      Effect.gen(function* () {
        const sessionScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
        const session = yield* Session.make().pipe(Effect.provideService(Scope.Scope, sessionScope))
        yield* initialize(session)
        const initial = yield* Deferred.make<void>()
        const fiber = yield* Session.watchMaterialized(session, current, target).pipe(
          Stream.tap(() => Deferred.succeed(initial, undefined)),
          Stream.runDrain,
          Effect.forkChild,
        )
        yield* Deferred.await(initial)
        assert.strictEqual((yield* SessionRuntime.get(session)).subscribers.size, 1)
        yield* Fiber.interrupt(fiber)
        assert.strictEqual((yield* SessionRuntime.get(session)).subscribers.size, 0)
        const pull = yield* Session.watchMaterialized(session, current, target).pipe(Stream.toPull)
        yield* pull
        yield* Scope.close(sessionScope, Exit.void)
        const ended = yield* pull.pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(ended))
        if (Exit.isFailure(ended)) assert.isTrue(Pull.isDoneCause(ended.cause))
        assert.strictEqual((yield* SessionRuntime.get(session)).subscribers.size, 0)
      }),
    ),
  )

  it.effect(
    'runs real serviceful target decoding for materialization and every watch version',
    () =>
      withMemory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const cutoff = yield* initialize(session)
          const result = some(
            yield* Session.materializedSnapshot(session, serviceDocument, target).pipe(
              Effect.provideService(Labels, 'read'),
            ),
          )
          assert.deepEqual(result.value, { count: 4, label: 'read' })
          const historical = some(
            yield* Session.materializedSnapshotAsOf(
              session,
              serviceDocument,
              target,
              cutoff.id,
            ).pipe(Effect.provideService(Labels, 'history')),
          )
          assert.strictEqual(historical.value.label, 'history')
          const pull = yield* Session.watchMaterialized(session, serviceDocument, target).pipe(
            Stream.rechunk(1),
            Stream.toPull,
            Effect.provideService(Labels, 'watch'),
          )
          assert.strictEqual((yield* pull)[0].value.label, 'watch')
          yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, current, target))
          assert.deepEqual((yield* pull)[0].value, { count: 4, label: 'watch' })
        }),
      ),
  )

  it.effect(
    'Conversation local runtime bridge checks ownership and supports current/history/watch',
    () => {
      const model = Layer.effect(
        LanguageModel.LanguageModel,
        ScriptedStream.make({
          generateText: () => Effect.succeed([]),
        }),
      )
      return withMemory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          class CallIO extends Context.Service<CallIO, boolean>()('test/materialization/CallIO') {}
          const count = (increment: () => void) =>
            Effect.serviceOption(CallIO).pipe(
              Effect.map((probe) => {
                if (Option.isSome(probe)) increment()
              }),
            )
          let reads = 0
          let writes = 0
          const format = yield* StorageFormat
          const counted = {
            ...storage,
            document: (...args: Parameters<typeof storage.document>) =>
              count(() => {
                reads++
              }).pipe(Effect.andThen(storage.document(...args))),
            findDocument: (...args: Parameters<typeof storage.findDocument>) =>
              count(() => {
                reads++
              }).pipe(Effect.andThen(storage.findDocument(...args))),
            entry: (...args: Parameters<typeof storage.entry>) =>
              count(() => {
                reads++
              }).pipe(Effect.andThen(storage.entry(...args))),
            commit: (changes: Iterable<Record.StorageWrite>) =>
              count(() => {
                writes++
              }).pipe(Effect.andThen(storage.commit(changes))),
          } satisfies Storage.Storage['Service']
          const runtimeScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void))
          const runtime = yield* HarnessRuntime.make().pipe(
            Effect.provideService(Scope.Scope, runtimeScope),
            Effect.provideService(Storage.Storage, counted),
            Effect.provideService(StorageFormat, { ...format, storage: counted }),
          )
          const harness = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, runtime.backend),
          )
          const conversation = yield* harness.root
          const cutoff = yield* initialize(runtime.session)
          const userWatchers = new Set<SessionRuntime.Subscriber>()
          const run = Effect.gen(function* () {
            const result = some(
              yield* conversation.pipe(Conversation.materializedSnapshot(serviceDocument, target)),
            )
            assert.deepEqual(result.value, { count: 4, label: 'client' })
            assert.strictEqual(result.storedVersion, 1)
            assert.strictEqual(
              some(
                yield* Conversation.materializedSnapshotAsOf(
                  conversation,
                  current,
                  target,
                  cutoff.id,
                ),
              ).storedVersion,
              1,
            )
            const pull = yield* conversation.pipe(
              Conversation.watchMaterialized(current, target),
              Stream.rechunk(1),
              Stream.toPull,
            )
            yield* pull
            for (const subscriber of (yield* SessionRuntime.get(runtime.session)).subscribers) {
              if (subscriber.documentId !== undefined) userWatchers.add(subscriber)
            }
            assert.strictEqual(userWatchers.size, 1)
            yield* Session.commit(runtime.session, (tx) =>
              Transaction.ensureDocument(tx, current, target),
            )
            assert.strictEqual((yield* pull)[0].storedVersion, 3)
          })
          yield* Effect.scoped(run).pipe(
            Effect.provideService(CallIO, true),
            Effect.provide([
              Layer.succeed(HarnessRuntime.HarnessRuntime, runtime),
              Layer.succeed(Labels, 'client'),
            ]),
          )
          const sessionState = yield* SessionRuntime.get(runtime.session)
          for (const subscriber of userWatchers)
            assert.isFalse(sessionState.subscribers.has(subscriber))
          assert.isTrue(reads > 0)
          assert.isTrue(writes > 0)
          reads = 0
          writes = 0
          let sessionAccesses = 0
          const wrong = {
            backend: { ...runtime.backend },
            get session() {
              sessionAccesses++
              return runtime.session
            },
          }
          const denied = yield* Conversation.materializedSnapshot(
            conversation,
            current,
            target,
          ).pipe(
            Effect.provideService(CallIO, true),
            Effect.provideService(HarnessRuntime.HarnessRuntime, wrong),
            Effect.flip,
          )
          assert.strictEqual(denied._tag, 'HarnessError')
          const historyDenied = yield* Conversation.materializedSnapshotAsOf(
            conversation,
            current,
            target,
            cutoff.id,
          ).pipe(
            Effect.provideService(CallIO, true),
            Effect.provideService(HarnessRuntime.HarnessRuntime, wrong),
            Effect.flip,
          )
          assert.strictEqual(historyDenied._tag, 'HarnessError')
          const watchDenied = yield* Conversation.watchMaterialized(
            conversation,
            current,
            target,
          ).pipe(
            Stream.runCollect,
            Effect.provideService(CallIO, true),
            Effect.provideService(HarnessRuntime.HarnessRuntime, wrong),
            Effect.flip,
          )
          assert.strictEqual(watchDenied._tag, 'HarnessError')
          assert.strictEqual(sessionAccesses, 0)
          assert.strictEqual(reads, 0)
          assert.strictEqual(writes, 0)
          for (const subscriber of userWatchers)
            assert.isFalse(sessionState.subscribers.has(subscriber))
          yield* Scope.close(runtimeScope, Exit.void)
          assert.strictEqual(sessionState.subscribers.size, 0)
        }).pipe(Effect.provide(model)),
      )
    },
  )
})
