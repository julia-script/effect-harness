import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Document from 'effect-harness/Document'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Remote from 'effect-harness/RemoteDocument'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Native from '../src/internal/SessionRuntime.js'
const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const document = Document.define({
  kind: 'remote.native',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
const fixture = Effect.gen(function* () {
  const model = yield* LanguageModel.make({
    generateText: () => Effect.die('No provider'),
    streamText: () => Stream.die('No provider'),
  })
  const runtime = yield* HarnessRuntime.make().pipe(
    Effect.provideService(LanguageModel.LanguageModel, model),
  )
  const harness = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
  const conversation = yield* harness.root
  yield* Session.commit(runtime.session, (tx) => Transaction.ensureDocument(tx, document, target))
  return { runtime, conversation }
})
const run = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))
describe('remote document native seam', () => {
  it.effect('missing capabilities fail before document, target and option getters', () =>
    run(
      Effect.gen(function* () {
        const { runtime } = yield* fixture
        const harness = yield* Harness.make.pipe(
          Effect.provideService(HarnessBackend, Struct.omit(runtime.backend, ['documents'])),
        )
        const conversation = yield* harness.root
        let reads = 0
        const bad = {
          ...document,
          get definition() {
            reads++
            return document.definition
          },
        }
        const badTarget = {
          get scope() {
            reads++
            return target.scope
          },
        }
        const options = {
          get capacity() {
            reads++
            return 1
          },
        }
        const current = yield* Remote.snapshot(conversation, bad, badTarget).pipe(Effect.flip)
        const history = yield* Remote.snapshotAsOf(
          conversation,
          bad,
          badTarget,
          Record.EntryId.make(1),
        ).pipe(Effect.flip)
        const watch = yield* Remote.watch(conversation, bad, badTarget, options).pipe(
          Stream.runDrain,
          Effect.flip,
        )
        for (const error of [current, history, watch]) {
          assert.strictEqual(error._tag, 'RemoteDocumentError')
          if (error._tag === 'RemoteDocumentError') assert.strictEqual(error.reason, 'unsupported')
        }
        assert.strictEqual(reads, 0)
      }),
    ),
  )
  it.effect('initial capture and subscription precede a queued concurrent writer', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture
        const native = yield* Native.get(runtime.session)
        native.cache.clear()
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const original = native.storage.document
        const storage = {
          ...native.storage,
          document: (...args: Parameters<typeof original>) =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(original(...args)),
            ),
        }
        Object.assign(native, { storage })
        const seen = yield* Deferred.make<void>()
        const values: number[] = []
        const watching = yield* Remote.watch(conversation, document, target).pipe(
          Stream.tap((frame) => Effect.sync(() => values.push(frame.value.count))),
          Stream.take(2),
          Stream.runDrain,
          Effect.tap(() => Deferred.succeed(seen, undefined)),
          Effect.forkChild,
        )
        yield* Deferred.await(entered)
        let enteredWrite = false
        const writer = yield* Session.commit(runtime.session, (tx) =>
          Effect.sync(() => {
            enteredWrite = true
          }).pipe(Effect.andThen(Transaction.setDocument(tx, document, target, { count: 7 }))),
        ).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        assert.isFalse(enteredWrite)
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(writer)
        yield* Deferred.await(seen)
        yield* Fiber.join(watching)
        assert.deepEqual(values, [0, 7])
        assert.strictEqual(native.documentMounts.size, 0)
      }),
    ),
  )
  it.effect('retire and recreate terminate the old incarnation and require a new mount', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture
        const pull = yield* Remote.watchFrames(conversation, document, target).pipe(
          Stream.rechunk(1),
          Stream.toPull,
        )
        const initial = (yield* pull)[0]
        const oldId = initial.incarnation
        yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.retireDocument(tx, document, target)
            yield* Transaction.ensureDocument(tx, document, target)
          }),
        )
        const retired = (yield* pull)[0]
        assert.strictEqual(retired._tag, 'Retired')
        assert.strictEqual(retired.incarnation, oldId)
        assert.isTrue(Exit.isFailure(yield* pull.pipe(Effect.exit)))
        const fresh = yield* Remote.watchFrames(conversation, document, target).pipe(Stream.runHead)
        assert.isTrue(Option.isSome(fresh))
        if (Option.isSome(fresh)) {
          assert.notStrictEqual(fresh.value.incarnation, oldId)
          assert.notStrictEqual(fresh.value.epoch, initial.epoch)
        }
      }),
    ),
  )
  it.effect('overflow followed by retirement remains reducible and final on the old mount', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture
        const pull = yield* Remote.watchFrames(conversation, document, target, {
          capacity: 1,
        }).pipe(Stream.rechunk(1), Stream.toPull)
        const initial = (yield* pull)[0]
        let state = yield* Remote.reduce(undefined, initial)
        for (let count = 1; count < 5; count++)
          yield* Session.commit(runtime.session, (tx) =>
            Transaction.setDocument(tx, document, target, { count }),
          )
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.retireDocument(tx, document, target),
        )
        const retired = (yield* pull)[0]
        state = yield* Remote.reduce(state, retired)
        assert.isTrue(Option.isNone(state.value))
        assert.strictEqual(retired._tag, 'Retired')
      }),
    ),
  )
  it.effect('reducer rejects foreign epoch, incarnation, address and discontinuous cursors', () =>
    run(
      Effect.gen(function* () {
        const { conversation } = yield* fixture
        const frame = Option.getOrThrow(
          yield* Remote.watchFrames(conversation, document, target).pipe(Stream.runHead),
        )
        if (frame._tag !== 'Snapshot') return assert.fail('Expected initial Snapshot')
        const state = yield* Remote.reduce(undefined, frame)
        const next = { ...frame, _tag: 'Replace' as const, from: 0, to: 1 }
        for (const invalid of [
          frame,
          { ...next, epoch: 'foreign' },
          { ...next, incarnation: Record.DocumentId.make(999) },
          { ...next, target: { ...target, kind: 'foreign' } },
          { ...next, from: 1, to: 2 },
          { ...next, to: 4 },
          { ...frame, _tag: 'Rebase' as const },
        ])
          assert.strictEqual(
            (yield* Remote.reduce(state, invalid).pipe(Effect.flip)).reason,
            'continuity',
          )
        assert.strictEqual((yield* Remote.reduce(state, next)).revision, 1)
      }),
    ),
  )
  it.effect(
    'malformed migration and future stored versions fail without poisoning native readers',
    () =>
      run(
        Effect.gen(function* () {
          const { runtime, conversation } = yield* fixture
          const invalid = Document.define({
            ...document.definition,
            version: 3,
            migrations: { 1: () => Effect.succeed({ count: -1 }) },
          })
          assert.strictEqual(
            (yield* Remote.snapshot(conversation, invalid, target).pipe(Effect.flip))._tag,
            'SchemaError',
          )
          const missing = Document.define({ ...document.definition, version: 3 })
          assert.strictEqual(
            (yield* Remote.snapshot(conversation, missing, target).pipe(Effect.flip))._tag,
            'SessionError',
          )
          assert.deepEqual(
            Option.getOrThrow(yield* Session.snapshot(runtime.session, document, target)).value,
            { count: 0 },
          )
          assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
        }),
      ),
  )
  it.effect('consumer Scope cancellation removes the raw mount exactly once', () =>
    run(
      Effect.gen(function* () {
        const { runtime, conversation } = yield* fixture
        const scope = yield* Scope.make()
        const pull = yield* Remote.watch(conversation, document, target).pipe(
          Stream.toPull,
          Effect.provideService(Scope.Scope, scope),
        )
        yield* pull
        assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 1)
        yield* Scope.close(scope, Exit.void)
        assert.strictEqual((yield* Native.get(runtime.session)).documentMounts.size, 0)
      }),
    ),
  )
})
