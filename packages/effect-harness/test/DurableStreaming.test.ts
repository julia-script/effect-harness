import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as LiveState from 'effect-harness/LiveState'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 2, uncached: 2, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const answer: Response.StreamPartEncoded[] = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'done' },
  { type: 'text-end', id: 'answer' },
  finish,
]
const entries = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root, order: 'ascending' }).pipe(
    Stream.runCollect,
  )
const partial = (runtime: HarnessRuntime.HarnessRuntimeService, knownUsage = false) =>
  Session.watch(runtime.session, LiveState.document, LiveState.target(root)).pipe(
    Stream.filter(
      (value) =>
        value.value.generation?.partial !== undefined &&
        (!knownUsage || value.value.generation.partial.usage !== undefined),
    ),
    Stream.take(1),
    Stream.runHead,
  )
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sqlite = Storage.layerJournal().pipe(
    Layer.provide(SqlEventJournal.layer()),
    Layer.provide(SqliteClient.layer({ filename: `${directory}/stream.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/stream.jsonl` })
  return (backend: 'memory' | 'sqlite' | 'jsonl') =>
    backend === 'memory'
      ? Effect.succeed(memory)
      : Layer.build(Layer.fresh(backend === 'sqlite' ? sqlite : jsonl))
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`${backend} native durable streaming`, () => {
    for (const committed of [false, true, 'known'] as const) {
      const location = committed ? 'after partial' : 'before partial'
      it.live(
        `cold reopen ${committed === 'known' ? 'after known final usage' : location} resends pinned request once`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* fixture
              const entered = yield* Deferred.make<void>()
              const policy = yield* Settings.make({ progress: { partialIntervalMs: 0 } })
              let calls = 0
              const requests: Prepared.Snapshot[] = []
              const model = yield* LanguageModel.make({
                generateText: () => Effect.die('Runtime must not call generateText'),
                streamText: () =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      calls++
                      requests.push(
                        Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
                          .current,
                      )
                      if (calls > 1) return Stream.fromIterable(answer)
                      yield* Deferred.succeed(entered, undefined)
                      let parts: Response.StreamPartEncoded[] = committed
                        ? [
                            { type: 'text-start', id: 't' },
                            { type: 'text-delta', id: 't', delta: 'saved text' },
                            { type: 'reasoning-start', id: 'r' },
                            { type: 'reasoning-delta', id: 'r', delta: 'saved reasoning' },
                            {
                              type: 'tool-params-start',
                              id: 'tool',
                              name: 'neverInvoke',
                              providerExecuted: false,
                            },
                            { type: 'tool-params-delta', id: 'tool', delta: '{"unfinished":' },
                          ]
                        : []
                      if (committed === 'known') parts = answer
                      return Stream.concat(
                        Stream.fromIterable(parts),
                        Stream.fromEffect(Effect.never),
                      )
                    }),
                  ),
              })
              const open = Effect.gen(function* () {
                const context = yield* store(backend)
                const runtime = yield* HarnessRuntime.make().pipe(
                  Effect.provideContext(context),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provideService(Settings.Settings, policy),
                )
                return { context, runtime }
              })
              const owner = yield* Scope.make()
              const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
              yield* first.runtime.backend.root
              const job = yield* first.runtime.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'go' },
              })
              yield* Deferred.await(entered)
              if (committed) {
                yield* partial(first.runtime, committed === 'known')
                const live = yield* Session.snapshot(
                  first.runtime.session,
                  LiveState.document,
                  LiveState.target(root),
                )
                assert.isTrue(Option.isSome(live))
                if (committed === true && Option.isSome(live))
                  assert.strictEqual(
                    live.value.value.generation?.partial?.parameters[0]?.text,
                    '{"unfinished":',
                  )
              }
              assert.strictEqual(
                (yield* StorageFormat.inspect.pipe(Effect.provideContext(first.context)))
                  .minimumReaderVersion,
                12,
              )
              yield* Scope.close(owner, Exit.void)
              const second = yield* open
              assert.strictEqual((yield* second.runtime.backend.wait(job.id)).status, 'done')
              const facts = yield* entries(second.runtime)
              assert.strictEqual(
                facts.filter((entry) => entry.kind === 'assistant.interrupted').length,
                committed ? 1 : 0,
              )
              assert.strictEqual(facts.filter((entry) => entry.kind === 'assistant').length, 1)
              assert.strictEqual(facts.filter((entry) => entry.kind === 'tool.result').length, 0)
              assert.strictEqual(calls, 2)
              assert.deepEqual(requests[1], requests[0])
              assert.deepEqual(
                (yield* Session.snapshot(
                  second.runtime.session,
                  LiveState.document,
                  LiveState.target(root),
                )).pipe(Option.map((value) => value.value)),
                Option.some({}),
              )
              assert.strictEqual(
                (yield* Session.usage(second.runtime.session, root)).unknownAttempts,
                committed === 'known' ? undefined : 1,
              )
              const receipts = yield* Session.commit(second.runtime.session, (tx) =>
                Usage.receipts(tx, root),
              )
              assert.deepEqual(
                receipts.map((receipt) =>
                  receipt.identity._tag === 'requestAttempt' ? receipt.identity.attempt : -1,
                ),
                [0, 1],
              )
              assert.deepEqual(
                receipts.map((receipt) => receipt.outcome._tag),
                ['interrupted', 'completed'],
              )
              if (committed === 'known') {
                const report = yield* Session.usage(second.runtime.session, root)
                assert.strictEqual(report.models[0]?.responses, 2)
                assert.strictEqual(report.models[0]?.tokens.input, 4)
                assert.strictEqual(
                  facts.find((entry) => entry.kind === 'assistant.interrupted')?.usage?._tag,
                  'model',
                )
                assert.isDefined(receipts[0]?.linkedEntryId)
              }
              assert.strictEqual((yield* second.runtime.backend.wait(job.id)).status, 'done')
              assert.strictEqual(calls, 2)
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )
    }

    it.live('committed terminal never repeats after a fresh runtime', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          let calls = 0
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('nonstreaming'),
            streamText: () => {
              calls++
              return Stream.fromIterable(answer)
            },
          })
          const firstOwner = yield* Scope.make()
          const open = Effect.gen(function* () {
            const context = yield* store(backend)
            return yield* HarnessRuntime.make().pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
          })
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstOwner))
          yield* first.backend.root
          const job = yield* first.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          yield* first.backend.wait(job.id)
          yield* Scope.close(firstOwner, Exit.void)
          const second = yield* open
          yield* second.backend.wait(job.id)
          assert.strictEqual(calls, 1)
          assert.strictEqual(
            (yield* entries(second)).filter((entry) => entry.kind === 'assistant').length,
            1,
          )
          assert.strictEqual((yield* Session.usage(second.session, root)).models[0]?.responses, 1)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
  })

it.effect('TestClock throttles partials and flushes trailing text before terminal', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const ready = yield* Deferred.make<void>()
      const end = yield* Deferred.make<void>()
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('nonstreaming'),
        streamText: () =>
          Stream.concat(
            Stream.fromIterable(answer.slice(0, 2)),
            Stream.unwrap(
              Deferred.succeed(ready, undefined).pipe(
                Effect.andThen(Deferred.await(end)),
                Effect.as(Stream.fromIterable(answer.slice(2))),
              ),
            ),
          ),
      })
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      yield* runtime.backend.root
      const job = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'go' },
      })
      yield* Deferred.await(ready)
      yield* TestClock.adjust(99)
      const before = yield* Session.snapshot(
        runtime.session,
        LiveState.document,
        LiveState.target(root),
      )
      assert.isTrue(Option.isSome(before))
      if (Option.isSome(before)) assert.strictEqual(before.value.value.generation?.revision, 0)
      yield* TestClock.adjust(1)
      yield* partial(runtime)
      yield* Deferred.succeed(end, undefined)
      yield* runtime.backend.wait(job.id)
      assert.deepEqual(
        (yield* Session.snapshot(runtime.session, LiveState.document, LiveState.target(root))).pipe(
          Option.map((value) => value.value),
        ),
        Option.some({}),
      )
      assert.strictEqual(
        (yield* entries(runtime)).filter((entry) => entry.kind === 'assistant').length,
        1,
      )
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

for (const parts of [
  [],
  answer.slice(0, 2),
  [{ type: 'tool-params-start', id: 'c', name: 'unfinished' }, finish],
] as ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>)
  it.live('incomplete or empty stream fails without a second provider call', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('no fallback'),
          streamText: () => {
            calls++
            return Stream.fromIterable(parts)
          },
        })
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
        )
        yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
        assert.strictEqual(calls, 1)
        assert.strictEqual(
          (yield* entries(runtime)).filter(
            (entry) => entry.kind === 'assistant' || entry.kind === 'tool.result',
          ).length,
          0,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

it.effect('abort releases the native stream and fences trailing progress and terminal output', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const released = yield* Deferred.make<void>()
      const end = yield* Deferred.make<void>()
      let calls = 0
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('nonstreaming'),
        streamText: () =>
          Stream.unwrap(
            Effect.gen(function* () {
              calls++
              yield* Effect.addFinalizer(() => Deferred.succeed(released, undefined))
              return Stream.concat(
                Stream.fromIterable(answer.slice(0, 2)),
                Stream.unwrap(
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(end)),
                    Effect.as(Stream.fromIterable(answer.slice(2))),
                  ),
                ),
              )
            }),
          ),
      })
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      yield* runtime.backend.root
      const job = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'go' },
      })
      yield* Deferred.await(entered)
      yield* TestClock.adjust(100)
      yield* partial(runtime)
      yield* runtime.backend.abort(root)
      yield* Deferred.await(released)
      const before = yield* entries(runtime)
      const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
      assert.strictEqual(receipts.length, 1)
      assert.strictEqual(receipts[0]?.outcome._tag, 'interrupted')
      yield* Deferred.succeed(end, undefined)
      yield* TestClock.adjust(10000)
      yield* runtime.backend.wait(job.id)
      assert.deepEqual(yield* entries(runtime), before)
      assert.deepEqual(
        yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
        receipts,
      )
      assert.deepEqual(
        (yield* Session.snapshot(runtime.session, LiveState.document, LiveState.target(root))).pipe(
          Option.map((value) => value.value),
        ),
        Option.some({}),
      )
      assert.strictEqual(calls, 1)
      assert.strictEqual(before.filter((entry) => entry.kind === 'assistant').length, 0)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect('throttle intervals beyond host timer bounds do not publish early', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const end = yield* Deferred.make<void>()
      const policy = yield* Settings.make({ progress: { partialIntervalMs: 0x7fffffff + 100 } })
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('nonstreaming'),
        streamText: () =>
          Stream.concat(
            Stream.fromIterable(answer.slice(0, 2)),
            Stream.unwrap(
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(end)),
                Effect.as(Stream.fromIterable(answer.slice(2))),
              ),
            ),
          ),
      })
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
        Effect.provideService(Settings.Settings, policy),
      )
      yield* runtime.backend.root
      const job = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'go' },
      })
      yield* Deferred.await(entered)
      yield* TestClock.adjust(0x7fffffff)
      const before = yield* Session.snapshot(
        runtime.session,
        LiveState.document,
        LiveState.target(root),
      )
      if (Option.isSome(before)) assert.strictEqual(before.value.value.generation?.revision, 0)
      else assert.fail('Stream marker was not committed')
      yield* TestClock.adjust(100)
      yield* partial(runtime)
      yield* Deferred.succeed(end, undefined)
      yield* runtime.backend.wait(job.id)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
