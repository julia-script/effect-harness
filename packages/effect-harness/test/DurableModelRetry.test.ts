import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Clock from 'effect/Clock'
import * as Duration from 'effect/Duration'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as AiError from 'effect/ai/AiError'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as LiveState from 'effect-harness/LiveState'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const rate = AiError.make({
  module: 'fixture',
  method: 'streamText',
  reason: new AiError.RateLimitError({
    retryAfter: Duration.millis(250),
    metadata: { usage: { promptTokens: 3, completionTokens: 1, totalTokens: 4 } },
  }),
})
const answer: Response.StreamPartEncoded[] = [
  { type: 'text-start', id: 't' },
  { type: 'text-delta', id: 't', delta: 'done' },
  { type: 'text-end', id: 't' },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 2, uncached: 2, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 1, text: 1, reasoning: undefined },
    },
    response: undefined,
  },
]
const retry = (runtime: HarnessRuntime.HarnessRuntimeService, at?: number) =>
  Session.watch(runtime.session, LiveState.document, LiveState.target(root)).pipe(
    Stream.filter(
      (value) =>
        value.value.generation?.retryAt !== undefined &&
        (at === undefined || value.value.generation.retryAt === at),
    ),
    Stream.take(1),
    Stream.runHead,
  )

it.live(
  'finite durable budget remains separate from the captured native transport retry budget',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const policy = yield* Settings.make({
          retry: { maxRetries: 2, baseDelayMs: 0, maxAgentDelayMs: 0 },
          stream: { maxRetries: 99 },
        })
        const transient = AiError.make({
          module: 'fixture',
          method: 'streamText',
          reason: new AiError.InternalProviderError({ description: 'transport still failing' }),
        })
        let calls = 0
        let transportCalls = 0
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('nonstreaming'),
          streamText: () => {
            calls++
            const transport = Effect.suspend(() => {
              transportCalls++
              return Effect.fail(transient)
            }).pipe(Effect.retry({ times: 1 }))
            return Stream.fromEffect(transport)
          },
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
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
        assert.strictEqual(calls, 3)
        assert.strictEqual(transportCalls, 6)
        assert.strictEqual((yield* Session.usage(runtime.session, root)).unknownAttempts, 3)
        const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
        assert.deepEqual(
          receipts.map((receipt) =>
            receipt.identity._tag === 'requestAttempt' ? receipt.identity.attempt : -1,
          ),
          [0, 1, 2],
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect('abort clears a saved retry deadline and prevents the next outbound attempt', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      let calls = 0
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('nonstreaming'),
        streamText: () =>
          Stream.unwrap(
            Deferred.succeed(entered, undefined).pipe(
              Effect.map(() => {
                calls++
                return Stream.fail(rate)
              }),
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
      yield* Deferred.await(entered)
      yield* retry(runtime)
      yield* runtime.backend.abort(root)
      const report = yield* Session.usage(runtime.session, root)
      yield* TestClock.adjust(60000)
      yield* runtime.backend.wait(job.id)
      assert.strictEqual(calls, 1)
      assert.deepEqual(yield* Session.usage(runtime.session, root), report)
      assert.deepEqual(
        (yield* Session.snapshot(runtime.session, LiveState.document, LiveState.target(root))).pipe(
          Option.map((value) => value.value),
        ),
        Option.some({}),
      )
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

for (const [cap, expected] of [
  [100, 100],
  [0, 250],
] as const)
  it.effect(`provider hint cap ${cap} saves a finite absolute deadline`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const policy = yield* Settings.make({
          retry: { baseDelayMs: 0, maxAgentDelayMs: 500 },
          stream: { maxRetryDelayMs: cap },
        })
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('nonstreaming'),
          streamText: () =>
            Stream.unwrap(Deferred.succeed(entered, undefined).pipe(Effect.as(Stream.fail(rate)))),
        })
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provideService(Settings.Settings, policy),
        )
        yield* runtime.backend.root
        const now = yield* Clock.currentTimeMillis
        yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        yield* Deferred.await(entered)
        yield* retry(runtime)
        const saved = yield* Schema.decodeUnknownEffect(checkpoint)(
          (yield* task(runtime)).state.checkpoint,
        )
        assert.strictEqual(saved.until, now + expected)
        yield* runtime.backend.abort(root)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

it.live('an infinite provider retry hint fails once without persisting an infinite deadline', () =>
  Effect.scoped(
    Effect.gen(function* () {
      let calls = 0
      const invalidHint = AiError.make({
        module: 'fixture',
        method: 'streamText',
        reason: new AiError.RateLimitError({ retryAfter: Duration.infinity }),
      })
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('nonstreaming'),
        streamText: () => {
          calls++
          return Stream.fail(invalidHint)
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
      assert.strictEqual((yield* Session.usage(runtime.session, root)).unknownAttempts, 1)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
const task = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanTasks(runtime.session, { kind: 'harness.run' }).pipe(
    Stream.runCollect,
    Effect.map((tasks) => tasks[0]!),
  )
const checkpoint = Schema.Struct({
  phase: Schema.String,
  until: Schema.Finite,
  attempt: Usage.AttemptId,
  retries: Schema.Finite,
  preparation: Schema.toCodecJson(Prepared.SnapshotSchema),
})
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sqlite = Storage.layerJournal().pipe(
    Layer.provide(SqlEventJournal.layer()),
    Layer.provide(SqliteClient.layer({ filename: `${directory}/retry.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/retry.jsonl` })
  return (backend: 'memory' | 'sqlite' | 'jsonl') =>
    backend === 'memory'
      ? Effect.succeed(memory)
      : Layer.build(Layer.fresh(backend === 'sqlite' ? sqlite : jsonl))
})
for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`${backend} durable retry TestClock`, () => {
    for (const expired of [false, true])
      it.effect(
        `cold reopen ${expired ? 'after' : 'before'} saved deadline preserves pinned request and known usage`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* fixture
              const policy = yield* Settings.make({
                retry: { maxRetries: 2, baseDelayMs: 100, maxAgentDelayMs: 200 },
                progress: { partialIntervalMs: 0 },
              })
              const entered = yield* Deferred.make<void>()
              const requests: Prepared.Snapshot[] = []
              let calls = 0
              const model = yield* LanguageModel.make({
                generateText: () => Effect.die('nonstreaming'),
                streamText: () =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      yield* Deferred.succeed(entered, undefined)
                      requests.push(
                        Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
                          .current,
                      )
                      calls++
                      return calls === 1 ? Stream.fail(rate) : Stream.fromIterable(answer)
                    }),
                  ),
              })
              const open = Effect.gen(function* () {
                const context = yield* store(backend)
                return yield* HarnessRuntime.make().pipe(
                  Effect.provideContext(context),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provideService(Settings.Settings, policy),
                )
              })
              const owner = yield* Scope.make()
              const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
              yield* first.backend.root
              const admittedAt = yield* Clock.currentTimeMillis
              const job = yield* first.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'go' },
              })
              yield* Deferred.await(entered)
              yield* retry(first)
              const saved = yield* Schema.decodeUnknownEffect(checkpoint)(
                (yield* task(first)).state.checkpoint,
              )
              assert.strictEqual(saved.phase, 'retry')
              assert.strictEqual(saved.until, admittedAt + 200)
              assert.strictEqual(saved.attempt, 1)
              assert.strictEqual(saved.retries, 1)
              assert.deepEqual(saved.preparation, requests[0])
              assert.strictEqual(
                (yield* Session.usage(first.session, root)).models[0]?.tokens.input,
                3,
              )
              yield* Scope.close(owner, Exit.void)
              yield* policy.update({ retry: { enabled: false, baseDelayMs: 9999 } })
              if (expired) yield* TestClock.adjust(201)
              const second = yield* open
              const waiter = yield* second.backend.wait(job.id).pipe(Effect.forkScoped)
              if (!expired) {
                yield* TestClock.adjust(199)
                assert.strictEqual(calls, 1)
                assert.deepEqual(
                  (yield* Schema.decodeUnknownEffect(checkpoint)(
                    (yield* task(second)).state.checkpoint,
                  )).preparation,
                  saved.preparation,
                )
                yield* TestClock.adjust(1)
              }
              assert.strictEqual((yield* Fiber.join(waiter)).status, 'done')
              assert.strictEqual(calls, 2)
              assert.deepEqual(requests[1], requests[0])
              const report = yield* Session.usage(second.session, root)
              assert.strictEqual(report.models[0]?.responses, 2)
              assert.strictEqual(report.models[0]?.tokens.input, 5)
              const receipts = yield* Session.commit(second.session, (tx) =>
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
                ['failed', 'completed'],
              )
              yield* second.backend.wait(job.id)
              assert.strictEqual(calls, 2)
              assert.deepEqual(yield* Session.usage(second.session, root), report)
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )
  })

it.effect('finite capped exponential retries use live policy only for new decisions', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const policy = yield* Settings.make({
        retry: { maxRetries: 2, baseDelayMs: 10, maxAgentDelayMs: 100 },
      })
      let calls = 0
      const entered = yield* Deferred.make<void>()
      const transient = AiError.make({
        module: 'fixture',
        method: 'streamText',
        reason: new AiError.InternalProviderError({ description: 'temporary' }),
      })
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('nonstreaming'),
        streamText: () =>
          Stream.unwrap(
            Deferred.succeed(entered, undefined).pipe(
              Effect.map(() => {
                calls++
                return Stream.fail(transient)
              }),
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
      yield* retry(runtime)
      yield* TestClock.adjust(10)
      yield* retry(runtime, 30)
      assert.strictEqual(calls, 2)
      yield* policy.update({ retry: { enabled: false } })
      yield* TestClock.adjust(20)
      assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
      assert.strictEqual(calls, 3)
      assert.strictEqual((yield* Session.usage(runtime.session, root)).unknownAttempts, 3)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

for (const reason of [
  new AiError.AuthenticationError({ kind: 'InvalidKey' }),
  new AiError.QuotaExhaustedError({}),
  new AiError.InvalidRequestError({ description: 'context overflow', parameter: 'input' }),
  new AiError.UnknownError({ description: 'not a transient type' }),
])
  it.live(`never retries ${reason._tag}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        let calls = 0
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('nonstreaming'),
          streamText: () => {
            calls++
            return Stream.fail(AiError.make({ module: 'fixture', method: 'streamText', reason }))
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
        assert.strictEqual((yield* Session.usage(runtime.session, root)).unknownAttempts, 1)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
