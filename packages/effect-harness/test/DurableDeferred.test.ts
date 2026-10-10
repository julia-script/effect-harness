import * as SchemaGetter from 'effect/SchemaGetter'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as NativeOpenAiConfig from '@effect/ai-openai/OpenAiConfig'
import * as NativeOpenAiModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import type * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as DeferredResponse from 'effect-harness/provider-openai/DeferredResponse'
import * as OwnedOpenAiModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Deferred from 'effect/Deferred'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as AiError from 'effect/ai/AiError'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import { StorageError } from 'effect-harness/StorageError'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const complete: Model.Complete = {
  _tag: 'Complete',
  response: new LanguageModel.GenerateTextResponse([
    Response.makePart('text', { text: 'done' }),
    Response.makePart('finish', {
      reason: 'stop',
      usage: {
        inputTokens: { total: 13, uncached: 13, cacheRead: undefined, cacheWrite: undefined },
        outputTokens: { total: 7, text: 7, reasoning: undefined },
      },
    }),
  ]),
}
const model = (deferred: Model.Deferred) =>
  Effect.map(
    LanguageModel.make({
      generateText: () => Effect.die('must use deferred descriptor'),
      streamText: () => Stream.die('must use deferred descriptor'),
    }),
    (languageModel) =>
      Model.make({
        definition: {
          ref: { provider: 'custom', modelId: 'background' },
          capabilities: { tools: true, reasoning: true, images: false, structuredOutput: false },
        },
        options: Schema.JsonObject,
        configure: () => Effect.succeed(Context.empty()),
        languageModel,
        deferred,
      }),
  )

it.live('saves a handle and polls the same prepared attempt rather than streaming', () =>
  Effect.scoped(
    Effect.gen(function* () {
      let starts = 0
      let polls = 0
      const ids: Model.DeferredIdentity[] = []
      const adapter = yield* Model.makeDeferred<Schema.String, never>({
        handle: Schema.String,
        start: (request) =>
          Effect.gen(function* () {
            starts++
            ids.push({ requestId: request.requestId, attempt: request.attempt })
            return {
              _tag: 'Deferred' as const,
              handle: 'same-handle',
              nextPollAt: (yield* Clock.currentTimeMillis) + 1,
            }
          }),
        poll: (handle, _previous, request) =>
          Effect.sync(() => {
            polls++
            assert.strictEqual(handle, 'same-handle')
            ids.push({ requestId: request.requestId, attempt: request.attempt })
            return complete
          }),
        cancel: () => Effect.void,
      })
      const policy = yield* Settings.make({ stream: { deferred: true } })
      const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
        Effect.provideService(Settings.Settings, policy),
      )
      yield* runtime.backend.root
      const job = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'go' },
      })
      assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
      assert.strictEqual(starts, 1)
      assert.strictEqual(polls, 1)
      assert.deepEqual(ids[1], ids[0])
      const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
      assert.strictEqual(receipts.length, 1)
      assert.strictEqual(receipts[0]?.usage?._tag, 'model')
      assert.strictEqual((yield* Session.usage(runtime.session, root)).models[0]?.tokens.input, 13)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

const entries = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root }).pipe(Stream.runCollect)
const saved = Effect.fnUntraced(function* (
  runtime: HarnessRuntime.HarnessRuntimeService,
  tag: string,
  failures?: number,
  pollAt?: number,
) {
  for (;;) {
    const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
    for (const task of tasks) {
      const value = Schema.decodeUnknownOption(Schema.JsonObject)(task.state.checkpoint)
      if (
        Option.isSome(value) &&
        value.value['phase'] === tag &&
        (failures === undefined || value.value['pollFailures'] === failures) &&
        (pollAt === undefined || value.value['pollAt'] === pollAt)
      )
        return { task, checkpoint: value.value }
    }
    yield* Effect.yieldNow
  }
})
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sqlite = Storage.layerSql.pipe(
    Layer.provide(SqliteClient.layer({ filename: `${directory}/deferred.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/deferred.jsonl` })
  return (backend: 'memory' | 'sqlite' | 'jsonl') =>
    backend === 'memory'
      ? Effect.succeed(memory)
      : Layer.build(Layer.fresh(backend === 'sqlite' ? sqlite : jsonl))
})
const submit = Effect.fnUntraced(function* (runtime: HarnessRuntime.HarnessRuntimeService) {
  yield* runtime.backend.root
  return yield* runtime.backend.submit({
    conversationId: root,
    draft: { type: 'input', content: 'go' },
  })
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`${backend} durable deferred recovery`, () => {
    it.effect(
      'cold reopen pins handle, deadlines, identity and configuration without another create',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const policy = yield* Settings.make({ stream: { deferred: { window: '1h' } } })
            const identities: Model.DeferredIdentity[] = []
            let starts = 0
            let polls = 0
            let cancels = 0
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: (request) =>
                Effect.sync(() => {
                  starts++
                  identities.push({ requestId: request.requestId, attempt: request.attempt })
                  return {
                    _tag: 'Deferred' as const,
                    handle: 'same',
                    nextPollAt: 100,
                    expiresAt: 500,
                  }
                }),
              poll: (handle, previous, request) =>
                Effect.sync(() => {
                  polls++
                  assert.strictEqual(handle, 'same')
                  identities.push({ requestId: request.requestId, attempt: request.attempt })
                  return previous === 100 ? { _tag: 'Pending' as const, nextPollAt: 200 } : complete
                }),
              cancel: () =>
                Effect.sync(() => {
                  cancels++
                }),
            })
            let configurations = 0
            const descriptor = {
              ...(yield* model(adapter)),
              configure: () =>
                Effect.sync(() => {
                  configurations++
                  return Context.empty()
                }),
            }
            const open = Effect.gen(function* () {
              const context = yield* store(backend)
              const runtime = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
                Effect.provideContext(context),
                Effect.provideService(Settings.Settings, policy),
              )
              return { context, runtime }
            })
            const owner = yield* Scope.make()
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
            const job = yield* submit(first.runtime)
            const checkpoint = (yield* saved(first.runtime, 'deferred')).checkpoint
            assert.strictEqual(checkpoint['pollAt'], 100)
            assert.strictEqual(checkpoint['expiresAt'], 500)
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(first.context)))
                .minimumReaderVersion,
              7,
            )
            yield* policy.update({ stream: { deferred: false } })
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(cancels, 0)
            yield* TestClock.adjust(150)
            const second = (yield* open).runtime
            const completion = yield* Effect.forkChild(second.backend.wait(job.id))
            yield* saved(second, 'deferred', 0, 200)
            assert.strictEqual(polls, 1)
            yield* TestClock.adjust(49)
            assert.strictEqual(polls, 1)
            yield* TestClock.adjust(1)
            assert.strictEqual((yield* Fiber.join(completion)).status, 'done')
            assert.strictEqual(starts, 1)
            assert.strictEqual(polls, 2)
            assert.strictEqual(configurations, 1)
            assert.deepEqual(identities, [identities[0], identities[0], identities[0]])
            assert.strictEqual(
              (yield* entries(second)).filter((entry) => entry.kind === 'assistant').length,
              1,
            )
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            assert.strictEqual(receipts.length, 1)
            assert.strictEqual(receipts[0]?.outcome._tag, 'completed')
            assert.strictEqual(
              (yield* Session.usage(second.session, root)).models[0]?.tokens.input,
              13,
            )
            yield* second.backend.wait(job.id)
            assert.strictEqual(polls, 2)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.effect(
      'saved expiry wins over a later poll and cold reopen never recreates expired work',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            let creates = 0
            let polls = 0
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.sync(() => {
                  creates++
                  return {
                    _tag: 'Deferred' as const,
                    handle: 'expire',
                    nextPollAt: 100,
                    expiresAt: 50,
                  }
                }),
              poll: () =>
                Effect.sync(() => {
                  polls++
                  return complete
                }),
              cancel: () => Effect.void,
            })
            const policy = yield* Settings.make({ stream: { deferred: true } })
            const descriptor = yield* model(adapter)
            const open = store(backend).pipe(
              Effect.flatMap((context) =>
                HarnessRuntime.make({ models: [descriptor] }).pipe(
                  Effect.provideContext(context),
                  Effect.provideService(Settings.Settings, policy),
                ),
              ),
            )
            const owner = yield* Scope.make()
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
            const job = yield* submit(first)
            yield* saved(first, 'deferred')
            yield* Scope.close(owner, Exit.void)
            yield* TestClock.adjust(50)
            const second = yield* open
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'unanswered')
            assert.strictEqual(creates, 1)
            assert.strictEqual(polls, 0)
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            assert.strictEqual(receipts.length, 1)
            assert.isUndefined(receipts[0]?.usage)
            assert.include(JSON.stringify(receipts[0]?.outcome), 'expiry')
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.effect(
      'finite typed poll retries keep the same attempt and a saved deadline despite live policy change',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            let creates = 0
            let polls = 0
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.sync(() => {
                  creates++
                  return {
                    _tag: 'Deferred' as const,
                    handle: 'retry',
                    nextPollAt: 10,
                    expiresAt: 1000,
                  }
                }),
              poll: () =>
                Effect.suspend(() => {
                  polls++
                  return polls === 1
                    ? Effect.fail(
                        AiError.make({
                          module: 'test',
                          method: 'poll',
                          reason: new AiError.InternalProviderError({ description: 'retry' }),
                        }),
                      )
                    : Effect.succeed(complete)
                }),
              cancel: () => Effect.void,
            })
            const descriptor = yield* model(adapter)
            const policy = yield* Settings.make({
              stream: { deferred: true },
              retry: { baseDelayMs: 100, maxAgentDelayMs: 100, maxRetries: 1 },
            })
            const open = store(backend).pipe(
              Effect.flatMap((context) =>
                HarnessRuntime.make({ models: [descriptor] }).pipe(
                  Effect.provideContext(context),
                  Effect.provideService(Settings.Settings, policy),
                ),
              ),
            )
            const owner = yield* Scope.make()
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
            const job = yield* submit(first)
            yield* saved(first, 'deferred')
            yield* TestClock.adjust(10)
            const retry = yield* saved(first, 'deferred', 1)
            assert.strictEqual(retry.checkpoint['pollAt'], 110)
            assert.strictEqual(retry.checkpoint['attempt'], 0)
            yield* policy.update({ retry: { enabled: false }, stream: { deferred: false } })
            yield* Scope.close(owner, Exit.void)
            const second = yield* open
            yield* TestClock.adjust(99)
            assert.strictEqual(polls, 1)
            yield* TestClock.adjust(1)
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
            assert.strictEqual(creates, 1)
            assert.strictEqual(polls, 2)
            assert.strictEqual(
              (yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))).length,
              1,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'rejected known completion preserves thirteen/seven once without an assistant or replay',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const owner = yield* Scope.make()
            const context = yield* store(backend).pipe(Effect.provideService(Scope.Scope, owner))
            const native = Context.get(context, Storage.Storage)
            const format = Context.get(context, StorageFormat.StorageFormat)
            let creates = 0
            let rejected = false
            const wrapped = Storage.Storage.of({
              ...native,
              commit: (input) =>
                Effect.suspend(() => {
                  const writes = Array.from(input)
                  const known = writes.some(
                    (write) =>
                      write._tag === 'task' &&
                      Option.isSome(
                        Schema.decodeUnknownOption(
                          Schema.Struct({
                            phase: Schema.Literal('deferred-start'),
                            usage: Usage.RecordSchema,
                          }),
                        )(write.value.state.checkpoint),
                      ),
                  )
                  if (known && !rejected) {
                    rejected = true
                    return Effect.fail(
                      new StorageError({
                        reason: 'io',
                        operation: 'test.deferred',
                        message: 'reject known response checkpoint',
                      }),
                    )
                  }
                  return native.commit(writes)
                }),
            })
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.sync(() => {
                  creates++
                  return complete
                }),
              poll: () => Effect.die('no poll'),
              cancel: () => Effect.void,
            })
            const descriptor = yield* model(adapter)
            const policy = yield* Settings.make({ stream: { deferred: true } })
            const first = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(
                context.pipe(
                  Context.add(Storage.Storage, wrapped),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
                ),
              ),
              Effect.provideService(Settings.Settings, policy),
              Effect.provideService(Scope.Scope, owner),
            )
            const job = yield* submit(first)
            assert.strictEqual((yield* first.backend.wait(job.id)).status, 'unanswered')
            assert.isTrue(rejected)
            assert.strictEqual(
              (yield* entries(first)).filter((entry) => entry.kind === 'assistant').length,
              0,
            )
            const report = yield* Session.usage(first.session, root)
            assert.strictEqual(report.models[0]?.tokens.input, 13)
            assert.strictEqual(report.models[0]?.tokens.output, 7)
            assert.strictEqual(report.unknownAttempts ?? 0, 0)
            yield* Scope.close(owner, Exit.void)
            const second = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(yield* store(backend)),
            )
            yield* second.backend.wait(job.id)
            assert.deepEqual(yield* Session.usage(second.session, root), report)
            assert.strictEqual(creates, 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  })

for (const cancellation of ['success', 'typed', 'defect', 'never', 'uninterruptible'] as const)
  it.effect(
    `abort persists its fence before ${cancellation} cancellation, settles locally and owns cleanup`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let finalized = false
          let runtime: HarnessRuntime.HarnessRuntimeService | undefined
          const adapter = yield* Model.makeDeferred<Schema.String, never>({
            handle: Schema.String,
            start: () =>
              Effect.succeed({
                _tag: 'Deferred',
                handle: 'cancel',
                nextPollAt: 100,
                expiresAt: 1000,
              }),
            poll: () => Effect.die('abort before poll'),
            cancel: () =>
              Effect.gen(function* () {
                if (runtime === undefined) return assert.fail('missing runtime')
                const tasks = yield* Session.scanTasks(runtime.session).pipe(
                  Stream.runCollect,
                  Effect.orDie,
                )
                assert.isTrue(tasks[0]?.abortRequested)
                yield* Deferred.succeed(entered, undefined)
                if (cancellation === 'typed')
                  return yield* new Model.DeferredTerminalError({
                    reason: 'failed',
                    responseId: 'cancel',
                    message: 'remote refused',
                  })
                if (cancellation === 'defect') return yield* Effect.die('cancel defect')
                if (cancellation === 'never') return yield* Effect.never
                if (cancellation === 'uninterruptible')
                  yield* Deferred.await(release).pipe(Effect.uninterruptible)
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    finalized = true
                  }),
                ),
              ),
          })
          const owner = yield* Scope.make()
          const policy = yield* Settings.make({ stream: { deferred: true } })
          runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
            Effect.provideService(Scope.Scope, owner),
            Effect.provideService(Settings.Settings, policy),
          )
          const job = yield* submit(runtime)
          yield* saved(runtime, 'deferred')
          const abort = yield* Effect.forkChild(runtime.backend.abort(root))
          yield* Deferred.await(entered)
          if (cancellation === 'never' || cancellation === 'uninterruptible')
            yield* TestClock.adjust(10000)
          yield* Fiber.join(abort)
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
          const before = yield* entries(runtime)
          const terminal = (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect))[0]
          if (terminal?.state.status !== 'terminal')
            return assert.fail('missing durable terminal task')
          const outcome = yield* Schema.decodeUnknownEffect(Record.TaskOutcome)(
            terminal.state.outcome,
          )
          if (outcome.status !== 'aborted') return assert.fail('missing durable aborted outcome')
          const diagnostic = yield* Schema.decodeUnknownEffect(
            Schema.Struct({ cancellation: Schema.Struct({ status: Schema.String }) }),
          )(outcome.result)
          let expectedStatus = 'failed'
          if (cancellation === 'success') expectedStatus = 'completed'
          if (cancellation === 'never' || cancellation === 'uninterruptible')
            expectedStatus = 'timeout'
          assert.strictEqual(diagnostic.cancellation.status, expectedStatus)
          const closing = yield* Effect.forkChild(Scope.close(owner, Exit.void))
          if (cancellation === 'uninterruptible') {
            yield* Effect.yieldNow
            assert.isFalse(finalized)
            assert.isUndefined(closing.pollUnsafe())
            yield* Deferred.succeed(release, undefined)
          }
          yield* Fiber.join(closing)
          assert.isTrue(finalized)
          const reopened = yield* HarnessRuntime.make({ models: [yield* model(adapter)] })
          assert.strictEqual((yield* reopened.backend.wait(job.id)).status, 'unanswered')
          assert.deepEqual(yield* entries(reopened), before)
          const receipts = yield* Session.commit(reopened.session, (tx) => Usage.receipts(tx, root))
          assert.strictEqual(receipts.length, 1)
          assert.strictEqual(receipts[0]?.outcome._tag, 'interrupted')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  it.effect(
    `${backend}: interrupted create without a saved handle is inspectably unknown and never recreated`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          const entered = yield* Deferred.make<void>()
          let creates = 0
          const adapter = yield* Model.makeDeferred<Schema.String, never>({
            handle: Schema.String,
            start: () =>
              Effect.gen(function* () {
                creates++
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never
              }),
            poll: () => Effect.die('no durable handle'),
            cancel: () => Effect.die('no durable handle'),
          })
          const descriptor = yield* model(adapter)
          const policy = yield* Settings.make({ stream: { deferred: true } })
          const open = store(backend).pipe(
            Effect.flatMap((context) =>
              HarnessRuntime.make({ models: [descriptor] }).pipe(
                Effect.provideContext(context),
                Effect.provideService(Settings.Settings, policy),
              ),
            ),
          )
          const owner = yield* Scope.make()
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
          const job = yield* submit(first)
          yield* Deferred.await(entered)
          assert.isUndefined((yield* saved(first, 'deferred-start')).checkpoint['handle'])
          yield* Scope.close(owner, Exit.void)
          const second = yield* open
          assert.strictEqual((yield* second.backend.wait(job.id)).status, 'unanswered')
          const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
          assert.strictEqual(receipts.length, 1)
          assert.isUndefined(receipts[0]?.usage)
          assert.include(JSON.stringify(receipts[0]?.outcome), 'outcome is unknown')
          assert.strictEqual((yield* Session.usage(second.session, root)).unknownAttempts, 1)
          assert.strictEqual(creates, 1)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}

for (const hint of [undefined, false, true, { window: '1h' as const }] as const)
  it.live(
    `explicit execution selection ${JSON.stringify(hint)} keeps streaming native or requires the capability`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let streams = 0
          const languageModel = yield* LanguageModel.make({
            generateText: () => Effect.die('native stream required'),
            streamText: () =>
              Stream.fromIterable([
                Response.makePart('text-start', { id: 'a' }),
                Response.makePart('text-delta', { id: 'a', delta: 'native' }),
                Response.makePart('text-end', { id: 'a' }),
                Response.makePart('finish', { reason: 'stop', usage: complete.response.usage }),
              ]).pipe(
                Stream.onStart(
                  Effect.sync(() => {
                    streams++
                  }),
                ),
              ),
          })
          const descriptor = Model.make({
            definition: {
              ref: { provider: 'custom', modelId: 'stream' },
              capabilities: {
                tools: true,
                reasoning: false,
                images: false,
                structuredOutput: false,
              },
            },
            options: Schema.JsonObject,
            configure: () => Effect.succeed(Context.empty()),
            languageModel,
          })
          const policy = yield* Settings.make({
            stream: hint === undefined ? {} : { deferred: hint },
          })
          const runtime = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
            Effect.provideService(Settings.Settings, policy),
          )
          const job = yield* submit(runtime)
          const result = yield* runtime.backend.wait(job.id)
          assert.strictEqual(
            result.status,
            hint === undefined || hint === false ? 'done' : 'unanswered',
          )
          assert.strictEqual(streams, hint === undefined || hint === false ? 1 : 0)
          assert.strictEqual(
            (yield* StorageFormat.inspect).minimumReaderVersion,
            hint === undefined || hint === false ? 6 : 4,
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

for (const window of ['15m', '1h', '24h'] as const)
  it.effect(`the ${window} setting pins a finite local expiry even without a provider expiry`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const policy = yield* Settings.make({ stream: { deferred: { window } } })
        const adapter = yield* Model.makeDeferred<Schema.String, never>({
          handle: Schema.String,
          start: () => Effect.succeed({ _tag: 'Deferred', handle: 'cap', nextPollAt: 1 }),
          poll: () => Effect.succeed(complete),
          cancel: () => Effect.void,
        })
        const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
          Effect.provideService(Settings.Settings, policy),
        )
        const job = yield* submit(runtime)
        const checkpoint = (yield* saved(runtime, 'deferred')).checkpoint
        assert.strictEqual(
          checkpoint['expiresAt'],
          { '15m': 900000, '1h': 3600000, '24h': 86400000 }[window],
        )
        yield* TestClock.adjust(1)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

for (const deadline of [0, -1, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1])
  it.effect(
    `an invalid absolute poll deadline ${deadline} fails without a hot loop or a replacement create`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let creates = 0
          let polls = 0
          const adapter = yield* Model.makeDeferred<Schema.String, never>({
            handle: Schema.String,
            start: () =>
              Effect.sync(() => {
                creates++
                return { _tag: 'Deferred' as const, handle: 'bad', nextPollAt: deadline }
              }),
            poll: () =>
              Effect.sync(() => {
                polls++
                return complete
              }),
            cancel: () => Effect.void,
          })
          const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
            Effect.provideService(
              Settings.Settings,
              yield* Settings.make({ stream: { deferred: true } }),
            ),
          )
          const job = yield* submit(runtime)
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
          assert.strictEqual(creates, 1)
          assert.strictEqual(polls, 0)
          assert.strictEqual((yield* Session.usage(runtime.session, root)).unknownAttempts, 1)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

it.effect(
  'poll failure budget is cumulative across pending results and never increments the provider attempt',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const identities: Model.DeferredIdentity[] = []
        let creates = 0
        let polls = 0
        const adapter = yield* Model.makeDeferred<Schema.String, never>({
          handle: Schema.String,
          start: () =>
            Effect.sync(() => {
              creates++
              return { _tag: 'Deferred' as const, handle: 'budget', nextPollAt: 1 }
            }),
          poll: (_handle, previous, request) =>
            Effect.suspend(() => {
              polls++
              identities.push({ requestId: request.requestId, attempt: request.attempt })
              if (polls === 2)
                return Effect.succeed({ _tag: 'Pending' as const, nextPollAt: previous + 1 })
              return Effect.fail(
                AiError.make({
                  module: 'test',
                  method: 'poll',
                  reason: new AiError.InternalProviderError({ description: 'transient' }),
                }),
              )
            }),
          cancel: () => Effect.void,
        })
        const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
          Effect.provideService(
            Settings.Settings,
            yield* Settings.make({
              stream: { deferred: true },
              retry: { baseDelayMs: 0, maxAgentDelayMs: 1, maxRetries: 1 },
            }),
          ),
        )
        const job = yield* submit(runtime)
        yield* saved(runtime, 'deferred')
        yield* TestClock.adjust(1)
        yield* saved(runtime, 'deferred', 1, 2)
        yield* TestClock.adjust(1)
        yield* saved(runtime, 'deferred', 1, 3)
        yield* TestClock.adjust(1)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
        assert.strictEqual(creates, 1)
        assert.strictEqual(polls, 3)
        assert.deepEqual(identities, [identities[0], identities[0], identities[0]])
        assert.strictEqual(identities[0]?.attempt, 0)
        assert.strictEqual(
          (yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))).length,
          1,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

for (const action of ['complete', 'abort', 'cancel-timeout'] as const)
  it.effect(
    `native bound OpenAI wire ${action} retains auth, transforms and restored pinned configuration on cold reopen`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls: HttpClientRequest.HttpClientRequest[] = []
          let clientFinalized = false
          let cancelFinalized = false
          const transport = HttpClient.make((request) => {
            assert.isFalse(clientFinalized)
            calls.push(request)
            if (request.url.endsWith('/cancel') && action === 'cancel-timeout')
              return Effect.never.pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    cancelFinalized = true
                  }),
                ),
              )
            let status = 'completed'
            if (calls.length === 1) status = 'queued'
            if (request.url.endsWith('/cancel')) status = 'cancelled'
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new globalThis.Response(
                  JSON.stringify({
                    id: 'resp/a b',
                    model: 'gpt-4.1',
                    created_at: 0,
                    status,
                    output:
                      status === 'completed'
                        ? [
                            {
                              type: 'message',
                              id: 'msg',
                              role: 'assistant',
                              status: 'completed',
                              content: [{ type: 'output_text', text: 'wire', annotations: [] }],
                            },
                          ]
                        : [],
                    ...(status === 'completed'
                      ? { usage: { input_tokens: 13, output_tokens: 7, total_tokens: 20 } }
                      : {}),
                  }),
                  { headers: { 'content-type': 'application/json' } },
                ),
              ),
            )
          })
          const clientOwner = yield* Scope.make()
          yield* Scope.addFinalizer(
            clientOwner,
            Effect.sync(() => {
              clientFinalized = true
            }),
          )
          const client = yield* OpenAiClient.make({
            apiUrl: 'https://proxy.invalid/v2',
            apiKey: Redacted.make('fixture-key'),
            organizationId: Redacted.make('fixture-org'),
            projectId: Redacted.make('fixture-project'),
            transformClient: (client) =>
              HttpClient.mapRequest(client, (request) => ({
                ...request,
                headers: { ...request.headers, 'x-bound': 'yes' },
              })),
          }).pipe(
            Effect.provideService(HttpClient.HttpClient, transport),
            Effect.provideService(Scope.Scope, clientOwner),
          )
          const deferred = yield* DeferredResponse.make({
            model: 'gpt-4.1',
            pollIntervalMs: 100,
          }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, client))
          const languageModel = yield* OwnedOpenAiModel.make({ model: 'gpt-4.1' }).pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, client),
          )
          const configSchema = Schema.Struct({ temperature: Schema.Finite, marker: Schema.String })
          const restore = (value: typeof configSchema.Type) =>
            Context.make(NativeOpenAiModel.Config, {
              model: 'gpt-4.1',
              temperature: value.temperature,
            }).pipe(
              Context.add(NativeOpenAiConfig.OpenAiConfig, {
                transformClient: (client) =>
                  HttpClient.mapRequest(client, (request) => ({
                    ...request,
                    headers: { ...request.headers, 'x-pinned': value.marker },
                  })),
              }),
            )
          const configuration = yield* Model.makeConfiguration<typeof configSchema, never, never>({
            id: 'test/native-openai-deferred',
            version: 1,
            schema: configSchema,
            tags: [NativeOpenAiModel.Config, NativeOpenAiConfig.OpenAiConfig],
            capture: (context) =>
              Effect.succeed({
                temperature: Context.getUnsafe(context, NativeOpenAiModel.Config).temperature ?? 0,
                marker: 'saved',
              }),
            restore: (value) => Effect.succeed(restore(value)),
          })
          let configured = 0
          const descriptor = Model.make({
            definition: {
              ref: { provider: 'openai', modelId: 'gpt-4.1' },
              capabilities: { tools: true, reasoning: true, images: true, structuredOutput: true },
            },
            options: Schema.JsonObject,
            languageModel,
            deferred,
            configuration,
            configure: () =>
              Effect.sync(() => {
                configured++
                return restore({ temperature: 0.7, marker: 'saved' })
              }),
          })
          const storage = yield* Layer.build(Storage.layerMemory)
          const policy = yield* Settings.make({ stream: { deferred: true } })
          const open = HarnessRuntime.make({ models: [descriptor] }).pipe(
            Effect.provideContext(storage),
            Effect.provideService(Settings.Settings, policy),
          )
          const firstOwner = yield* Scope.make()
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstOwner))
          const job = yield* submit(first)
          const accepted = (yield* saved(first, 'deferred')).checkpoint
          assert.notInclude(JSON.stringify(accepted), 'fixture-key')
          assert.notInclude(JSON.stringify(accepted), 'https://proxy.invalid')
          yield* Scope.close(firstOwner, Exit.void)
          assert.strictEqual(calls.length, 1)
          assert.isFalse(clientFinalized)
          yield* policy.update({ stream: { deferred: false } })
          const secondOwner = yield* Scope.make()
          const second = yield* open.pipe(Effect.provideService(Scope.Scope, secondOwner))
          if (action === 'complete') {
            const waiting = yield* Effect.forkChild(second.backend.wait(job.id))
            yield* TestClock.adjust(100)
            assert.strictEqual((yield* Fiber.join(waiting)).status, 'done')
            assert.strictEqual(
              (yield* Session.usage(second.session, root)).models[0]?.tokens.input,
              13,
            )
          } else {
            const abort = yield* Effect.forkChild(second.backend.abort(root))
            if (action === 'cancel-timeout') {
              while (calls.length < 2) yield* Effect.yieldNow
              yield* TestClock.adjust(10000)
            }
            yield* Fiber.join(abort)
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'unanswered')
            const task = (yield* Session.scanTasks(second.session).pipe(Stream.runCollect))[0]
            const outcome = yield* Schema.decodeUnknownEffect(Record.TaskOutcome)(
              task?.state.outcome,
            )
            if (outcome.status !== 'aborted') return assert.fail('missing abort diagnostic')
            assert.strictEqual(
              (yield* Schema.decodeUnknownEffect(
                Schema.Struct({ cancellation: Schema.Struct({ status: Schema.String }) }),
              )(outcome.result)).cancellation.status,
              action === 'abort' ? 'completed' : 'timeout',
            )
          }
          assert.strictEqual(configured, 1)
          assert.deepEqual(
            calls.map((request) => [request.method, request.url]),
            [
              ['POST', 'https://proxy.invalid/v2/responses'],
              [
                action === 'complete' ? 'GET' : 'POST',
                `https://proxy.invalid/v2/responses/resp%2Fa%20b${action === 'complete' ? '' : '/cancel'}`,
              ],
            ],
          )
          for (const request of calls) {
            assert.strictEqual(request.headers['authorization'], 'Bearer fixture-key')
            assert.strictEqual(request.headers['openai-organization'], 'fixture-org')
            assert.strictEqual(request.headers['openai-project'], 'fixture-project')
            assert.strictEqual(request.headers['x-bound'], 'yes')
            assert.strictEqual(request.headers['x-pinned'], 'saved')
          }
          const firstRequest = calls[0]
          if (firstRequest?.body._tag !== 'Uint8Array')
            return assert.fail('missing native request body')
          const body = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.JsonObject))(
            new TextDecoder().decode(firstRequest.body.body),
          )
          assert.strictEqual(body['temperature'], 0.7)
          assert.strictEqual(body['background'], true)
          assert.strictEqual(body['store'], true)
          assert.strictEqual(body['stream'], false)
          assert.isString(body['prompt_cache_key'])
          yield* Scope.close(secondOwner, Exit.void)
          assert.isFalse(clientFinalized)
          if (action === 'cancel-timeout') assert.isTrue(cancelFinalized)
          yield* Scope.close(clientOwner, Exit.void)
          assert.isTrue(clientFinalized)
        }),
      ),
  )

for (const boundary of ['start-error', 'poll-error', 'poll-complete', 'terminal'] as const)
  it.effect(
    `${boundary} known usage survives a directly rejected checkpoint or terminal commit`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Storage.layerMemory)
          const native = Context.get(context, Storage.Storage)
          const format = Context.get(context, StorageFormat.StorageFormat)
          let rejected = false
          let creates = 0
          let polls = 0
          const wrapped = Storage.Storage.of({
            ...native,
            commit: (input) =>
              Effect.suspend(() => {
                const writes = Array.from(input)
                const known = writes.some((write) =>
                  boundary === 'terminal'
                    ? write._tag === 'entry' && write.value.kind === 'assistant'
                    : write._tag === 'task' &&
                      Option.isSome(
                        Schema.decodeUnknownOption(Schema.Struct({ usage: Usage.RecordSchema }))(
                          write.value.state.checkpoint,
                        ),
                      ),
                )
                if (known && !rejected) {
                  rejected = true
                  return Effect.fail(
                    new StorageError({
                      reason: 'io',
                      operation: 'test.deferred.known',
                      message: 'known publication rejected',
                    }),
                  )
                }
                return native.commit(writes)
              }),
          })
          const providerError = AiError.make({
            module: 'test',
            method: boundary,
            reason: new AiError.InternalProviderError({
              description: 'native observed failed response',
              metadata: { usage: { promptTokens: 13, completionTokens: 7, totalTokens: 20 } },
            }),
          })
          const adapter = yield* Model.makeDeferred<Schema.String, never>({
            handle: Schema.String,
            start: () =>
              Effect.suspend(
                (): Effect.Effect<
                  Model.Complete | Model.DeferredHandle<string>,
                  AiError.AiError
                > => {
                  creates++
                  if (boundary === 'start-error') return Effect.fail(providerError)
                  if (boundary === 'terminal') return Effect.succeed(complete)
                  return Effect.succeed({ _tag: 'Deferred', handle: 'known', nextPollAt: 1 })
                },
              ),
            poll: () =>
              Effect.suspend(() => {
                polls++
                return boundary === 'poll-error'
                  ? Effect.fail(providerError)
                  : Effect.succeed(complete)
              }),
            cancel: () => Effect.void,
          })
          const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
            Effect.provideContext(
              context.pipe(
                Context.add(Storage.Storage, wrapped),
                Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
              ),
            ),
            Effect.provideService(
              Settings.Settings,
              yield* Settings.make({ stream: { deferred: true }, retry: { enabled: false } }),
            ),
          )
          const job = yield* submit(runtime)
          if (boundary.startsWith('poll')) {
            yield* saved(runtime, 'deferred')
            yield* TestClock.adjust(1)
          }
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
          assert.isTrue(rejected)
          const usage = yield* Session.usage(runtime.session, root)
          assert.strictEqual(usage.models[0]?.tokens.input, 13)
          assert.strictEqual(usage.models[0]?.tokens.output, 7)
          assert.strictEqual(usage.unknownAttempts ?? 0, 0)
          assert.strictEqual(
            (yield* entries(runtime)).filter((entry) => entry.kind === 'assistant').length,
            0,
          )
          assert.strictEqual(
            (yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))).length,
            1,
          )
          assert.strictEqual(creates, 1)
          assert.strictEqual(polls, boundary.startsWith('poll') ? 1 : 0)
        }),
      ),
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const boundary of ['handle', 'known'] as const)
    it.effect(
      `${backend}: genuinely committed uncertain ${boundary} retains authoritative recovery and never repeats create`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const owner = yield* Scope.make()
            const context = yield* store(backend).pipe(Effect.provideService(Scope.Scope, owner))
            const native = Context.get(context, Storage.Storage)
            const format = Context.get(context, StorageFormat.StorageFormat)
            let lost = false
            let creates = 0
            let polls = 0
            const wrapped = Storage.Storage.of({
              ...native,
              commit: (input) =>
                Effect.gen(function* () {
                  const writes = Array.from(input)
                  const revision = yield* native.commit(writes)
                  const matched = writes.some(
                    (write) =>
                      write._tag === 'task' &&
                      Option.isSome(
                        Schema.decodeUnknownOption(
                          boundary === 'handle'
                            ? Schema.Struct({ phase: Schema.Literal('deferred') })
                            : Schema.Struct({ usage: Usage.RecordSchema }),
                        )(write.value.state.checkpoint),
                      ),
                  )
                  if (matched && !lost) {
                    lost = true
                    return yield* new StorageError({
                      reason: 'uncertain',
                      operation: 'test.deferred.uncertain',
                      message: 'commit succeeded, acknowledgement lost',
                    })
                  }
                  return revision
                }),
            })
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.sync((): Model.Complete | Model.DeferredHandle<string> => {
                  creates++
                  return boundary === 'known'
                    ? complete
                    : { _tag: 'Deferred', handle: 'committed', nextPollAt: 1 }
                }),
              poll: () =>
                Effect.sync(() => {
                  polls++
                  return complete
                }),
              cancel: () => Effect.void,
            })
            const descriptor = yield* model(adapter)
            const policy = yield* Settings.make({ stream: { deferred: true } })
            const first = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(
                context.pipe(
                  Context.add(Storage.Storage, wrapped),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
                ),
              ),
              Effect.provideService(Settings.Settings, policy),
              Effect.provideService(Scope.Scope, owner),
            )
            const job = yield* submit(first)
            assert.isTrue(Exit.isFailure(yield* Effect.exit(first.backend.wait(job.id))))
            assert.isTrue(lost)
            assert.isTrue(Exit.isFailure(yield* Effect.exit(Session.usage(first.session, root))))
            yield* Scope.close(owner, Exit.void)
            const second = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(yield* store(backend)),
            )
            const waiting = yield* Effect.forkChild(second.backend.wait(job.id))
            yield* TestClock.adjust(1)
            assert.strictEqual(
              (yield* Fiber.join(waiting)).status,
              boundary === 'handle' ? 'done' : 'unanswered',
            )
            assert.strictEqual(creates, 1)
            assert.strictEqual(polls, boundary === 'handle' ? 1 : 0)
            const usage = yield* Session.usage(second.session, root)
            assert.strictEqual(usage.models[0]?.tokens.input, 13)
            assert.strictEqual(usage.models[0]?.tokens.output, 7)
            assert.strictEqual(usage.unknownAttempts ?? 0, 0)
            assert.strictEqual(
              (yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))).length,
              1,
            )
            assert.strictEqual(
              (yield* entries(second)).filter((entry) => entry.kind === 'assistant').length,
              boundary === 'handle' ? 1 : 0,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  for (const uncertain of [false, true])
    it.effect(
      `${backend}: ${uncertain ? 'uncertain committed' : 'rejected'} abort settlement is atomic and cold recovery cannot resurrect work`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const owner = yield* Scope.make()
            const context = yield* store(backend).pipe(Effect.provideService(Scope.Scope, owner))
            const native = Context.get(context, Storage.Storage)
            const format = Context.get(context, StorageFormat.StorageFormat)
            let rejected = false
            let cancels = 0
            let creates = 0
            const wrapped = Storage.Storage.of({
              ...native,
              commit: (input) =>
                Effect.gen(function* () {
                  const writes = Array.from(input)
                  const abort = writes.some(
                    (write) =>
                      write._tag === 'task' &&
                      (write.value.state.status === 'completing' ||
                        write.value.state.status === 'terminal') &&
                      Option.isSome(
                        Schema.decodeUnknownOption(
                          Schema.Struct({ status: Schema.Literal('aborted') }),
                        )(write.value.state.outcome),
                      ),
                  )
                  if (abort && !rejected) {
                    rejected = true
                    if (uncertain) yield* native.commit(writes)
                    return yield* new StorageError({
                      reason: uncertain ? 'uncertain' : 'io',
                      operation: 'test.abort.atomic',
                      message: 'abort commit rejected or acknowledgement lost',
                    })
                  }
                  return yield* native.commit(writes)
                }),
            })
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.sync(() => {
                  creates++
                  return { _tag: 'Deferred' as const, handle: 'abort', nextPollAt: 100 }
                }),
              poll: () => Effect.die('must not poll aborted work'),
              cancel: () =>
                Effect.sync(() => {
                  cancels++
                }),
            })
            const descriptor = yield* model(adapter)
            const first = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(
                context.pipe(
                  Context.add(Storage.Storage, wrapped),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
                ),
              ),
              Effect.provideService(
                Settings.Settings,
                yield* Settings.make({ stream: { deferred: true } }),
              ),
              Effect.provideService(Scope.Scope, owner),
            )
            const job = yield* submit(first)
            yield* saved(first, 'deferred')
            assert.isTrue(Exit.isFailure(yield* Effect.exit(first.backend.abort(root))))
            assert.isTrue(rejected)
            if (!uncertain) {
              assert.strictEqual(
                (yield* Session.commit(first.session, (tx) => Usage.receipts(tx, root))).length,
                0,
              )
              const current = (yield* Session.scanTasks(first.session).pipe(Stream.runCollect))[0]
              assert.isTrue(current?.abortRequested)
              assert.strictEqual(current?.state.status, 'running')
            }
            yield* Scope.close(owner, Exit.void)
            const second = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
              Effect.provideContext(yield* store(backend)),
            )
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'unanswered')
            assert.strictEqual(creates, 1)
            assert.strictEqual(cancels, uncertain ? 1 : 2)
            assert.strictEqual(
              (yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))).length,
              1,
            )
            const tasks = yield* Session.scanTasks(second.session).pipe(Stream.runCollect)
            const outcome = yield* Schema.decodeUnknownEffect(Record.TaskOutcome)(
              tasks[0]?.state.outcome,
            )
            assert.strictEqual(outcome.status, 'aborted')
            if (outcome.status !== 'aborted') return assert.fail('missing aborted result')
            assert.strictEqual(
              (yield* Schema.decodeUnknownEffect(
                Schema.Struct({ cancellation: Schema.Struct({ status: Schema.String }) }),
              )(outcome.result)).cancellation.status,
              'completed',
            )
            assert.strictEqual(
              (yield* entries(second)).filter(
                (entry) => entry.kind === 'assistant' || entry.kind === 'tool.result',
              ).length,
              0,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
}

it.effect(
  'a legacy preparation without execution stays streaming when live deferred policy and descriptor become available',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* Layer.build(Storage.layerMemory)
        let streams = 0
        let deferredStarts = 0
        const entered = yield* Deferred.make<void>()
        const languageModel = yield* LanguageModel.make({
          generateText: () => Effect.die('native streaming required'),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                streams++
                if (streams === 1) {
                  yield* Deferred.succeed(entered, undefined)
                  return Stream.fromEffect(Effect.never)
                }
                return Stream.fromIterable([
                  Response.makePart('text-start', { id: 'a' }),
                  Response.makePart('text-delta', { id: 'a', delta: 'legacy native' }),
                  Response.makePart('text-end', { id: 'a' }),
                  Response.makePart('finish', { reason: 'stop', usage: complete.response.usage }),
                ])
              }),
            ),
        })
        const deferred = yield* Model.makeDeferred<Schema.String, never>({
          handle: Schema.String,
          start: () =>
            Effect.sync(() => {
              deferredStarts++
              return complete
            }),
          poll: () => Effect.die('legacy must not poll'),
          cancel: () => Effect.void,
        })
        const descriptor = Model.make({
          definition: {
            ref: { provider: 'custom', modelId: 'legacy' },
            capabilities: { tools: true, reasoning: true, images: false, structuredOutput: false },
          },
          options: Schema.JsonObject,
          languageModel,
          deferred,
          configure: () => Effect.succeed(Context.empty()),
        })
        const owner = yield* Scope.make()
        const first = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
          Effect.provideContext(storage),
          Effect.provideService(Scope.Scope, owner),
        )
        const job = yield* submit(first)
        yield* Deferred.await(entered)
        const task = (yield* saved(first, 'streaming')).task
        yield* Scope.close(owner, Exit.void)
        const editScope = yield* Scope.make()
        const session = yield* Session.make().pipe(
          Effect.provideContext(storage),
          Effect.provideService(Scope.Scope, editScope),
        )
        const wire = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(task.state.checkpoint)
        const preparation = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(
          wire['preparation'],
        )
        const { execution: _execution, ...legacy } = preparation
        yield* Session.commit(session, (tx) =>
          Transaction.putTask(tx, {
            ...task,
            state: {
              status: 'pending',
              checkpoint: { ...wire, _tag: 'request', phase: 'request', preparation: legacy },
            },
          }),
        )
        yield* Scope.close(editScope, Exit.void)
        const second = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
          Effect.provideContext(storage),
          Effect.provideService(
            Settings.Settings,
            yield* Settings.make({ stream: { deferred: true } }),
          ),
        )
        assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
        assert.strictEqual(streams, 2)
        assert.strictEqual(deferredStarts, 0)
        assert.strictEqual(
          (yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))).minimumReaderVersion,
          6,
        )
      }),
    ),
)

it.effect(
  'captured callback and both handle codec resources outlive timed-out cancellation until runtime close joins them',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        class Resource extends Context.Service<Resource, { readonly alive: () => boolean }>()(
          'test/deferred/Resource',
        ) {}
        let alive = true
        let encoded = 0
        let decoded = 0
        let finished = false
        const resourceOwner = yield* Scope.make()
        yield* Scope.addFinalizer(
          resourceOwner,
          Effect.sync(() => {
            alive = false
          }),
        )
        const released = yield* Deferred.make<void>()
        const entered = yield* Deferred.make<void>()
        const check = Resource.pipe(
          Effect.map((resource) => {
            assert.isTrue(resource.alive())
          }),
        )
        const codec = Schema.String.pipe(
          Schema.decodeTo(Schema.String, {
            decode: SchemaGetter.transformEffect((value) =>
              check.pipe(
                Effect.as(value.slice(7)),
                Effect.tap(() =>
                  Effect.sync(() => {
                    decoded++
                  }),
                ),
              ),
            ),
            encode: SchemaGetter.transformEffect((value) =>
              check.pipe(
                Effect.as(`handle:${value}`),
                Effect.tap(() =>
                  Effect.sync(() => {
                    encoded++
                  }),
                ),
              ),
            ),
          }),
        )
        const adapter = yield* Model.makeDeferred<typeof codec, Resource>({
          handle: codec,
          start: () =>
            check.pipe(Effect.as({ _tag: 'Deferred' as const, handle: 'owned', nextPollAt: 100 })),
          poll: () => Effect.die('abort before poll'),
          cancel: (handle) =>
            Effect.gen(function* () {
              yield* check
              assert.strictEqual(handle, 'owned')
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(released).pipe(Effect.uninterruptible)
              yield* check
              finished = true
            }).pipe(Effect.uninterruptible),
        }).pipe(Effect.provideService(Resource, { alive: () => alive }))
        const runtimeOwner = yield* Scope.make()
        const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
          Effect.provideService(Scope.Scope, runtimeOwner),
          Effect.provideService(
            Settings.Settings,
            yield* Settings.make({ stream: { deferred: true } }),
          ),
        )
        const job = yield* submit(runtime)
        assert.strictEqual((yield* saved(runtime, 'deferred')).checkpoint['handle'], 'handle:owned')
        const abort = yield* Effect.forkChild(runtime.backend.abort(root))
        yield* Deferred.await(entered)
        yield* TestClock.adjust(10000)
        yield* Fiber.join(abort)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
        const before = yield* entries(runtime)
        const closing = yield* Effect.forkChild(Scope.close(runtimeOwner, Exit.void))
        yield* Effect.yieldNow
        assert.isUndefined(closing.pollUnsafe())
        assert.isFalse(finished)
        assert.isTrue(alive)
        yield* Deferred.succeed(released, undefined)
        yield* Fiber.join(closing)
        assert.isTrue(finished)
        assert.strictEqual(encoded, 1)
        assert.strictEqual(decoded, 1)
        const reopenedOwner = yield* Scope.make()
        const reopened = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
          Effect.provideService(Scope.Scope, reopenedOwner),
        )
        assert.strictEqual((yield* reopened.backend.wait(job.id)).status, 'unanswered')
        assert.deepEqual(yield* entries(reopened), before)
        yield* Scope.close(reopenedOwner, Exit.void)
        yield* Scope.close(resourceOwner, Exit.void)
        assert.isFalse(alive)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.live(
  'a deferred completion uses the existing tool and request boundaries once with a fresh identity for the next logical request',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const work = Tool.make('work', {
          parameters: Schema.Struct({ value: Schema.String }),
          success: Schema.String,
          replay: 'safe',
        })
        const toolkit = Toolkit.make(work)
        const identities: Model.DeferredIdentity[] = []
        let executions = 0
        const adapter = yield* Model.makeDeferred<Schema.String, never>({
          handle: Schema.String,
          start: (request) =>
            Effect.sync(() => {
              identities.push({ requestId: request.requestId, attempt: request.attempt })
              return identities.length === 1
                ? {
                    _tag: 'Complete' as const,
                    response: new LanguageModel.GenerateTextResponse([
                      Response.makePart('tool-call', {
                        id: 'call',
                        name: 'work',
                        params: { value: 'once' },
                        providerExecuted: false,
                      }),
                      Response.makePart('finish', {
                        reason: 'tool-calls',
                        usage: complete.response.usage,
                      }),
                    ]),
                  }
                : complete
            }),
          poll: () => Effect.die('immediate completion'),
          cancel: () => Effect.void,
        })
        const runtime = yield* HarnessRuntime.make({
          models: [yield* model(adapter)],
          tools: toolkit,
        }).pipe(
          Effect.provide(
            toolkit.toLayer({
              work: ({ value }) =>
                Effect.sync(() => {
                  executions++
                  return value
                }),
            }),
          ),
          Effect.provideService(
            Settings.Settings,
            yield* Settings.make({ stream: { deferred: true } }),
          ),
        )
        const job = yield* submit(runtime)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        assert.strictEqual(executions, 1)
        assert.strictEqual(identities.length, 2)
        assert.notStrictEqual(identities[0]?.requestId, identities[1]?.requestId)
        assert.strictEqual(identities[0]?.attempt, 0)
        assert.strictEqual(identities[1]?.attempt, 0)
        const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
        assert.strictEqual(receipts.length, 2)
        assert.strictEqual(
          (yield* Session.usage(runtime.session, root)).models[0]?.tokens.input,
          26,
        )
        assert.strictEqual(
          (yield* entries(runtime)).filter((entry) => entry.kind === 'tool.result').length,
          1,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

for (const delayed of ['start', 'poll'] as const)
  for (const future of [false, true])
    it.effect(
      `a slow ${delayed} ${future ? 'accepts completion plus one' : 'rejects an elapsed new deadline without an immediate poll'}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            let creates = 0
            let polls = 0
            const adapter = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.gen(function* () {
                  creates++
                  if (delayed === 'start') {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                  }
                  return {
                    _tag: 'Deferred' as const,
                    handle: 'elapsed',
                    nextPollAt:
                      future && delayed === 'start' ? (yield* Clock.currentTimeMillis) + 1 : 1,
                  }
                }),
              poll: () =>
                Effect.gen(function* () {
                  polls++
                  if (polls > 1 || delayed === 'start') return complete
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  return {
                    _tag: 'Pending' as const,
                    nextPollAt: future ? (yield* Clock.currentTimeMillis) + 1 : 2,
                  }
                }),
              cancel: () => Effect.void,
            })
            const runtime = yield* HarnessRuntime.make({ models: [yield* model(adapter)] }).pipe(
              Effect.provideService(
                Settings.Settings,
                yield* Settings.make({ stream: { deferred: true } }),
              ),
            )
            const job = yield* submit(runtime)
            const settled = yield* runtime.backend.wait(job.id).pipe(Effect.forkScoped)
            if (delayed === 'poll') {
              yield* saved(runtime, 'deferred', 0, 1)
              yield* TestClock.adjust(1)
            }
            yield* Deferred.await(entered)
            yield* TestClock.adjust(2)
            yield* Deferred.succeed(release, undefined)
            if (future) {
              yield* saved(runtime, 'deferred', 0, delayed === 'start' ? 3 : 4)
              assert.strictEqual(polls, delayed === 'start' ? 0 : 1)
              yield* TestClock.adjust(1)
            }
            assert.strictEqual((yield* Fiber.join(settled)).status, future ? 'done' : 'unanswered')
            assert.strictEqual(creates, 1)
            assert.strictEqual(polls, (delayed === 'start' ? 0 : 1) + (future ? 1 : 0))
            if (future)
              assert.strictEqual(
                (yield* Session.usage(runtime.session, root)).models[0]?.tokens.input,
                13,
              )
            else
              assert.strictEqual((yield* Session.usage(runtime.session, root)).unknownAttempts, 1)
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )
