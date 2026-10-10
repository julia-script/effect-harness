import * as Hook from 'effect-harness/Hook'
import * as Prompt from 'effect/ai/Prompt'
import * as Document from 'effect-harness/Document'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as Conversation from 'effect-harness/Conversation'
import * as ConversationInitializer from 'effect-harness/ConversationInitializer'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import { ProviderAffinity } from 'effect-harness/ProviderAffinity'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const pricing = { inputToken: 0.01, outputToken: 0.02, currency: 'USD', source: 'fixture-v1' }
const finish = Response.makePart('finish', {
  reason: 'stop',
  usage: new Response.Usage({
    inputTokens: { total: 10, uncached: 6, cacheRead: 4, cacheWrite: undefined },
    outputTokens: { total: 5, text: 3, reasoning: 2 },
  }),
})
const answer = [Response.makePart('text', { text: 'done' }), finish]
const definition = (
  native: typeof LanguageModel.LanguageModel.Service,
  identities?: Array<string>,
  price: Usage.Pricing | null = pricing,
) =>
  Model.make({
    definition: {
      ref: { provider: 'fixture', modelId: 'deterministic' },
      capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
      ...(price === null ? {} : { pricing: price }),
    },
    languageModel: native,
    options: Schema.Struct({}),
    configure: () =>
      Effect.gen(function* () {
        const current = yield* ProviderAffinity
        identities?.push(current.id)
        return Context.empty()
      }),
  })
const submit = (
  runtime: HarnessRuntime.HarnessRuntimeService,
  conversationId = root,
  requestId?: string,
) =>
  Effect.gen(function* () {
    const job = yield* runtime.backend.submit({
      conversationId,
      draft: { type: 'input', content: 'go', ...(requestId === undefined ? {} : { requestId }) },
    })
    assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
    return job
  })
const own = (runtime: HarnessRuntime.HarnessRuntimeService, conversationId = root) =>
  Session.scanEntries(runtime.session, { conversationId, order: 'ascending' }).pipe(
    Stream.runCollect,
  )
const providerDocument = Document.define({
  kind: 'harness.provider',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Schema.Struct({ id: Schema.String }),
  initial: () => ({ id: '' }),
})
const affinity = (session: Session.Session, id = root) =>
  Session.snapshot(session, providerDocument, {
    scope: { _tag: 'conversation', conversationId: id },
  }).pipe(Effect.map(Option.map((value) => value.value.id)))
const memory = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(Storage.layerMemory))

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  it.effect(
    `${backend}: affinity, priced receipts, forks and repeated queries survive reopen`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'provider-usage-' })
          const disk =
            backend === 'sqlite'
              ? Storage.layerSql.pipe(
                  Layer.provide(SqliteClient.layer({ filename: `${directory}/state.sqlite` })),
                )
              : Storage.layerJsonl({ filePath: `${directory}/state.jsonl` })
          const identities: Array<string> = []
          const native = yield* LanguageModel.make({
            generateText: () => Effect.succeed(answer),
            streamText: () => Stream.empty,
          })
          const models = [definition(native, identities)]
          const open = Effect.gen(function* () {
            const storage =
              backend === 'memory'
                ? yield* Storage.Storage
                : Context.get(yield* Layer.build(disk), Storage.Storage)
            return yield* HarnessRuntime.make({ models }).pipe(
              Effect.provideService(Storage.Storage, storage),
            )
          })
          const firstScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
          yield* first.backend.root
          const initialAffinity = Option.getOrThrow(yield* affinity(first.session))
          yield* submit(first, root, 'dedup')
          const baseline = yield* Session.usage(first.session)
          assert.strictEqual(baseline.models[0]?.tokens.input, 10)
          assert.deepEqual(baseline.costs, [{ currency: 'USD', model: 0.2, tool: 0 }])
          assert.strictEqual(identities[0], initialAffinity)
          assert.deepEqual(yield* Session.usage(first.session), baseline)
          const parentEntries = yield* own(first)
          const last = parentEntries[parentEntries.length - 1]!
          const child = yield* first.backend.fork({
            conversationId: root,
            options: { at: last.id },
          })
          assert.notStrictEqual(
            Option.getOrThrow(yield* affinity(first.session, child)),
            initialAffinity,
          )
          assert.deepEqual((yield* Session.usage(first.session, child)).models, [])
          assert.deepEqual(yield* Session.usage(first.session), baseline)
          yield* submit(first, child)
          assert.strictEqual((yield* Session.usage(first.session, child)).models[0]?.responses, 1)
          assert.strictEqual((yield* Session.usage(first.session)).models[0]?.responses, 2)
          yield* Scope.close(firstScope, Exit.void)
          const second = yield* open
          yield* second.backend.root
          assert.strictEqual(Option.getOrThrow(yield* affinity(second.session)), initialAffinity)
          yield* submit(second, root, 'dedup')
          assert.strictEqual(identities.length, 2)
          assert.strictEqual((yield* Session.usage(second.session)).models[0]?.responses, 2)
          yield* submit(second)
          assert.strictEqual(identities[2], initialAffinity)
          const client = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, second.backend),
          )
          const handle = yield* client.root
          assert.strictEqual((yield* Conversation.usage(handle)).models[0]?.responses, 2)
          assert.strictEqual((yield* client.usage()).models[0]?.responses, 3)
          const all = yield* own(second)
          assert.deepEqual(
            Usage.aggregate([...all, ...all]),
            yield* Session.usage(second.session, root),
          )
          const record = all.find((entry) => entry.usage !== undefined)?.usage
          assert.strictEqual(record?._tag, 'model')
          if (record?._tag === 'model') assert.deepEqual(record.pricing, pricing)
        }),
      ).pipe(Effect.provide(Layer.merge(NodeServices.layer, Storage.layerMemory))),
  )
}

describe('provider accounting boundaries', () => {
  it.effect(
    'legacy affinity is acquired once before generation and unknown history stays unknown',
    () =>
      memory(
        Effect.gen(function* () {
          const storage = yield* Storage.Storage
          yield* storage.commit([
            { _tag: 'conversation', value: { id: root } },
            {
              _tag: 'entry',
              value: {
                id: Record.EntryId.make(yield* storage.mintId()),
                conversationId: root,
                kind: 'assistant',
              },
            },
          ])
          const identities: Array<string> = []
          const native = yield* LanguageModel.make({
            generateText: () =>
              Effect.gen(function* () {
                const records = yield* storage
                  .scanDocuments({
                    scope: { _tag: 'conversation', conversationId: root },
                    kind: 'harness.provider',
                  })
                  .pipe(Stream.runCollect, Effect.orDie)
                assert.lengthOf(records, 1)
                return answer
              }),
            streamText: () => Stream.empty,
          })
          const runtime = yield* HarnessRuntime.make({
            models: [definition(native, identities, null)],
          })
          yield* runtime.backend.root
          assert.lengthOf(
            yield* storage
              .scanDocuments({
                scope: { _tag: 'conversation', conversationId: root },
                kind: 'harness.provider',
              })
              .pipe(Stream.runCollect),
            0,
          )
          yield* submit(runtime)
          yield* submit(runtime)
          assert.strictEqual(identities[0], identities[1])
          const summary = yield* Session.usage(runtime.session)
          assert.strictEqual(summary.legacyRecords, 1)
          assert.strictEqual(summary.unpricedModels, 2)
          assert.deepEqual(summary.costs, [])
        }),
      ),
  )

  it.effect('distinct durable sessions cannot reuse integer conversation IDs as affinity', () =>
    Effect.gen(function* () {
      const create = memory(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          return Option.getOrThrow(yield* affinity(session))
        }),
      )
      assert.notStrictEqual(yield* create, yield* create)
    }),
  )

  it.effect('caught initializer failure rolls back affinity along with creation', () =>
    memory(
      Effect.gen(function* () {
        const session = yield* Session.make({
          initializers: [ConversationInitializer.make({ execute: () => Effect.fail('reject') })],
        })
        yield* Session.commit(session, (tx) =>
          Transaction.ensureRoot(tx).pipe(Effect.catch(() => Effect.void)),
        )
        assert.isTrue(Option.isNone(yield* Session.conversation(session, root)))
        assert.deepEqual(
          yield* Session.scanDocuments(session, {
            scope: { _tag: 'conversation', conversationId: root },
          }).pipe(Stream.runCollect),
          [],
        )
      }),
    ),
  )

  it.effect(
    'unknown usage is absent even with caller pricing; partial counters poison only their aggregate',
    () =>
      memory(
        Effect.gen(function* () {
          let calls = 0
          const native = yield* LanguageModel.make({
            generateText: () =>
              Effect.sync(() =>
                ++calls === 1 ? answer : [Response.makePart('text', { text: 'no usage' })],
              ),
            streamText: () => Stream.empty,
          })
          const runtime = yield* HarnessRuntime.make({ models: [definition(native)] })
          yield* runtime.backend.root
          yield* submit(runtime)
          yield* submit(runtime)
          const report = yield* Session.usage(runtime.session)
          assert.deepEqual(report.models[0]?.tokens, {})
          assert.strictEqual(report.unpricedModels, 1)
          assert.strictEqual(report.costs[0]?.model, 0.2)
          assert.deepEqual(
            (yield* own(runtime))
              .filter((entry) => entry.usage?._tag === 'model')
              .map((entry) => (entry.usage?._tag === 'model' ? entry.usage.tokens : undefined)),
            [
              { input: 10, inputUncached: 6, cacheRead: 4, output: 5, outputText: 3, reasoning: 2 },
              {},
            ],
          )
        }),
      ),
  )

  it.effect('model failure and failed response commit never publish usage', () =>
    memory(
      Effect.gen(function* () {
        const real = yield* Storage.Storage
        let reject = false
        const storage = Storage.Storage.of({
          ...real,
          commit: (writes) =>
            reject &&
            Array.from(writes).some(
              (write) => write._tag === 'entry' && write.value.kind === 'assistant',
            )
              ? Effect.fail(
                  new StorageError({
                    reason: 'io',
                    operation: 'fixture.commit',
                    message: 'reject response',
                  }),
                )
              : real.commit(writes),
        })
        const native = yield* LanguageModel.make({
          generateText: () =>
            Effect.sync(() => {
              reject = true
              return answer
            }),
          streamText: () => Stream.empty,
        })
        const runtime = yield* HarnessRuntime.make({ models: [definition(native)] }).pipe(
          Effect.provideService(Storage.Storage, storage),
        )
        yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        const result = yield* runtime.backend.wait(job.id)
        assert.strictEqual(result.status, 'unanswered')
        assert.deepEqual((yield* Session.usage(runtime.session)).models, [])
        assert.isFalse((yield* own(runtime)).some((entry) => entry.usage !== undefined))
      }),
    ),
  )

  for (const failed of [false, true]) {
    it.effect(
      `reported ${failed ? 'failed' : 'successful'} tool spend is durable and not replayed after its checkpoint`,
      () =>
        memory(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>()
            const tools = Toolkit.make(Tool.makeResult('__proto__', { replay: 'safe' }))
            let executions = 0
            let block = true
            const native = yield* LanguageModel.make({
              generateText: ({ prompt }) => {
                if (prompt.content.some((message) => message.role === 'tool'))
                  return block
                    ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
                    : Effect.succeed(answer)
                return Effect.succeed([
                  Response.makePart('tool-call', {
                    id: 'tool-1',
                    name: '__proto__',
                    params: {},
                    providerExecuted: false,
                  }),
                  finish,
                ])
              },
              streamText: () => Stream.empty,
            })
            const layer = tools.toLayer({
              ['__proto__']: () => {
                executions++
                const output = {
                  content: [],
                  spend: { amount: 2, currency: 'EUR', source: 'tool-invoice-1' },
                }
                return failed ? Effect.fail(output) : Effect.succeed(output)
              },
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const open = HarnessRuntime.make({ models: [definition(native)], tools }).pipe(
              Effect.provide(layer),
            )
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* first.backend.root
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            const before = yield* Session.usage(first.session)
            assert.deepEqual(before.tools, [{ name: '__proto__', results: 1 }])
            assert.deepEqual(before.costs, [
              { currency: 'USD', model: 0.2, tool: 0 },
              { currency: 'EUR', model: 0, tool: 2 },
            ])
            const receipt = (yield* own(first)).find((entry) => entry.kind === 'tool.result')!
            assert.notInclude(JSON.stringify(receipt.model), 'tool-invoice')
            yield* Scope.close(firstScope, Exit.void)
            block = false
            const second = yield* open
            yield* second.backend.root
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
            assert.strictEqual(executions, 1)
            assert.deepEqual((yield* Session.usage(second.session)).tools, before.tools)
            assert.strictEqual(
              (yield* Session.usage(second.session)).costs.find((cost) => cost.currency === 'EUR')
                ?.tool,
              2,
            )
          }),
        ),
    )
  }
})

describe('accounting for omitted history and failed attempts', () => {
  it.effect('context omission cannot erase spend and prices stay snapshotted', () =>
    memory(
      Effect.gen(function* () {
        const declared = { ...pricing }
        const native = yield* LanguageModel.make({
          generateText: () => Effect.succeed(answer),
          streamText: () => Stream.empty,
        })
        const runtime = yield* HarnessRuntime.make({
          models: [definition(native, undefined, declared)],
        })
        yield* runtime.backend.root
        yield* submit(runtime)
        const before = yield* Session.usage(runtime.session)
        const entry = (yield* own(runtime)).find((entry) => entry.kind === 'assistant')!
        declared.inputToken = 99
        yield* Session.commit(runtime.session, (tx) =>
          Transaction.appendEntry(tx, root, {
            kind: 'context.edit',
            edits: [{ _tag: 'omit', target: entry.id }],
            head: 'self',
          }),
        )
        assert.deepEqual(yield* Session.usage(runtime.session), before)
        const saved = (yield* own(runtime)).find((current) => current.id === entry.id)!.usage
        assert.strictEqual(saved?._tag, 'model')
        if (saved?._tag === 'model') assert.strictEqual(saved.pricing?.inputToken, 0.01)
      }),
    ),
  )
  it.effect(
    'a failed generation following a committed continuation counts only the first response',
    () =>
      memory(
        Effect.gen(function* () {
          let requests = 0
          const native = yield* LanguageModel.make({
            generateText: () =>
              ++requests === 1
                ? Effect.succeed(answer)
                : Effect.die('provider failed before response'),
            streamText: () => Stream.empty,
          })
          const hook = Hook.make({
            event: 'onYield',
            execute: () =>
              Effect.succeed({
                _tag: 'continue' as const,
                input: Prompt.userMessage({ content: [Prompt.textPart({ text: 'continue' })] }),
              }),
          })
          const runtime = yield* HarnessRuntime.make({
            models: [definition(native)],
            hooks: [hook],
          })
          yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
          assert.strictEqual((yield* Session.usage(runtime.session)).models[0]?.responses, 1)
          assert.strictEqual((yield* Session.usage(runtime.session)).costs[0]?.model, 0.2)
        }),
      ),
  )
  it.effect('unpriced native defaults keep unknown model identity and real counters', () =>
    memory(
      Effect.gen(function* () {
        const native = yield* LanguageModel.make({
          generateText: () => Effect.succeed(answer),
          streamText: () => Stream.empty,
        })
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(LanguageModel.LanguageModel, native),
        )
        yield* runtime.backend.root
        yield* submit(runtime)
        const summary = yield* Session.usage(runtime.session)
        assert.isUndefined(summary.models[0]?.model)
        assert.strictEqual(summary.models[0]?.tokens.reasoning, 2)
        assert.strictEqual(summary.unpricedModels, 1)
        assert.deepEqual(summary.costs, [])
      }),
    ),
  )
})
