import { awaitTransition } from './ModeledWorkflow.ts'
import * as Option from 'effect/Option'
import * as Structured from '@effect-harness/durable/workflow/Structured'
import * as Identity from '@effect-harness/durable/Identity'
import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Harness from '@effect-harness/harness/Executor'
import { HookError, HookFailure } from '@effect-harness/harness/HookError'
import { ToolError, ToolExecution } from '@effect-harness/harness/ToolError'
import { type RegistryError } from '@effect-harness/harness/RegistryError'
import * as Hook from '@effect-harness/harness/Hook'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Registry from '@effect-harness/harness/Registry'
import * as Tool from '@effect-harness/harness/Tool'
import * as ToolContent from '@effect-harness/harness/ToolResult'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as AiError from 'effect/ai/AiError'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Executor from '@effect-harness/durable/Executor'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Directory from '@effect-harness/durable/SessionDirectory'
import * as Memory from '@effect-harness/durable/storage/Memory'
import * as Store from '@effect-harness/durable/Store'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import { Compaction } from '@effect-harness/durable/workflow/Compaction'
import * as CompactionExecutor from '@effect-harness/durable/workflow/CompactionExecutor'
import { ToolCall } from '@effect-harness/durable/workflow/ToolCall'
import * as ToolExecutor from '@effect-harness/durable/workflow/ToolExecutor'

const ref = { provider: 'custom', modelId: 'model' }
const finish = (reason: Response.FinishReason = 'stop'): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { uncached: 1, total: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const answer: Response.StreamPartEncoded[] = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'done' },
  { type: 'text-end', id: 'answer' },
  finish(),
]
const error = () =>
  new AiError.AiError({
    module: 'test',
    method: 'request',
    reason: new AiError.InvalidRequestError({ description: 'provider-specific-transient' }),
  })
const input = (requestId: string) => ({
  sessionId: Identity.SessionId.make('parity'),
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId: Identity.RequestId.make(requestId),
  submission: {
    _tag: 'input' as const,
    type: 'input' as const,
    message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'request' })] }),
  },
})
const runtime = (
  descriptor: Model.Descriptor,
  registry: Layer.Layer<Registry.Registry, RegistryError>,
  config = Conversation.layerConfiguration({
    settings: {
      compaction: { enabled: false },
      retry: { enabled: true, maxRetries: 1, baseDelayMs: 0 },
    },
  }),
) => {
  const catalog = Model.layer([descriptor])
  const creation = Conversation.layerCreation.pipe(
    Layer.provide(config),
    Layer.provide(BunCrypto.layer),
  )
  const session = Session.layer.pipe(Layer.provideMerge(Memory.layer), Layer.provide(creation))
  return Executor.layer.pipe(
    Layer.provideMerge(WorkflowEngine.layerMemory),
    Layer.provideMerge(
      Directory.layerSingle(Identity.SessionId.make('parity')).pipe(Layer.provideMerge(session)),
    ),
    Layer.provideMerge(config),
    Layer.provideMerge(catalog),
    Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalog)))),
  )
}
const selectModel = (session: Session.Service) =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID })
      agent.model = ref
    }),
  )
const descriptor = (model: NativeModel.LanguageModel): Model.Descriptor => ({
  ref,
  model,
  contextWindow: 100000,
  maxOutputTokens: 1000,
  configure: () => Effect.succeed(Context.empty()),
})

describe('GenerationParity', () => {
  for (const outcome of ['recover', 'exhaust', 'later-input'] as const) {
    it.effect(
      `records rejected tool responses and handles ${outcome} without inventing calls`,
      () =>
        Effect.gen(function* () {
          const requests: Prompt.Prompt[] = []
          let executions = 0
          const toolkit = Toolkit.make(
            AiTool.make('known', {
              parameters: Schema.Struct({ text: Schema.String }),
              success: Schema.String,
            }),
          )
          const tools = yield* Tool.bind(toolkit, { known: { replay: 'safe' } }).pipe(
            Effect.provide(
              toolkit.toLayer({
                known: ({ text }) =>
                  Effect.sync(() => {
                    executions++
                    return text
                  }),
              }),
            ),
          )
          const native = yield* NativeModel.make({
            generateText: () => Effect.succeed([]),
            streamText: ({ prompt }) => {
              requests.push(prompt)
              const corrected = prompt.content.some(
                (message) =>
                  message.role === 'user' &&
                  message.content.some(
                    (part) =>
                      part.type === 'text' &&
                      part.text.includes('Your previous response could not be validated'),
                  ),
              )
              return Stream.fromIterable<Response.StreamPartEncoded>(
                corrected && outcome !== 'exhaust'
                  ? answer
                  : [
                      { type: 'text-start', id: 'partial' },
                      { type: 'text-delta', id: 'partial', delta: 'incomplete response' },
                      {
                        type: 'tool-call',
                        id: 'rejected-call',
                        name: 'unoffered',
                        params: { text: 'hello' },
                        providerExecuted: false,
                      },
                      finish('tool-calls'),
                    ],
              ).pipe(Stream.rechunk(1))
            },
          })
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            yield* session.root()
            yield* selectModel(session)
            const payload = input(`invalid-tool-${outcome}`)
            const receipt = yield* awaitTransition(Submission.execute(payload))
            assert.strictEqual(receipt.status, outcome === 'recover' ? 'done' : 'unanswered')
            if (receipt._tag === 'InputUnanswered')
              assert.strictEqual(receipt.reason, 'model_error')
            assert.strictEqual(requests.length, outcome === 'later-input' ? 1 : 2)
            assert.strictEqual(executions, 0)
            const view = yield* Conversation.context(session, Record.ROOT_CONVERSATION_ID)
            const failures = view.entries.filter((entry) => entry.status === 'error')
            assert.strictEqual(failures.length, outcome === 'exhaust' ? 2 : 1)
            for (const entry of failures) {
              assert.ok(AiError.isAiError(entry.error))
              assert.strictEqual(entry.error.reason._tag, 'InvalidOutputError')
              assert.strictEqual(entry.error.module, 'LanguageModel')
              assert.strictEqual(entry.error.method, 'streamText')
              assert.ok(
                entry.messages?.some(
                  (message) =>
                    message.role === 'assistant' &&
                    message.content.some(
                      (part) => part.type === 'text' && part.text === 'incomplete response',
                    ),
                ),
              )
            }
            assert.isFalse(
              view.messages.some(
                (message) =>
                  message.role === 'assistant' &&
                  message.content.some(
                    (part) => part.type === 'text' && part.text === 'incomplete response',
                  ),
              ),
            )
            assert.isFalse(view.messages.some((message) => message.role === 'tool'))
            assert.isFalse(
              view.messages.some(
                (message) =>
                  message.role === 'assistant' &&
                  message.content.some((part) => part.type === 'tool-call'),
              ),
            )
            const beforeReplay = requests.length
            assert.deepStrictEqual(yield* awaitTransition(Submission.execute(payload)), receipt)
            assert.strictEqual(requests.length, beforeReplay)
            if (outcome === 'later-input') {
              const next = yield* awaitTransition(Submission.execute(input('after-invalid-tool')))
              assert.strictEqual(next.status, 'done')
              assert.strictEqual(requests.length, 2)
              assert.strictEqual(executions, 0)
            }
          }).pipe(
            Effect.provide(
              runtime(
                descriptor(native),
                Registry.layer([{ name: 'tools', tools }]),
                Conversation.layerConfiguration({
                  settings: {
                    compaction: { enabled: false },
                    retry: { enabled: outcome !== 'later-input', maxRetries: 1, baseDelayMs: 0 },
                  },
                }),
              ),
            ),
          )
        }),
    )
  }
  it.effect(
    'replays a committed preparation before changed model/section planning when the native cache is lost',
    () =>
      Effect.gen(function* () {
        const committed = yield* Deferred.make<void>()
        let recovering = false
        let renders = 0
        const requested: Prompt.Prompt[] = []
        const native = yield* NativeModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) => {
            requested.push(options.prompt)
            return Stream.fromIterable(answer)
          },
        })
        const catalog = Model.layer([descriptor(native)])
        const registry = Registry.layer([
          {
            name: 'sections',
            sections: [
              {
                key: 'saved',
                render: () => {
                  renders++
                  return recovering
                    ? Effect.die('changed renderer must not run')
                    : Effect.succeed('original pinned section')
                },
              },
            ],
          },
        ])
        const config = Conversation.layerConfiguration({
          settings: { compaction: { enabled: false } },
        })
        const underlying = yield* Memory.make
        const original = yield* Session.make.pipe(
          Effect.provideService(Store.Store, underlying),
          Effect.provide(
            Conversation.layerCreation.pipe(Layer.provide(config), Layer.provide(BunCrypto.layer)),
          ),
        )
        yield* original.root()
        yield* selectModel(original)
        // This observes a real domain commit and then withholds the native Activity reply.
        // The second normal memory engine deliberately has no first-engine Activity cache.
        const transaction = <A extends Record.Json | void, E, R>(
          change: (tx: Session.Transaction) => Effect.Effect<A, E, R>,
          options: Store.CommitOptions = {},
        ) =>
          original
            .transaction(change, options)
            .pipe(
              Effect.tap(() =>
                options.key?.startsWith('workflow/generation/prepare/') && !recovering
                  ? Deferred.succeed(committed, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
              ),
            )
        const intercepted = new Proxy(original, {
          get: (target, key, receiver) =>
            key === 'transaction' ? transaction : Reflect.get(target, key, receiver),
        })
        const common = yield* Layer.build(
          Layer.mergeAll(
            Directory.layer.pipe(
              Layer.provide(
                Layer.succeed(
                  Directory.Registrations,
                  new Map([[Identity.SessionId.make('parity'), intercepted]]),
                ),
              ),
            ),
            config,
            catalog,
            Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalog))),
          ),
        )
        const executors = Executor.layer.pipe(
          Layer.provideMerge(WorkflowEngine.layerMemory),
          Layer.provide(Layer.succeedContext(common)),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* Submission.execute(input('pin-gap')).pipe(Effect.forkScoped)
            yield* Deferred.await(committed).pipe(Effect.timeout('3 seconds'))
            const state = yield* original.committed
            assert.ok(
              state.receipts.some((receipt) =>
                receipt.key.startsWith('workflow/generation/prepare/'),
              ),
            )
            assert.strictEqual(requested.length, 0)
          }).pipe(Effect.provide(executors)),
        )
        recovering = true
        yield* original.transaction(
          Effect.fnUntraced(function* (tx) {
            const agent = yield* tx.doc(Conversation.AgentDoc, {
              owner: Record.ROOT_CONVERSATION_ID,
            })
            agent.model = { provider: 'removed', modelId: 'removed' }
          }),
        )
        const receipt = yield* awaitTransition(Submission.execute(input('pin-gap'))).pipe(
          Effect.provide(executors),
          Effect.timeout('3 seconds'),
        )
        assert.strictEqual(receipt.status, 'done')
        assert.strictEqual(renders, 1)
        assert.strictEqual(requested.length, 1)
        assert.ok(
          requested[0]?.content.some(
            (message) =>
              message.role === 'system' && message.content.includes('original pinned section'),
          ),
        )
      }),
  )

  for (const failure of ['typed', 'eof'] as const) {
    it.effect(
      `descriptor classifier retries ${failure} generation and repairs legacy affinity before requests`,
      () =>
        Effect.gen(function* () {
          const calls = yield* Ref.make(0)
          const classifications: unknown[] = []
          const affinities: string[] = []
          let session: Session.Service | undefined
          const native = yield* NativeModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const call = yield* Ref.updateAndGet(calls, (n) => n + 1)
                  if (call === 1) return failure === 'typed' ? Stream.fail(error()) : Stream.empty
                  return Stream.fromIterable(answer)
                }),
              ),
          })
          const model: Model.Descriptor = {
            ...descriptor(native),
            classify: (value) => {
              classifications.push(value)
              return { retryable: true, overflow: false }
            },
            configure: (options) =>
              Effect.gen(function* () {
                assert.ok(session)
                const stored = yield* session
                  .snapshot(Conversation.ProviderDoc, { owner: Record.ROOT_CONVERSATION_ID })
                  .pipe(Effect.map(Option.getOrUndefined), Effect.orDie)
                assert.strictEqual(stored?.value.sessionId, options.sessionId)
                assert.match(options.sessionId ?? '', /^[0-9a-f]{8}-[0-9a-f]{4}-7/)
                affinities.push(options.sessionId!)
                return Context.empty()
              }),
          }
          yield* Effect.gen(function* () {
            session = yield* Session.Session
            yield* session.root()
            yield* selectModel(session)
            yield* session.transaction((tx) =>
              tx.retire(Conversation.ProviderDoc, { owner: Record.ROOT_CONVERSATION_ID }),
            )
            assert.isUndefined(
              yield* session
                .snapshot(Conversation.ProviderDoc, {
                  owner: Record.ROOT_CONVERSATION_ID,
                })
                .pipe(Effect.map(Option.getOrUndefined)),
            )
            const receipt = yield* awaitTransition(
              Submission.execute(input(`classifier-${failure}`)),
            )
            assert.strictEqual(receipt.status, 'done')
            assert.strictEqual(yield* Ref.get(calls), 2)
            assert.ok(classifications.length >= 1)
            assert.strictEqual(new Set(affinities).size, 1)
            if (receipt.status !== 'done') return yield* Effect.die('Expected completed input')
            const fork = yield* session.transaction((tx) =>
              tx.forkConversation(Record.ROOT_CONVERSATION_ID, receipt.answer!, {
                ownership: { _tag: 'ownerless' as const, kind: 'ownerless' },
              }),
            )
            assert.notStrictEqual(
              (yield* session
                .snapshot(Conversation.ProviderDoc, { owner: fork.id })
                .pipe(Effect.map(Option.getOrUndefined)))?.value.sessionId,
              affinities[0],
            )
          }).pipe(Effect.provide(runtime(model, Registry.layer([]))))
        }),
    )
  }
  it.effect(
    'a policy update during a failed request disables retry without changing the pinned request',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        let settings: Conversation.Configuration['Service'] | undefined
        const native = yield* NativeModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Ref.update(calls, (n) => n + 1)
                assert.ok(settings)
                yield* settings
                  .updateSettings({ retry: { enabled: false }, compaction: { enabled: false } })
                  .pipe(Effect.orDie)
                return Stream.fail(error())
              }),
            ),
        })
        yield* Effect.gen(function* () {
          settings = yield* Conversation.Configuration
          const session = yield* Session.Session
          yield* session.root()
          yield* selectModel(session)
          const receipt = yield* awaitTransition(Submission.execute(input('policy-update')))
          assert.strictEqual(receipt.status, 'unanswered')
          assert.strictEqual(yield* Ref.get(calls), 1)
          const invalid = yield* Effect.result(
            settings.updateSettings({ retry: { maxRetries: -1 } }),
          )
          assert.strictEqual(invalid._tag, 'Failure')
          assert.strictEqual(settings.settings.retry.enabled, false)
        }).pipe(
          Effect.provide(
            runtime(
              { ...descriptor(native), classify: () => ({ retryable: true, overflow: false }) },
              Registry.layer([]),
            ),
          ),
        )
      }),
  )
  it.effect(
    'legacy compaction commits affinity before its first request and uses its custom retry classifier',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        const ids: string[] = []
        const classifications: unknown[] = []
        let session: Session.Service | undefined
        const native = yield* NativeModel.make({
          generateText: () =>
            Effect.gen(function* () {
              const call = yield* Ref.updateAndGet(calls, (n) => n + 1)
              if (call === 1) return yield* error()
              return [{ type: 'text', text: 'compact summary' }, finish()]
            }),
          streamText: () => Stream.empty,
        })
        const model = {
          ...descriptor(native),
          classify: (value: unknown) => {
            classifications.push(value)
            return { retryable: true, overflow: false }
          },
          configure: (options: Model.RequestOptions) =>
            Effect.gen(function* () {
              assert.ok(session)
              const stored = yield* session
                .snapshot(Conversation.ProviderDoc, { owner: Record.ROOT_CONVERSATION_ID })
                .pipe(Effect.map(Option.getOrUndefined), Effect.orDie)
              assert.strictEqual(stored?.value.sessionId, options.sessionId)
              ids.push(options.sessionId!)
              return Context.empty()
            }),
        }
        const config = Conversation.layerConfiguration({
          settings: {
            retry: { enabled: true, maxRetries: 1, baseDelayMs: 0 },
            compaction: { enabled: true, keepRecentTokens: 0 },
          },
        })
        yield* Effect.gen(function* () {
          session = yield* Session.Session
          yield* session.root()
          yield* selectModel(session)
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              yield* tx.retire(Conversation.ProviderDoc, { owner: Record.ROOT_CONVERSATION_ID })
              for (let n = 0; n < 3; n++) {
                const user = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Message))(
                  Prompt.userMessage({ content: [Prompt.textPart({ text: `old user ${n}` })] }),
                )
                const assistant = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Message))(
                  Prompt.assistantMessage({
                    content: [Prompt.textPart({ text: `old assistant ${n}` })],
                  }),
                )
                yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
                  kind: 'harness.user',
                  model: [user],
                })
                yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
                  kind: 'harness.assistant',
                  model: [assistant],
                })
              }
            }),
          )
          const payload = yield* session.transaction((tx) =>
            CompactionExecutor.make(
              tx,
              Identity.SessionId.make('parity'),
              Record.ROOT_CONVERSATION_ID,
              'manual',
            ),
          )
          const result = yield* awaitTransition(Compaction.execute(payload))
          assert.ok(
            result.entryId !== undefined || result.submissionId !== undefined,
            JSON.stringify(result),
          )
          assert.strictEqual(yield* Ref.get(calls), 2)
          assert.strictEqual(new Set(ids).size, 1)
          assert.match(ids[0] ?? '', /^[0-9a-f]{8}-[0-9a-f]{4}-7/)
          assert.ok(classifications.length >= 1)
        }).pipe(Effect.provide(runtime(model, Registry.layer([]), config)))
      }),
  )
  it.effect(
    'parallel afterTools sees call-ordered committed entries through the typed current invocation',
    () =>
      Effect.gen(function* () {
        const fast = yield* Deferred.make<void>()
        const calls = yield* Ref.make(0)
        const reports: unknown[] = []
        const seen: Hook.SettledTool[] = []
        const finished: string[] = []
        const file = Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) })
        const tools = Toolkit.make(
          ...['slow', 'fast', 'broken'].map((name) =>
            AiTool.make(name, {
              parameters: Schema.Struct({}),
              success: Invocation.Result,
              failure: ToolError,
            }),
          ),
        )
        const bound = yield* Tool.bind(tools, {
          slow: { project: (value) => Tool.decodeResult('fixture', value) },
          fast: { project: (value) => Tool.decodeResult('fixture', value) },
        }).pipe(
          Effect.provide(
            tools.toLayer({
              slow: () =>
                Deferred.await(fast).pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      finished.push('slow')
                      return {
                        content: [
                          Prompt.textPart({ text: 'slow before' }),
                          file,
                          Prompt.textPart({ text: 'slow after' }),
                        ],
                        details: { private: 'private' },
                        diagnostics: [{ kind: 'notice', message: 'visible diagnostic' }],
                      }
                    }),
                  ),
                ),
              fast: () =>
                Effect.sync(() => {
                  finished.push('fast')
                  return { content: [Prompt.textPart({ text: 'fast' })] }
                }),
              broken: () =>
                Effect.fail(
                  new ToolError({
                    reason: new ToolExecution({ name: 'broken', message: 'broken handler' }),
                  }),
                ),
            }),
          ),
        )
        const handlers = yield* Hook.bind(
          {
            afterTools: (results) =>
              Effect.gen(function* () {
                const current = yield* Ownership.Current
                yield* current.check
                for (const result of results) {
                  const entry = yield* current.session
                    .entry(
                      yield* Schema.decodeEffect(Record.EntryId)(result.entryId),
                      current.conversationId,
                    )
                    .pipe(Effect.map(Option.getOrUndefined))
                  assert.isDefined(entry)
                  assert.strictEqual(entry?.entry.kind, 'harness.tool')
                  seen.push(result)
                }
              }).pipe(
                Effect.mapError(
                  (error) =>
                    new HookError({
                      reason: new HookFailure({ message: error.message, cause: error }),
                    }),
                ),
              ),
          },
          [Ownership.Current],
        )
        const native = yield* NativeModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.updateAndGet(calls, (n) => n + 1).pipe(
                Effect.map((call) =>
                  call === 1
                    ? Stream.fromIterable<Response.StreamPartEncoded>([
                        ...['slow', 'fast', 'broken'].map((name) => ({
                          type: 'tool-call' as const,
                          id: name,
                          name,
                          params: {},
                          providerExecuted: false,
                        })),
                        finish('tool-calls'),
                      ])
                    : Stream.fromIterable(answer),
                ),
              ),
            ),
        })
        const registry = Registry.layer([
          { name: 'tools', tools: bound, hooks: [{ operation: 'generation', handlers }] },
        ])
        yield* Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          yield* selectModel(session)
          // The committed ledger, rather than handler return or elapsed time, proves entry ID order.
          yield* Effect.gen(function* () {
            while (true) {
              const state = yield* session.committed
              if (
                state.entries.some(
                  ({ entry }) =>
                    entry.kind === 'harness.tool' &&
                    Schema.is(Schema.Struct({ callId: Schema.Literal('fast') }))(entry.data),
                )
              ) {
                yield* Deferred.succeed(fast, undefined)
                return
              }
              // Native Workflow execution advances outside the deciding transaction;
              // each positive poll inspects actual committed state before releasing slow.
              // This child poll sleeps on TestClock; the admitted execution's
              // outer awaitTransition is the single clock driver.
              yield* Effect.sleep('1 millis')
            }
          }).pipe(Effect.forkScoped)
          const receipt = yield* awaitTransition(Submission.execute(input('parallel-hooks')))
          assert.strictEqual(receipt.status, 'done', JSON.stringify(receipt))
          assert.deepStrictEqual(finished, ['fast', 'slow'])
          assert.deepStrictEqual(
            seen.map((item) => item.id),
            ['slow', 'fast', 'broken'],
          )
          assert.ok(seen[0]!.entryId > seen[1]!.entryId)
          assert.strictEqual(
            seen[2]?.result.diagnostics?.some((item) => item.kind === 'tool_error'),
            true,
          )
          const view = yield* Conversation.context(session, Record.ROOT_CONVERSATION_ID)
          const modelResult = view.messages.flatMap((message) =>
            message.role === 'tool'
              ? message.content.filter((part) => part.type === 'tool-result' && part.id === 'slow')
              : [],
          )[0]
          assert.ok(modelResult?.type === 'tool-result')
          const content = yield* ToolContent.decode(modelResult.result)
          assert.deepStrictEqual(
            content.content.map((part) => part.type),
            ['text', 'file', 'text', 'text'],
          )
          assert.strictEqual(JSON.stringify(modelResult.result).includes('private'), false)
          assert.deepStrictEqual(reports, [])
        }).pipe(
          Effect.provide(
            runtime(
              descriptor(native),
              registry,
              Conversation.layerConfiguration({
                report: (value) =>
                  Effect.sync(() => {
                    reports.push(value)
                  }),
                settings: { compaction: { enabled: false }, retry: { enabled: false } },
              }),
            ),
          ),
        )
      }),
  )
  for (const mode of ['parallel', 'sequential'] as const)
    it.effect(`configured finite tool admission preserves result order (${mode})`, () =>
      Effect.gen(function* () {
        const names = Array.from({ length: 6 }, (_, index) => `tool${index}`)
        const active = yield* Ref.make(0)
        const peak = yield* Ref.make(0)
        const calls = yield* Ref.make(0)
        const seen = yield* Ref.make<ReadonlyArray<string>>([])
        const toolkit = Toolkit.make(
          ...names.map((name) =>
            AiTool.make(name, {
              parameters: Schema.Struct({}),
              success: Invocation.Result,
              failure: ToolError,
            }),
          ),
        )
        const bound = yield* Tool.bind(
          toolkit,
          Object.fromEntries(
            names.map((name) => [
              name,
              { project: (value: unknown) => Tool.decodeResult(name, value) },
            ]),
          ),
        ).pipe(
          Effect.provide(
            toolkit.toLayer(
              Object.fromEntries(
                names.map((name) => [
                  name,
                  Effect.fnUntraced(
                    function* () {
                      const running = yield* Ref.updateAndGet(active, (count) => count + 1)
                      yield* Ref.update(peak, (count) => Math.max(count, running))
                      yield* Effect.sleep('50 millis')
                      return { content: [Prompt.textPart({ text: name })] }
                    },
                    Effect.ensuring(Ref.update(active, (count) => count - 1)),
                  ),
                ]),
              ),
            ),
          ),
        )
        const hooks = yield* Hook.bind({
          afterTools: (results) =>
            Ref.set(
              seen,
              results.map((result) => result.id),
            ),
        })
        const native = yield* NativeModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.updateAndGet(calls, (count) => count + 1).pipe(
                Effect.map((count) =>
                  count === 1
                    ? Stream.fromIterable<Response.StreamPartEncoded>([
                        ...names.map((name) => ({
                          type: 'tool-call' as const,
                          id: name,
                          name,
                          params: {},
                          providerExecuted: false,
                        })),
                        finish('tool-calls'),
                      ])
                    : Stream.fromIterable(answer),
                ),
              ),
            ),
        })
        yield* Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          yield* selectModel(session)
          const receipt = yield* awaitTransition(Submission.execute(input(`finite-${mode}`)))
          assert.strictEqual(receipt.status, 'done', JSON.stringify(receipt))
          assert.strictEqual(yield* Ref.get(peak), mode === 'sequential' ? 1 : 2)
          assert.strictEqual(yield* Ref.get(active), 0)
          assert.deepStrictEqual(yield* Ref.get(seen), names)
        }).pipe(
          Effect.provide(
            runtime(
              descriptor(native),
              Registry.layer([
                {
                  name: 'tools',
                  tools: bound,
                  hooks: [{ operation: 'generation', handlers: hooks }],
                },
              ]),
              Conversation.layerConfiguration({
                toolConcurrency: 2,
                settings: {
                  toolExecution: mode,
                  compaction: { enabled: false },
                  retry: { enabled: false },
                },
              }),
            ),
          ),
        )
      }),
    )

  // Actual filesystem reads and platform services are outside TestClock.
  it.live(
    'safe tool replay resolves actual relative files against the current conversation cwd',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const host = yield* fs.makeTempDirectoryScoped()
        const current = yield* fs.makeTempDirectoryScoped()
        yield* fs.writeFileString(path.join(host, 'file.txt'), 'wrong host')
        yield* fs.writeFileString(path.join(current, 'file.txt'), 'current conversation')
        const calls = yield* Ref.make(0)
        const read = AiTool.make('read-current', {
          parameters: Schema.Struct({ path: Schema.String }),
          success: Schema.String,
          failure: ToolError,
        }).addDependency(Invocation.Invocation)
        const toolkit = Toolkit.make(read)
        const bound = yield* Tool.bind(toolkit, { 'read-current': { replay: 'safe' } }).pipe(
          Effect.provide(
            toolkit.toLayer({
              'read-current': ({ path: relative }) =>
                Effect.gen(function* () {
                  const invocation = yield* Invocation.Invocation
                  yield* Ref.update(calls, (n) => n + 1)
                  return yield* fs.readFileString(path.join(invocation.cwd, relative)).pipe(
                    Effect.mapError(
                      (error) =>
                        new ToolError({
                          reason: new ToolExecution({
                            name: 'read-current',
                            message: error.message,
                            cause: error,
                          }),
                        }),
                    ),
                  )
                }),
            }),
          ),
        )
        const native = yield* NativeModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.empty,
        })
        yield* Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const payload = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const agent = yield* tx.doc(Conversation.AgentDoc, {
                owner: Record.ROOT_CONVERSATION_ID,
              })
              agent.cwd = current
              const assistant = yield* tx.appendEntry(Record.ROOT_CONVERSATION_ID, {
                kind: 'harness.assistant',
              })
              const taskId = yield* tx.mint(Record.TaskId)
              const payload = {
                sessionId: Identity.SessionId.make('parity'),
                conversationId: Record.ROOT_CONVERSATION_ID,
                taskId,
                generationTaskId: taskId,
                assistantId: assistant.id,
                callId: 'read',
                name: 'read-current',
                arguments: { path: 'file.txt' },
              }
              yield* tx.write({
                _tag: 'task' as const,
                type: 'task',
                value: {
                  id: taskId,
                  conversationId: payload.conversationId,
                  kind: 'harness.tool',
                  version: 1,
                  input: {
                    workflow: ToolCall._tag,
                    executionId: yield* ToolCall.executionId(payload),
                    payload,
                  },
                  background: false,
                  abortRequested: false,
                  state: { status: 'running' },
                },
              })
              const intent = yield* tx.doc(ToolExecutor.IntentDoc, {
                owner: taskId,
                seed: {
                  id: 'read',
                  name: 'read-current',
                  args: payload.arguments,
                  replay: 'safe',
                },
              })
              intent.started = true
              return payload
            }),
          )
          const receipt = yield* ToolCall.execute(payload)
          const entry = yield* session
            .entry(receipt.entryId, payload.conversationId)
            .pipe(Effect.map(Option.getOrUndefined))
          const messages = yield* Schema.decodeEffect(
            Schema.toCodecJson(Schema.Array(Prompt.Message)),
          )(entry?.entry.model ?? [])
          const toolResult = messages.flatMap((message) =>
            message.role === 'tool' ? message.content : [],
          )[0]
          assert.ok(toolResult?.type === 'tool-result')
          assert.deepStrictEqual(
            (yield* ToolContent.decode(toolResult.result)).content.map((part) =>
              part.type === 'text' ? part.text : '',
            ),
            ['current conversation'],
          )
          yield* ToolCall.execute(payload)
          assert.strictEqual(yield* Ref.get(calls), 1)
        }).pipe(
          Effect.provide(
            runtime(
              descriptor(native),
              Registry.layer([{ name: 'tools', tools: bound }]),
              Conversation.layerConfiguration({ cwd: host }),
            ),
          ),
        )
      }).pipe(Effect.provide(NodeServices.layer)),
  )
})

it.effect('preserves terminal projection failure instead of settling an interrupted tool', () =>
  Effect.gen(function* () {
    const native = yield* NativeModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () => Stream.empty,
    })
    const toolkit = Toolkit.make(
      AiTool.make('invalid-projection', {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: ToolError,
      }),
    )
    const tools = yield* Tool.bind(toolkit, {
      'invalid-projection': {
        replay: 'safe',
        project: () => Tool.decodeResult('invalid-projection', { isError: 'invalid' }),
      },
    }).pipe(
      Effect.provide(
        toolkit.toLayer({ 'invalid-projection': () => Effect.succeed('valid native success') }),
      ),
    )
    yield* Effect.gen(function* () {
      const session = yield* Session.Session
      const root = yield* session.root()
      const payload = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const assistant = yield* tx.appendEntry(root.id, { kind: 'test.assistant' })
          const taskId = yield* tx.createTask({
            conversationId: root.id,
            kind: 'harness.tool',
            version: 1,
            input: null,
            background: false,
            abortRequested: false,
            state: { status: 'pending' },
          })
          const payload = {
            sessionId: Identity.SessionId.make('parity'),
            conversationId: root.id,
            taskId,
            generationTaskId: taskId,
            assistantId: assistant.id,
            callId: 'invalid-call',
            name: 'invalid-projection',
            arguments: {},
          }
          yield* Structured.bind(
            tx,
            {
              id: taskId,
              conversationId: root.id,
              kind: 'harness.tool',
              version: 1,
              input: null,
              background: false,
              abortRequested: false,
              state: { status: 'pending' },
            },
            ToolCall,
            payload,
          )
          return payload
        }),
      )
      const failure = yield* awaitTransition(ToolCall.execute(payload)).pipe(Effect.flip)
      assert.strictEqual(failure.reason._tag, 'InvalidState')
      assert.strictEqual(failure.message, 'Tool terminal projection failed')
      assert.strictEqual(
        (yield* session.task(payload.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state.status,
        'running',
      )
      assert.isUndefined(
        (yield* session.task(payload.taskId).pipe(Effect.map(Option.getOrUndefined)))?.state
          .outcome,
      )
    }).pipe(
      Effect.provide(
        runtime(descriptor(native), Registry.layer([{ name: 'invalid-projection', tools }])),
      ),
    )
  }),
)
