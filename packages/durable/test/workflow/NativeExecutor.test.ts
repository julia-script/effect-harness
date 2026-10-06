import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Harness from '@effect-harness/harness/Executor'
import { ToolError } from '@effect-harness/harness/Error'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Registry from '@effect-harness/harness/Registry'
import * as Tool from '@effect-harness/harness/Tool'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as NativeResponse from 'effect/ai/Response'
import * as Stream from 'effect/Stream'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Activity from 'effect/workflow/Activity'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from '../../src/Conversation.ts'
import * as Inbox from '../../src/Inbox.ts'
import * as Ownership from '../../src/Ownership.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as DurableUsage from '../../src/Usage.ts'
import * as Directory from '../../src/SessionDirectory.ts'
import * as Memory from '../../src/storage/Memory.ts'
import { Generation } from '../../src/workflow/Generation.ts'
import { Submission } from '../../src/workflow/Submission.ts'
import * as SubmissionExecutor from '../../src/workflow/SubmissionExecutor.ts'
import {
  ExecutionError,
  ExecutionErrorCodec,
  InvalidState,
} from '../../src/workflow/ExecutionError.ts'
import { ToolCall } from '../../src/workflow/ToolCall.ts'
import * as ToolExecutor from '../../src/workflow/ToolExecutor.ts'
import * as GenerationExecutor from '../../src/workflow/GenerationExecutor.ts'
import * as CompactionExecutor from '../../src/workflow/CompactionExecutor.ts'
import { Compaction } from '../../src/workflow/Compaction.ts'
import * as Cancellation from '../../src/workflow/Cancellation.ts'
import * as Executor from '../../src/Executor.ts'
import { Abort } from '../../src/workflow/Abort.ts'

const workflowSupport: Layer.Layer<Cancellation.Cancellation | Ownership.Declarations> =
  Layer.mergeAll(
    Cancellation.layer,
    Ownership.layerDeclarations([Generation, ToolCall, Compaction]),
  )

const config = Conversation.layerConfiguration()
const services = Session.layer.pipe(
  Layer.provideMerge(Memory.layer),
  Layer.provide(
    Conversation.layerCreation.pipe(Layer.provide(config), Layer.provide(BunCrypto.layer)),
  ),
)
const directory = Directory.layerSingle('native').pipe(Layer.provideMerge(services))
const input = (requestId: string) => ({
  sessionId: 'native',
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId,
  submission: {
    type: 'input' as const,
    message: Prompt.userMessage({ content: [Prompt.textPart({ text: requestId })] }),
  },
})
const write = (requestId: string, text = 'original') => ({
  sessionId: 'native',
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId,
  submission: { type: 'write' as const, entry: { kind: 'passive', data: { text } } },
})

/** A real native Workflow test handler isolates admission/settlement from the model boundary under development. */
const fakeGeneration = (
  started: Effect.Effect<void> = Effect.void,
  proceed: Effect.Effect<void> = Effect.void,
) =>
  Generation.toLayer(
    Effect.fnUntraced(function* (payload) {
      yield* started
      yield* proceed
      const session = yield* (yield* Directory.SessionDirectory)
        .resolve(payload.sessionId)
        .pipe(Effect.mapError(SubmissionExecutor.storageError))
      const settlement = yield* Activity.make({
        name: 'test/answer',
        success: Schema.Struct({
          answer: Record.EntryId,
          inputs: Schema.Array(Record.SubmissionId),
        }),
        error: ExecutionErrorCodec,
        execute: session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              const task = yield* tx.task(payload.taskId)
              if (task === undefined)
                return yield* new ExecutionError({
                  reason: new InvalidState({ message: 'Missing generation' }),
                })
              const live = yield* tx.doc(Inbox.LiveDoc, { owner: payload.conversationId })
              const answer = yield* tx.appendEntry(payload.conversationId, {
                kind: 'test.answer',
                byTaskId: payload.taskId,
              })
              const inputs = yield* Inbox.endRun(tx, live, payload.taskId, {
                status: 'done',
                answer: answer.id,
              })
              yield* tx.write({
                type: 'task',
                value: {
                  ...task,
                  state: { status: 'terminal', outcome: { status: 'answered', answer: answer.id } },
                },
              })
              return { answer: answer.id, inputs }
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              error._tag === 'StorageError' ? SubmissionExecutor.storageError(error) : error,
            ),
          ),
      })
      yield* SubmissionExecutor.notify(session, settlement.inputs)
      return { status: 'answered' as const, answer: settlement.answer }
    }),
  )
const runtime = (generation = fakeGeneration()) =>
  Layer.mergeAll(SubmissionExecutor.layer, generation).pipe(
    Layer.provideMerge(workflowSupport),
    Layer.provideMerge(WorkflowEngine.layerMemory),
    Layer.provideMerge(directory),
    Layer.provide(config),
  )

describe('native submission executor', () => {
  for (const abort of [false, true]) {
    it.live(
      `native deferred provider ${abort ? 'cancels with durable intent' : 'fetches without resending'}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const sends = yield* Ref.make(0)
            const fetches = yield* Ref.make(0)
            const cancels = yield* Ref.make(0)
            const finished = yield* Ref.make(false)
            const native = yield* NativeModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.update(sends, (n) => n + 1).pipe(
                    Effect.as(
                      Stream.fromIterable<NativeResponse.StreamPartEncoded>([
                        {
                          type: 'finish',
                          reason: 'other',
                          usage: {
                            inputTokens: {
                              uncached: 1,
                              total: 1,
                              cacheRead: undefined,
                              cacheWrite: undefined,
                            },
                            outputTokens: { total: 0, text: 0, reasoning: undefined },
                          },
                          response: undefined,
                        },
                      ]),
                    ),
                  ),
                ),
            })
            const response = NativeResponse.makePart('finish', {
              reason: 'stop',
              usage: {
                inputTokens: { uncached: 1, total: 1, cacheRead: undefined, cacheWrite: undefined },
                outputTokens: { total: 1, text: 1, reasoning: undefined },
              },
              response: undefined,
            })
            const catalogue = Model.layer([
              {
                ref: { provider: 'test', modelId: 'model' },
                model: native,
                contextWindow: 100000,
                maxOutputTokens: 1000,
                configure: () => Effect.succeed(Context.empty()),
                deferred: {
                  inspect: (parts) =>
                    parts.some((part) => part.type === 'finish' && part.reason === 'other')
                      ? { handle: { job: 'pinned' }, pollAfterMs: abort ? 60000 : 0 }
                      : undefined,
                  fetch: (handle, options) =>
                    Stream.unwrap(
                      Effect.gen(function* () {
                        assert.deepStrictEqual(handle, { job: 'pinned' })
                        assert.match(options.sessionId ?? '', /^[0-9a-f]{8}-[0-9a-f]{4}-7/)
                        yield* Ref.update(fetches, (n) => n + 1)
                        yield* Ref.set(finished, true)
                        return Stream.fromIterable([
                          NativeResponse.makePart('text-start', { id: 'fetched' }),
                          NativeResponse.makePart('text-delta', {
                            id: 'fetched',
                            delta: 'fetched',
                          }),
                          NativeResponse.makePart('text-end', { id: 'fetched' }),
                          response,
                        ])
                      }),
                    ),
                  cancel: (handle) =>
                    Effect.gen(function* () {
                      assert.deepStrictEqual(handle, { job: 'pinned' })
                      yield* Ref.update(cancels, (n) => n + 1)
                    }),
                },
              },
            ])
            const registry = Registry.layer([])
            const layers = Executor.layer.pipe(
              Layer.provideMerge(WorkflowEngine.layerMemory),
              Layer.provideMerge(directory),
              Layer.provide(config),
              Layer.provideMerge(catalogue),
              Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
            )
            yield* Effect.gen(function* () {
              const session = yield* Session.Session
              yield* session.root()
              yield* session.transaction((tx) =>
                tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID }).pipe(
                  Effect.tap((agent) =>
                    Effect.sync(() => {
                      agent.model = { provider: 'test', modelId: 'model' }
                    }),
                  ),
                ),
              )
              const receipt = yield* Submission.execute(input('deferred')).pipe(Effect.forkScoped)
              if (abort) {
                yield* Effect.gen(function* () {
                  while (
                    (yield* session.snapshot(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID }))
                      ?.value.generation?.deferred === undefined
                  )
                    yield* Effect.sleep('5 millis')
                }).pipe(Effect.timeout('3 seconds'))
                const reached = yield* Abort.execute({
                  sessionId: 'native',
                  requestId: 'deferred-abort',
                  target: { type: 'conversation', id: Record.ROOT_CONVERSATION_ID },
                  background: false,
                }).pipe(Effect.timeout('3 seconds'))
                assert.strictEqual(reached.reached.length, 1)
              }
              const result = yield* Fiber.join(receipt).pipe(Effect.timeout('3 seconds'))
              assert.strictEqual(result.status, abort ? 'unanswered' : 'done')
              assert.strictEqual(yield* Ref.get(sends), 1)
              assert.strictEqual(yield* Ref.get(fetches), abort ? 0 : 1)
              assert.strictEqual(yield* Ref.get(cancels), abort ? 1 : 0)
              assert.strictEqual(yield* Ref.get(finished), !abort)
              assert.isTrue(
                (yield* session.scanTasks({}, 20)).items.every(
                  (task) => task.state.status === 'terminal',
                ),
              )
            }).pipe(Effect.provide(layers))
          }),
        ),
    )
  }
  for (const scenario of [
    'eof-retry',
    'continuation',
    'threshold',
    'overflow',
    'overflow-incomplete',
  ] as const) {
    it.live(`generation preserves native boundaries (${scenario})`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0)
          const summaries = yield* Ref.make(0)
          const yielded = yield* Ref.make<string[]>([])
          const finish = (
            reason: NativeResponse.FinishReason,
          ): NativeResponse.FinishPartEncoded => ({
            type: 'finish',
            reason,
            usage: {
              inputTokens: { uncached: 10, total: 10, cacheRead: undefined, cacheWrite: undefined },
              outputTokens: { total: 5, text: 5, reasoning: undefined },
            },
            response: undefined,
          })
          const native = yield* NativeModel.make({
            generateText: () =>
              Ref.update(summaries, (n) => n + 1).pipe(
                Effect.as([
                  { type: 'text' as const, text: 'summary' },
                  finish(scenario === 'overflow-incomplete' ? 'length' : 'stop'),
                ]),
              ),
            streamText: () =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const call = yield* Ref.updateAndGet(calls, (n) => n + 1)
                  if (call === 1 && scenario === 'eof-retry')
                    return Stream.fromIterable<NativeResponse.StreamPartEncoded>([
                      { type: 'text-start', id: 'text' },
                      { type: 'text-delta', id: 'text', delta: 'partial' },
                    ])
                  if (call === 1 && scenario.startsWith('overflow'))
                    return Stream.fromIterable<NativeResponse.StreamPartEncoded>([
                      { type: 'error', error: 'prompt too long' },
                      finish('error'),
                    ])
                  return Stream.fromIterable<NativeResponse.StreamPartEncoded>([
                    { type: 'text-start', id: 'text' },
                    { type: 'text-delta', id: 'text', delta: `answer-${call}` },
                    { type: 'text-end', id: 'text' },
                    finish('stop'),
                  ])
                }),
              ),
          })
          const catalogue = Model.layer([
            {
              ref: { provider: 'test', modelId: 'model' },
              model: native,
              contextWindow: scenario === 'threshold' ? 10 : 100000,
              maxOutputTokens: 10,
              configure: () => Effect.succeed(Context.empty()),
            },
          ])
          const registry = Registry.layer([
            {
              name: 'hooks',
              hooks: [
                {
                  operation: 'generation',
                  handlers: {
                    onYield: (parts) =>
                      Effect.gen(function* () {
                        const text = parts
                          .flatMap((part) => (part.type === 'text' ? [part.text] : []))
                          .join('')
                        yield* Ref.update(yielded, (texts) => [...texts, text])
                        if (scenario === 'continuation' && text === 'answer-1')
                          return Prompt.userMessage({
                            content: [Prompt.textPart({ text: 'continue' })],
                          })
                        return undefined
                      }),
                  },
                },
              ],
            },
          ])
          const settings = Conversation.layerConfiguration({
            settings: {
              retry: { enabled: true, maxRetries: 1, baseDelayMs: 0 },
              compaction: {
                enabled: scenario === 'threshold' || scenario.startsWith('overflow'),
                keepRecentTokens: 0,
                reserveTokens: 50,
                backgroundTokens: 0,
              },
            },
          })
          const layers = Layer.mergeAll(
            SubmissionExecutor.layer,
            GenerationExecutor.layer,
            ToolExecutor.layer,
            CompactionExecutor.layer,
          ).pipe(
            Layer.provideMerge(workflowSupport),
            Layer.provideMerge(WorkflowEngine.layerMemory),
            Layer.provideMerge(directory),
            Layer.provide(settings),
            Layer.provideMerge(catalogue),
            Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
          )
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            const root = yield* session.root()
            yield* session.transaction(
              Effect.fnUntraced(function* (tx) {
                const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
                agent.model = { provider: 'test', modelId: 'model' }
                if (scenario === 'threshold' || scenario.startsWith('overflow')) {
                  const message = yield* Schema.encodeEffect(
                    Schema.toCodecJson(Prompt.UserMessage),
                  )(
                    Prompt.userMessage({
                      content: [Prompt.textPart({ text: 'an older message to summarize' })],
                    }),
                  )
                  yield* tx.appendEntry(root.id, { kind: 'harness.user', model: [message] })
                }
              }),
            )
            const result = yield* Submission.execute(input(scenario)).pipe(
              Effect.timeout('3 seconds'),
            )
            assert.strictEqual(
              result.status,
              scenario === 'overflow-incomplete' ? 'unanswered' : 'done',
            )
            assert.strictEqual(
              yield* Ref.get(calls),
              scenario === 'threshold' || scenario === 'overflow-incomplete' ? 1 : 2,
            )
            assert.strictEqual(
              yield* Ref.get(summaries),
              scenario === 'threshold' || scenario.startsWith('overflow') ? 1 : 0,
            )
            if (scenario !== 'overflow-incomplete')
              assert.isTrue((yield* Ref.get(yielded)).every((text) => text.startsWith('answer-')))
            const live = yield* session.snapshot(Inbox.LiveDoc, { owner: root.id })
            assert.strictEqual(live?.value.run, undefined)
            assert.strictEqual(live?.value.generation, undefined)
            if (scenario === 'continuation') {
              assert.deepStrictEqual(yield* Ref.get(yielded), ['answer-1', 'answer-2'])
              const view = yield* Conversation.context(session, root.id)
              assert.deepStrictEqual(
                view.messages.map((message) => message.role),
                ['user', 'assistant', 'user', 'assistant'],
              )
              if (result.status === 'done' && result.type === 'input')
                assert.strictEqual(result.answer, view.entries.at(-1)?.id)
            }
            if (scenario === 'overflow-incomplete') {
              assert.strictEqual((yield* Conversation.context(session, root.id)).head, undefined)
              if (result.status === 'unanswered')
                assert.match(JSON.stringify(result.detail), /prompt too long/)
            }
          }).pipe(Effect.provide(layers))
        }),
      ),
    )
  }
  for (const reason of ['stop', 'length'] as const) {
    it.live(
      `compaction accepts only complete summaries (${reason}) and records its own spend`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const requests = yield* Ref.make<Model.RequestOptions[]>([])
            const calls = yield* Ref.make(0)
            const native = yield* NativeModel.make({
              generateText: () =>
                Ref.update(calls, (n) => n + 1).pipe(
                  Effect.as([
                    { type: 'text' as const, text: 'checkpoint summary' },
                    {
                      type: 'finish' as const,
                      reason,
                      usage: {
                        inputTokens: {
                          total: 10,
                          uncached: 10,
                          cacheRead: undefined,
                          cacheWrite: undefined,
                        },
                        outputTokens: { total: 3, text: 3, reasoning: undefined },
                      },
                      response: undefined,
                    },
                  ]),
                ),
              streamText: () => Stream.empty,
            })
            const catalogue = Model.layer([
              {
                ref: { provider: 'test', modelId: 'summary' },
                model: native,
                contextWindow: 100000,
                maxOutputTokens: 1000,
                configure: (options) =>
                  Ref.update(requests, (values) => [...values, options]).pipe(
                    Effect.as(Context.empty()),
                  ),
              },
            ])
            const registry = Registry.layer([])
            const settings = Conversation.layerConfiguration({
              settings: {
                compaction: { keepRecentTokens: 0, reserveTokens: 100 },
                retry: { enabled: false },
              },
            })
            const layers = Layer.mergeAll(SubmissionExecutor.layer, CompactionExecutor.layer).pipe(
              Layer.provideMerge(workflowSupport),
              Layer.provideMerge(WorkflowEngine.layerMemory),
              Layer.provideMerge(directory),
              Layer.provide(settings),
              Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
            )
            yield* Effect.gen(function* () {
              const session = yield* Session.Session
              const root = yield* session.root()
              const payload = yield* session.transaction(
                Effect.fnUntraced(function* (tx) {
                  const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
                  agent.model = { provider: 'test', modelId: 'summary' }
                  const first = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
                    Prompt.userMessage({ content: [Prompt.textPart({ text: 'old' })] }),
                  )
                  const last = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))(
                    Prompt.userMessage({ content: [Prompt.textPart({ text: 'new' })] }),
                  )
                  yield* tx.appendEntry(root.id, { kind: 'harness.user', model: [first] })
                  yield* tx.appendEntry(root.id, { kind: 'harness.user', model: [last] })
                  return yield* CompactionExecutor.create(tx, 'native', root.id, 'manual')
                }),
              )
              const result = yield* Effect.result(Compaction.execute(payload))
              assert.strictEqual(yield* Ref.get(calls), 1)
              const options = (yield* Ref.get(requests))[0]
              assert.strictEqual(options?.maxTokens, 80)
              assert.strictEqual(options?.cache, 'none')
              assert.match(options?.sessionId ?? '', /^[0-9a-f]{8}-[0-9a-f]{4}-7/)
              const ledger = yield* session.snapshot(DurableUsage.UsageDoc, { owner: root.id })
              assert.strictEqual(ledger?.value.models['test/summary']?.totalTokens, 13)
              assert.strictEqual(
                (yield* session.snapshot(Inbox.LiveDoc, { owner: root.id }))?.value.compactions
                  ?.length,
                0,
              )
              if (reason === 'stop') {
                assert.strictEqual(result._tag, 'Success')
                if (result._tag !== 'Success') return
                assert.isDefined(result.success.submissionId)
                const submission = yield* session.submission(result.success.submissionId!)
                assert.strictEqual(submission?.status, 'done')
                const view = yield* Conversation.context(session, root.id)
                assert.isDefined(view.head)
                assert.strictEqual(view.messages.length, 2)
                assert.match(
                  view.messages[0]?.role === 'user' ? JSON.stringify(view.messages[0].content) : '',
                  /checkpoint summary/,
                )
              } else {
                assert.strictEqual(result._tag, 'Failure')
                const view = yield* Conversation.context(session, root.id)
                assert.strictEqual(view.head, undefined)
                assert.strictEqual(view.messages.length, 2)
              }
            }).pipe(Effect.provide(layers))
          }),
        ),
    )
  }
  it.live('real generation uses native AI and a committed child tool round before its answer', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const calls = yield* Ref.make(0)
        const tools = yield* Ref.make(0)
        const requestAffinity = yield* Ref.make<string[]>([])
        const finish = (reason: NativeResponse.FinishReason): NativeResponse.FinishPartEncoded => ({
          type: 'finish',
          reason,
          usage: {
            inputTokens: { uncached: 10, total: 10, cacheRead: undefined, cacheWrite: undefined },
            outputTokens: { total: 5, text: 5, reasoning: undefined },
          },
          response: undefined,
        })
        const nativeModel = yield* NativeModel.make({
          generateText: () => Effect.succeed([{ type: 'text', text: 'unused' }, finish('stop')]),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                const call = yield* Ref.updateAndGet(calls, (n) => n + 1)
                if (call === 1)
                  return Stream.fromIterable<NativeResponse.StreamPartEncoded>([
                    {
                      type: 'tool-call',
                      id: 'call',
                      name: 'echo',
                      params: { text: 'persisted' },
                      providerExecuted: false,
                    },
                    finish('tool-calls'),
                  ])
                return Stream.fromIterable<NativeResponse.StreamPartEncoded>([
                  { type: 'text-start', id: 'text' },
                  { type: 'text-delta', id: 'text', delta: 'answer' },
                  { type: 'text-end', id: 'text' },
                  finish('stop'),
                ])
              }),
            ),
        })
        const declaration = AiTool.make('echo', {
          parameters: Schema.Struct({ text: Schema.String }),
          success: Schema.String,
        })
        const toolkit = Toolkit.make(declaration)
        const bound = yield* Tool.bind(toolkit, { echo: { replay: 'safe' } }).pipe(
          Effect.provide(
            toolkit.toLayer({
              echo: ({ text }) => Ref.update(tools, (n) => n + 1).pipe(Effect.as(text)),
            }),
          ),
        )
        const catalogue = Model.layer([
          {
            ref: { provider: 'test', modelId: 'model' },
            model: nativeModel,
            contextWindow: 100000,
            maxOutputTokens: 10000,
            configure: (options) =>
              Ref.update(requestAffinity, (ids) => [...ids, options.sessionId ?? '']).pipe(
                Effect.as(Context.empty()),
              ),
          },
        ])
        const registry = Registry.layer([{ name: 'test', tools: bound }])
        const layers = Layer.mergeAll(
          SubmissionExecutor.layer,
          GenerationExecutor.layer,
          ToolExecutor.layer,
        ).pipe(
          Layer.provideMerge(workflowSupport),
          Layer.provideMerge(WorkflowEngine.layerMemory),
          Layer.provideMerge(directory),
          Layer.provide(config),
          Layer.provideMerge(catalogue),
          Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
        )
        yield* Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          yield* session.transaction((tx) =>
            tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID }).pipe(
              Effect.tap((agent) =>
                Effect.sync(() => {
                  agent.model = { provider: 'test', modelId: 'model' }
                }),
              ),
            ),
          )
          const result = yield* Submission.execute(input('real'))
          assert.strictEqual(result.status, 'done')
          assert.strictEqual(yield* Ref.get(calls), 2)
          assert.strictEqual(yield* Ref.get(tools), 1)
          const tasks = (yield* session.scanTasks({}, 20)).items
          assert.strictEqual(tasks.length, 3)
          assert.isTrue(tasks.every((task) => task.state.status === 'terminal'))
          const view = yield* Conversation.context(session, Record.ROOT_CONVERSATION_ID)
          assert.deepStrictEqual(
            view.messages.map((message) => message.role),
            ['user', 'system', 'assistant', 'tool', 'assistant'],
          )
          const system = view.entries.find((entry) => entry.system !== undefined)
          assert.ok(system)
          assert.strictEqual(
            view.contributions[view.entries.indexOf(system)]?.some(
              (message) => message.role === 'system',
            ),
            true,
          )
          const ids = yield* Ref.get(requestAffinity)
          assert.strictEqual(ids.length, 2)
          assert.strictEqual(ids[0], ids[1])
          assert.match(ids[0] ?? '', /^[0-9a-f]{8}-[0-9a-f]{4}-7/)
          const ledger = yield* session.snapshot(DurableUsage.UsageDoc, {
            owner: Record.ROOT_CONVERSATION_ID,
          })
          assert.strictEqual(ledger?.value.models['test/model']?.totalTokens, 30)
          assert.strictEqual(ledger?.value.models['test/model']?.cost.known, false)
        }).pipe(Effect.provide(layers))
      }),
    ),
  )
  for (const mode of ['fresh', 'safe-recovery', 'unsafe-recovery'] as const) {
    it.live(`tool intent and terminal result are durable (${mode})`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0)
          const hooks = yield* Ref.make(0)
          const declaration = AiTool.make('echo', {
            parameters: Schema.Struct({ text: Schema.String }),
            success: Schema.String,
            failure: ToolError,
          }).addDependency(Invocation.ToolCall)
          const toolkit = Toolkit.make(declaration)
          const bound = yield* Tool.bind(toolkit, {
            echo: { replay: mode === 'unsafe-recovery' ? 'unsafe' : 'safe' },
          }).pipe(
            Effect.provide(
              toolkit.toLayer({
                echo: ({ text }) =>
                  Effect.gen(function* () {
                    yield* Ref.update(calls, (n) => n + 1)
                    const api = yield* Invocation.ToolCall
                    yield* api.output('progress')
                    yield* api.details({ committed: true })
                    return text
                  }),
              }),
            ),
          )
          const registry = Registry.layer([
            {
              name: 'tools',
              tools: bound,
              hooks: [
                {
                  operation: 'tool',
                  handlers: {
                    beforeTool: () => Ref.update(hooks, (n) => n + 1).pipe(Effect.as(undefined)),
                  },
                },
              ],
            },
          ])
          const native = ToolExecutor.layer.pipe(
            Layer.provideMerge(workflowSupport),
            Layer.provideMerge(WorkflowEngine.layerMemory),
            Layer.provideMerge(directory),
            Layer.provide(config),
            Layer.provide(
              Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, Model.layer([])))),
            ),
          )
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            const root = yield* session.root()
            const payload = yield* session.transaction(
              Effect.fnUntraced(function* (tx) {
                const generationTaskId = yield* tx.createTask({
                  conversationId: root.id,
                  kind: 'test.generation',
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: 'running' },
                })
                const assistant = yield* tx.appendEntry(root.id, { kind: 'test.assistant' })
                const taskId = yield* tx.createTask({
                  conversationId: root.id,
                  owner: generationTaskId,
                  kind: 'harness.tool',
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: 'pending' },
                })
                const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
                live.tools = [{ callId: 'call', name: 'echo', taskId, status: 'pending' }]
                if (mode !== 'fresh') {
                  const intent = yield* tx.doc(ToolExecutor.IntentDoc, {
                    owner: taskId,
                    seed: {
                      id: 'call',
                      name: 'echo',
                      args: { text: 'persisted' },
                      replay: mode === 'safe-recovery' ? 'safe' : 'unsafe',
                    },
                  })
                  intent.started = true
                }
                return {
                  sessionId: 'native',
                  conversationId: root.id,
                  taskId,
                  generationTaskId,
                  assistantId: assistant.id,
                  callId: 'call',
                  name: 'echo',
                  arguments: { text: 'new' },
                }
              }),
            )
            const result = yield* ToolCall.execute(payload)
            assert.strictEqual(result.status, mode === 'unsafe-recovery' ? 'aborted' : 'completed')
            assert.strictEqual(yield* Ref.get(calls), mode === 'unsafe-recovery' ? 0 : 1)
            assert.strictEqual(yield* Ref.get(hooks), mode === 'fresh' ? 1 : 0)
            const entry = yield* session.entry(result.entryId)
            assert.isDefined(entry)
            assert.strictEqual((yield* session.task(payload.taskId))?.state.status, 'terminal')
            assert.isUndefined(
              yield* session.snapshot(ToolExecutor.IntentDoc, { owner: payload.taskId }),
            )
            assert.deepStrictEqual(yield* ToolCall.execute(payload), result)
            assert.strictEqual(yield* Ref.get(calls), mode === 'unsafe-recovery' ? 0 : 1)
            const outcome = (yield* session.task(payload.taskId))?.state.outcome
            const decoded = yield* Schema.decodeEffect(Schema.toCodecJson(ToolExecutor.Outcome))(
              outcome ?? null,
            )
            if (mode === 'safe-recovery')
              assert.strictEqual(decoded.execution.result.content?.[0]?.type, 'text')
          }).pipe(Effect.provide(native))
        }),
      ),
    )
  }
  it.live(
    'settles idle writes through native Workflow and deduplicates before busy/type checks',
    () =>
      Effect.gen(function* () {
        const first = yield* Submission.execute(write('same'))
        const second = yield* Submission.execute(write('same', 'different'))
        assert.deepStrictEqual(second, first)
        assert.strictEqual(first.status, 'done')
        const session = yield* Session.Session
        assert.strictEqual(
          (yield* session.scanEntries({ conversationId: first.conversationId }, 10)).items.length,
          1,
        )
        const conflict = yield* Submission.execute(input('same')).pipe(Effect.flip)
        assert.strictEqual(conflict.reason._tag, 'RequestConflict')
        assert.strictEqual((yield* session.scanSubmissions({}, 10)).items.length, 1)
        const polled = yield* Submission.poll(yield* Submission.executionId(write('same')))
        assert.strictEqual(polled._tag, 'Some')
      }).pipe(Effect.provide(runtime())),
  )

  it.live(
    'starts a normal native generation and wakes the input receipt after domain settlement',
    () =>
      Effect.gen(function* () {
        const receipt = yield* Submission.execute(input('first'))
        assert.strictEqual(receipt.status, 'done')
        assert.isDefined(receipt.entry)
        const session = yield* Session.Session
        assert.deepStrictEqual(
          (yield* session.snapshot(Inbox.LiveDoc, { owner: receipt.conversationId }))?.value,
          {},
        )
        const tasks = (yield* session.scanTasks({}, 10)).items
        assert.strictEqual(tasks.length, 1)
        assert.strictEqual(tasks[0]?.state.status, 'terminal')
        assert.strictEqual(tasks[0]?.kind, 'harness.generation')
        assert.deepStrictEqual(yield* Submission.execute(input('first')), receipt)
      }).pipe(Effect.provide(runtime())),
  )

  it.live('busy reject is atomic while an already settled request still deduplicates', () =>
    Effect.gen(function* () {
      const previous = yield* Submission.execute(write('previous'))
      const session = yield* Session.Session
      yield* session.transaction((tx) =>
        SubmissionExecutor.createGeneration(tx, 'native', previous.conversationId, []),
      )
      const rejected = yield* Submission.execute({
        ...input('reject'),
        submission: { ...input('reject').submission, whenBusy: 'reject' },
      }).pipe(Effect.flip)
      assert.strictEqual(rejected.reason._tag, 'ConversationBusy')
      assert.strictEqual((yield* session.scanSubmissions({}, 10)).items.length, 1)
      assert.deepStrictEqual(yield* Submission.execute(write('previous', 'changed')), previous)
    }).pipe(Effect.provide(runtime())),
  )

  it.live(
    'interrupting the execute caller leaves admitted work running and the durable receipt can be reacquired',
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        const continueGeneration = yield* Deferred.make<void>()
        const native = runtime(
          fakeGeneration(
            Deferred.succeed(started, undefined).pipe(Effect.asVoid),
            Deferred.await(continueGeneration),
          ),
        )
        yield* Effect.gen(function* () {
          const caller = yield* Effect.forkChild(Submission.execute(input('survives')))
          yield* Deferred.await(started)
          yield* Fiber.interrupt(caller)
          const queued = (yield* (yield* Session.Session).scanSubmissions({}, 10)).items[0]
          assert.strictEqual(queued?.status, 'placed')
          yield* Deferred.succeed(continueGeneration, undefined)
          const reacquired = yield* Submission.execute(input('survives'))
          assert.strictEqual(reacquired.status, 'done')
          assert.strictEqual(reacquired.id, queued?.id)
        }).pipe(Effect.provide(native))
      }),
  )
})

const task = (
  id: number,
  options: {
    readonly owner?: number
    readonly background?: boolean
    readonly conversation?: number
    readonly terminal?: boolean
  } = {},
) =>
  Schema.decodeSync(Record.Task)({
    id,
    conversationId: options.conversation ?? 1,
    kind: 'test',
    version: 1,
    input: null,
    ...(options.owner === undefined ? {} : { owner: options.owner }),
    background: options.background ?? false,
    abortRequested: false,
    state: { status: options.terminal ? 'terminal' : 'running' },
  })
describe('ownership domain capabilities', () => {
  it('fences background subtrees and visits ordinary descendants bottom-up', () => {
    const graph = {
      conversations: [
        Schema.decodeSync(Record.Conversation)({ id: 1 }),
        Schema.decodeSync(Record.Conversation)({ id: 6, owner: { conversationId: 1, taskId: 2 } }),
      ],
      tasks: [
        task(2),
        task(3, { owner: 2 }),
        task(4, { owner: 2, background: true }),
        task(5, { owner: 4 }),
        task(7, { conversation: 6 }),
      ],
    }
    assert.deepStrictEqual(
      Ownership.reach(graph, { kind: 'conversation', id: Record.ROOT_CONVERSATION_ID })?.tasks.map(
        (value) => value.id,
      ),
      [3, 7, 2],
    )
    assert.deepStrictEqual(
      Ownership.reach(graph, { kind: 'task', id: Schema.decodeSync(Record.TaskId)(4) })?.tasks.map(
        (value) => value.id,
      ),
      [5, 4],
    )
    assert.deepStrictEqual(
      Ownership.reach(
        graph,
        { kind: 'conversation', id: Record.ROOT_CONVERSATION_ID },
        true,
      )?.tasks.map((value) => value.id),
      [3, 5, 4, 7, 2],
    )
  })

  it.effect(
    'memo uses own keys and replays the first committed value without executing the producer again',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(services)
          const session = Context.get(context, Session.Session)
          const root = yield* session.root()
          const taskId = yield* session.transaction((tx) =>
            tx.createTask({
              conversationId: root.id,
              kind: 'test',
              version: 1,
              input: null,
              background: false,
              abortRequested: false,
              state: { status: 'running' },
            }),
          )
          const count = yield* Ref.make(0)
          const scope = Ownership.layerCurrent(
            { sessionId: 'native', conversationId: root.id, taskId },
            session,
          )
          yield* Effect.gen(function* () {
            const first = yield* Ownership.memo(
              '__proto__',
              Schema.String,
              Ref.update(count, (n) => n + 1).pipe(Effect.as('saved')),
            )
            const second = yield* Ownership.memo(
              '__proto__',
              Schema.String,
              Effect.die('Memo producer must not repeat'),
            )
            assert.strictEqual(first, 'saved')
            assert.strictEqual(second, 'saved')
            assert.strictEqual(yield* Ref.get(count), 1)
          }).pipe(Effect.provide(scope))
          assert.isTrue(Object.hasOwn((yield* session.task(taskId))?.memos ?? {}, '__proto__'))
        }),
      ),
  )
})
