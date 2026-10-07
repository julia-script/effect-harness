import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as BunServices from '@effect/platform-bun/BunServices'
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as DurableExecutor from '@effect-harness/durable/Executor'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Record from '@effect-harness/durable/Record'
import * as Identity from '@effect-harness/durable/Identity'
import * as Session from '@effect-harness/durable/Session'
import * as Directory from '@effect-harness/durable/SessionDirectory'
import * as SqlStore from '@effect-harness/durable/storage/Sqlite'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import * as Harness from '@effect-harness/harness/Executor'
import { ToolError } from '@effect-harness/harness/Error'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Model from '@effect-harness/harness/Model'
import * as Registry from '@effect-harness/harness/Registry'
import * as Tool from '@effect-harness/harness/Tool'
import * as Context from 'effect/Context'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as NativeModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'
import * as Activity from 'effect/workflow/Activity'
import * as Workflow from 'effect/workflow/Workflow'
import * as Database from './Database.ts'

// An application-authored Workflow uses the ordinary native declaration and executor API.
const Greeting = Workflow.make('example/greeting/v1', {
  payload: { name: Schema.String },
  success: Schema.String,
  error: Schema.Never,
  idempotencyKey: ({ name }) => name,
})
const greetingExecutor = Greeting.toLayer(({ name }) =>
  Activity.make({
    name: 'greet',
    success: Schema.String,
    execute: Effect.succeed(`Hello, ${name}`),
  }),
)

const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: 5, reasoning: undefined },
  },
  response: undefined,
})

// This checkout's pinned Effect patch preserves unknown calls only when the
// caller explicitly owns settlement. Ordinary native tool validation stays strict.
const checkUnknownToolBoundary = Effect.gen(function* () {
  const call: Response.ToolCallPartEncoded = {
    type: 'tool-call',
    id: 'unknown-call',
    name: 'unregistered',
    params: { text: 'preserved' },
    providerExecuted: false,
  }
  const model = yield* NativeModel.make({
    generateText: () => Effect.succeed([call, finish('tool-calls')]),
    streamText: () => Stream.fromIterable([call, finish('tool-calls')]),
  })
  const prompt = Prompt.make('Check caller-owned tool settlement')
  // Dynamic names and unknown parameters make this fixture's public response
  // type honest about calls outside its declared tools. Normal tools below
  // retain their precise parameter codecs.
  const declaredName: string = 'declared'
  const toolkit = Toolkit.make(
    AiTool.make(declaredName, { parameters: Schema.Unknown, success: Schema.String }),
  )
  if (
    !(yield* Effect.isFailure(
      model.generateText({ prompt, toolkit, disableToolCallResolution: true }),
    )) ||
    !(yield* Effect.isFailure(
      Stream.runCollect(model.streamText({ prompt, toolkit, disableToolCallResolution: true })),
    )) ||
    !(yield* Effect.isFailure(
      model
        .generateText({ prompt, toolkit, allowUnknownToolCalls: true })
        .pipe(Effect.provide(toolkit.toLayer({ declared: () => Effect.succeed('unused') }))),
    ))
  )
    return yield* Effect.die('Native unknown-tool validation lost its strict default')
  const options = {
    prompt,
    toolkit,
    disableToolCallResolution: true,
    allowUnknownToolCalls: true,
  } as const
  const generated = yield* model.generateText(options)
  const streamed = yield* Stream.runCollect(model.streamText(options))
  const unknownCall = Response.ToolCallPart('unregistered', Schema.Struct({ text: Schema.String }))
  const decoded = yield* Schema.decodeUnknownEffect(unknownCall)(generated.toolCalls[0])
  const streamedCall = yield* Schema.decodeUnknownEffect(unknownCall)(
    streamed.find(Schema.is(unknownCall)),
  )
  if (decoded.params.text !== 'preserved' || streamedCall.params.text !== 'preserved')
    return yield* Effect.die('Native unknown tool name or parameters changed')
})

const main = Effect.gen(function* () {
  yield* checkUnknownToolBoundary
  // Set EXAMPLE_DB to an absolute path to keep the same database across invocations.
  const filename = yield* Database.filename
  const sessionId = yield* Schema.decodeEffect(Identity.SessionId)('example')
  const requestId = yield* Schema.decodeEffect(Identity.RequestId)('uppercase-v1')
  const modelCalls = yield* Ref.make(0)
  const toolCalls = yield* Ref.make(0)
  const native = yield* NativeModel.make({
    generateText: () => Effect.succeed([{ type: 'text', text: 'summary' }, finish('stop')]),
    streamText: () =>
      Stream.unwrap(
        Effect.gen(function* () {
          const call = yield* Ref.updateAndGet(modelCalls, (count) => count + 1)
          if (call === 1)
            return Stream.fromIterable<Response.StreamPartEncoded>([
              {
                type: 'tool-call',
                id: 'uppercase-call',
                name: 'uppercase',
                params: { text: 'hello' },
                providerExecuted: false,
              },
              finish('tool-calls'),
            ])
          return Stream.fromIterable<Response.StreamPartEncoded>([
            { type: 'text-start', id: 'answer' },
            { type: 'text-delta', id: 'answer', delta: 'HELLO' },
            { type: 'text-end', id: 'answer' },
            finish('stop'),
          ])
        }),
      ),
  })
  const descriptor: Model.Descriptor = {
    ref: { provider: 'example', modelId: 'deterministic' },
    model: native,
    contextWindow: 100000,
    maxOutputTokens: 1000,
    configure: () => Effect.succeed(Context.empty()),
  }
  const catalogue = Model.layer([descriptor])
  const uppercase = AiTool.make('uppercase', {
    description: 'Convert text to uppercase without external side effects.',
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    failure: ToolError,
  }).addDependency(Invocation.ToolCall)
  const toolkit = Toolkit.make(uppercase)
  const tools = yield* Tool.bind(toolkit, { uppercase: { replay: 'safe' } }).pipe(
    Effect.provide(
      toolkit.toLayer({
        uppercase: ({ text }) =>
          Effect.gen(function* () {
            const call = yield* Invocation.ToolCall
            yield* Ref.update(toolCalls, (count) => count + 1)
            yield* call.output(text.toUpperCase())
            return text.toUpperCase()
          }),
      }),
    ),
  )
  const registry = Registry.layer([{ name: 'example', tools }])
  const configuration = Conversation.layerConfiguration({
    settings: {
      retry: { enabled: false },
      compaction: { enabled: false },
      progress: { partialIntervalMs: 0, outputIntervalMs: 0 },
    },
  })
  const database = SqliteClient.layer({ filename })
  // Native cluster message storage and harness domain storage share this SqlClient.
  // Memory runner membership still uses durable SQL message/journal storage.
  const cluster = SingleRunner.layer({
    runnerStorage: 'memory',
    shardingConfig: {
      shardsPerGroup: 1,
      entityMessagePollInterval: '25 millis',
      entityReplyPollInterval: '25 millis',
    },
  }).pipe(Layer.provideMerge(database), Layer.provide(BunCrypto.layer))
  const engine = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(cluster))
  const creation = Conversation.layerCreation.pipe(
    Layer.provide(configuration),
    Layer.provide(registry),
    Layer.provide(BunCrypto.layer),
  )
  const session = Session.layer.pipe(Layer.provideMerge(SqlStore.layer), Layer.provide(creation))
  const directory = Directory.layerSingle(sessionId).pipe(Layer.provideMerge(session))
  const runtime = Layer.mergeAll(DurableExecutor.layer, greetingExecutor).pipe(
    Layer.provideMerge(engine),
    Layer.provideMerge(directory),
    Layer.provide(configuration),
    Layer.provideMerge(catalogue),
    Layer.provide(Harness.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
  )
  yield* Effect.gen(function* () {
    const current = yield* Session.Session
    const root = yield* current.root()
    yield* current.transaction(
      Effect.fnUntraced(function* (tx) {
        const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
        agent.model = descriptor.ref
      }),
    )
    const greeting = yield* Greeting.execute({ name: 'Effect' })
    if (greeting !== 'Hello, Effect') return yield* Effect.die('Unexpected native greeting')
    const payload = {
      sessionId,
      conversationId: Record.ROOT_CONVERSATION_ID,
      requestId,
      submission: {
        type: 'input' as const,
        message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'uppercase hello' })] }),
      },
    }
    const result = yield* Submission.execute(payload)
    if (result.status !== 'done' || result.type !== 'input')
      return yield* Effect.die('Submission did not finish')
    yield* Conversation.awaitIdle(current, root.id)
    const answer = yield* current.entry(result.answer, root.id)
    if (answer === undefined) return yield* Effect.die('Answer was not committed')
    const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
      answer.entry.model ?? [],
    )
    if (
      !messages.some(
        (message) =>
          message.role === 'assistant' &&
          message.content.some((part) => part.type === 'text' && part.text === 'HELLO'),
      )
    )
      return yield* Effect.die('Unexpected committed model answer')
    const counts = { model: yield* Ref.get(modelCalls), tool: yield* Ref.get(toolCalls) }
    const executionId = yield* Submission.execute(payload, { discard: true })
    const polled = yield* Submission.poll(executionId)
    if (Option.isNone(polled) || polled.value._tag !== 'Complete')
      return yield* Effect.die('Native poll did not observe completion')
    yield* Submission.resume(executionId)
    const replayed = yield* Submission.execute(payload)
    if (
      replayed.id !== result.id ||
      (yield* Ref.get(modelCalls)) !== counts.model ||
      (yield* Ref.get(toolCalls)) !== counts.tool
    )
      return yield* Effect.die('Idempotent replay performed new work')
    if (!((counts.model === 2 && counts.tool === 1) || (counts.model === 0 && counts.tool === 0)))
      return yield* Effect.die('Unexpected model/tool invocation counts')
    yield* Console.log('native-workflow-example-ok')
  }).pipe(Effect.provide(runtime.pipe(Layer.provide(database))))
})

BunRuntime.runMain(
  main.pipe(
    Effect.scoped,
    // Preserve explicit empty environment values, matching the previous nullish fallback.
    Effect.provide(
      Layer.merge(
        ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true })),
        BunServices.layer,
      ),
    ),
  ),
)
