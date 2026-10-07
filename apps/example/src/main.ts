import { BunCrypto, BunRuntime, BunServices } from '@effect/platform-bun'
import { SqliteClient } from '@effect/sql-sqlite-bun'
import { Conversation, Identity, Record, Session, SessionDirectory } from '@effect-harness/durable'
import { Executor as DurableExecutor } from '@effect-harness/durable'
// effect-review-allow P9-namespace-alias-equals-module: durable Executor and harness Executor share the same basename; the aliases distinguish their Layer composition.
import { SqliteStore } from '@effect-harness/durable/storage'
import { Submission } from '@effect-harness/durable/workflow'
import { Invocation, Model, Registry, Tool, ToolError } from '@effect-harness/harness'
import { Executor as HarnessExecutor } from '@effect-harness/harness'
// effect-review-allow P9-namespace-alias-equals-module: harness Executor and durable Executor share the same basename; the aliases distinguish their Layer composition.
import {
  Array as Arr,
  ConfigProvider,
  Console,
  Context,
  Effect,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from 'effect'
import { LanguageModel, Prompt, Response, Toolkit } from 'effect/ai'
import { Tool as AiTool } from 'effect/ai'
// effect-review-allow P9-namespace-alias-equals-module: Effect AI Tool and harness Tool share the same basename; AiTool declares native tools while Tool binds harness replay policy.
import { ClusterWorkflowEngine, SingleRunner } from 'effect/cluster'
import { Activity, Workflow } from 'effect/workflow'
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
  const model = yield* LanguageModel.make({
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
  const generatedCall = yield* Arr.head(generated.toolCalls).pipe(
    Option.match({
      onNone: () => Effect.die('Native generation omitted the unknown tool call'),
      onSome: Effect.succeed,
    }),
  )
  const streamedPart = yield* Arr.findFirst(streamed, Schema.is(unknownCall)).pipe(
    Option.match({
      onNone: () => Effect.die('Native stream omitted the unknown tool call'),
      onSome: Effect.succeed,
    }),
  )
  const decoded = yield* Schema.decodeUnknownEffect(unknownCall)(generatedCall)
  const streamedCall = yield* Schema.decodeEffect(unknownCall)(streamedPart)
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
  const native = yield* LanguageModel.make({
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
    failure: ToolError.ToolError,
  }).addDependency(Invocation.ToolCall)
  const toolkit = Toolkit.make(uppercase)
  const tools = yield* Tool.bind(toolkit, { uppercase: { replay: 'safe' } }).pipe(
    Effect.provide(
      toolkit.toLayer({
        uppercase: Effect.fn('example.uppercase')(function* ({
          text,
        }: typeof uppercase.parametersSchema.Type) {
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
      progress: { partialIntervalMs: '0 millis', outputIntervalMs: '0 millis' },
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
  const session = Session.layer.pipe(Layer.provideMerge(SqliteStore.layer), Layer.provide(creation))
  const directory = SessionDirectory.layerSingle(sessionId).pipe(Layer.provideMerge(session))
  const runtime = Layer.mergeAll(DurableExecutor.layer, greetingExecutor).pipe(
    Layer.provideMerge(engine),
    Layer.provideMerge(directory),
    Layer.provide(configuration),
    Layer.provideMerge(catalogue),
    Layer.provide(HarnessExecutor.layer.pipe(Layer.provide(Layer.mergeAll(registry, catalogue)))),
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
        _tag: 'input' as const,
        type: 'input' as const,
        message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'uppercase hello' })] }),
      },
    }
    const result = yield* Submission.Submission.execute(payload)
    if (result._tag !== 'InputDone') return yield* Effect.die('Submission did not finish')
    yield* Conversation.awaitIdle(current, root.id)
    const answer = yield* current.entry(result.answer, root.id).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.die('Answer was not committed'),
          onSome: Effect.succeed,
        }),
      ),
    )
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
    const executionId = yield* Submission.Submission.execute(payload, { discard: true })
    const polled = yield* Submission.Submission.poll(executionId)
    if (Option.isNone(polled) || polled.value._tag !== 'Complete')
      return yield* Effect.die('Native poll did not observe completion')
    yield* Submission.Submission.resume(executionId)
    const replayed = yield* Submission.Submission.execute(payload)
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
