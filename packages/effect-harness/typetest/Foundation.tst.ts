import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import type * as Execution from 'effect-harness/Execution'
import type { Failure } from 'effect-harness/ExecutionError'
import * as Extension from 'effect-harness/Extension'
import * as Harness from 'effect-harness/Harness'
import type { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import { HookExecution } from 'effect-harness/HookExecution'
import * as Model from 'effect-harness/Model'
import * as PromptSection from 'effect-harness/PromptSection'
import type * as Session from 'effect-harness/Session'
import type { Storage } from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import * as Toolkit from 'effect-harness/Toolkit'
import type * as Transaction from 'effect-harness/Transaction'

const IssueSchema = Schema.Struct({ id: Schema.String, title: Schema.String })
type Issue = typeof IssueSchema.Type
class TrackerError extends Schema.TaggedError<TrackerError>()('TrackerError', {
  message: Schema.String,
}) {}
class IssueTracker extends Context.Service<
  IssueTracker,
  {
    readonly search: (query: string) => Effect.Effect<ReadonlyArray<Issue>, TrackerError>
    readonly save: (issues: ReadonlyArray<Issue>) => Effect.Effect<void, Failure, ToolExecution>
  }
>()('typetest/IssueTracker') {}
class Decoder extends Context.Service<Decoder, string>()('typetest/foundation/Decoder') {}
class Encoder extends Context.Service<Encoder, string>()('typetest/foundation/Encoder') {}
class Credentials extends Context.Service<Credentials, string>()(
  'typetest/foundation/Credentials',
) {}
class Audit extends Context.Service<Audit, string>()('typetest/foundation/Audit') {}
class BuildError extends Schema.TaggedError<BuildError>()('BuildError', {}) {}
declare const trackerLayer: Layer.Layer<IssueTracker, BuildError, Credentials>
declare const auditLayer: Layer.Layer<Audit>
declare const nativeModel: typeof LanguageModel.LanguageModel.Service
declare const conversation: Conversation.Conversation
declare const submission: Submission.Submission
declare const execution: Execution.Access

const search = Tool.make('search_issues', {
  description: 'Find issues and save them in the durable session',
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Array(IssueSchema),
  failure: TrackerError,
  replay: 'safe',
})
const toolkit = Toolkit.make(search)
const handlers = toolkit.toLayer(
  Effect.gen(function* () {
    const tracker = yield* IssueTracker
    return {
      search_issues: Effect.fnUntraced(function* ({ query }) {
        expect(query).type.toBe<string>()
        const issues = yield* tracker.search(query)
        yield* tracker.save(issues)
        return issues
      }),
    } satisfies Toolkit.HandlersFrom<typeof toolkit.tools>
  }),
)

test('shared tool declarations and handler Layers have independent requirements', () => {
  expect(search.name).type.toBe<'search_issues'>()
  expect<Tool.Parameters<typeof search>>().type.toBe<{ readonly query: string }>()
  expect<Tool.Success<typeof search>>().type.toBe<ReadonlyArray<Issue>>()
  expect(handlers).type.toBe<Layer.Layer<Toolkit.Handler<'search_issues'>, never, IssueTracker>>()
  expect(Effect.provide(toolkit, handlers)).type.toBe<
    Effect.Effect<Toolkit.WithHandler<typeof toolkit.tools>, never, IssueTracker>
  >()
  expect(handlers.pipe(Layer.provide(trackerLayer))).type.toBe<
    Layer.Layer<Toolkit.Handler<'search_issues'>, BuildError, Credentials>
  >()
})

test('native tool dependency declarations are captured by the execution side', () => {
  const declared = search.addDependency(IssueTracker)
  const provided = Toolkit.make(declared)
  const layer = provided.toLayer({
    search_issues: Effect.fnUntraced(function* ({ query }) {
      const tracker = yield* IssueTracker
      return yield* tracker.search(query)
    }),
  })
  expect(layer).type.toBe<Layer.Layer<Toolkit.Handler<'search_issues'>, never, IssueTracker>>()
})

test('merged toolkits retain handler names and schema-typed results', () => {
  const other = Toolkit.make(Tool.make('get_time', { success: Schema.Finite }))
  const combined = Toolkit.merge(toolkit, other)
  expect(combined.tools.search_issues).type.toBe<typeof search>()
  expect(combined.tools.get_time.name).type.toBe<'get_time'>()
  expect(combined.toLayer).type.not.toBeCallableWith({ get_time: () => Effect.succeed(0) })
  expect(other.toLayer).type.not.toBeCallableWith({ get_time: () => Effect.succeed('bad') })
  expect(other.toLayer({ get_time: () => Effect.succeed(0) })).type.toBe<
    Layer.Layer<Toolkit.Handler<'get_time'>>
  >()
})

const beforeTool = Hook.make({
  event: 'beforeTool',
  execute: Effect.fnUntraced(function* ({ call }) {
    expect(call.name).type.toBe<string>()
    yield* Audit
    yield* HookExecution
    return { _tag: 'allow' } as const
  }),
})
test('runtime owns tool handlers, hooks, storage and provider dependencies', () => {
  expect(HarnessRuntime.layer({ tools: toolkit, hooks: [beforeTool] })).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | HarnessBackend,
      Harness.HarnessError,
      Storage | LanguageModel.LanguageModel | Toolkit.Handler<'search_issues'> | Audit
    >
  >()
  const extension = Extension.make({ name: 'issues', tools: [search], hooks: [beforeTool] })
  expect<Extension.Requirements<typeof extension>>().type.toBe<
    Toolkit.Handler<'search_issues'> | Audit | HookExecution
  >()
  expect(HarnessRuntime.make({ extensions: [extension] })).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      Harness.HarnessError,
      Scope.Scope | Storage | LanguageModel.LanguageModel | Toolkit.Handler<'search_issues'> | Audit
    >
  >()
})

test('hooks and extension providers retain dependencies in both dual forms', () => {
  expect<Hook.Requirements<typeof beforeTool>>().type.toBe<Audit | HookExecution>()
  const supplied = Hook.provide(beforeTool, auditLayer)
  const piped = beforeTool.pipe(Hook.provide(auditLayer))
  expect<Hook.Requirements<typeof supplied>>().type.toBe<HookExecution>()
  expect<Hook.Requirements<typeof piped>>().type.toBe<HookExecution>()
  const extension = Extension.make({ name: 'issues', tools: toolkit }).pipe(
    Extension.provide(handlers),
  )
  expect<Extension.Requirements<typeof extension>>().type.toBe<IssueTracker>()
})

test('prompt sections keep application services on the execution side', () => {
  const section = PromptSection.make({
    key: 'project',
    render: Effect.fnUntraced(function* () {
      yield* HookExecution
      return yield* Audit
    }),
  })
  const extension = Extension.make({ name: 'project', sections: [section] })
  expect<Extension.Requirements<typeof extension>>().type.toBe<Audit | HookExecution>()
  expect(HarnessRuntime.layer({ extensions: [extension] })).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | HarnessBackend,
      Harness.HarnessError,
      Storage | LanguageModel.LanguageModel | Audit
    >
  >()
})

const codec = Schema.Struct({ instant: Schema.DateFromString }).pipe(
  Schema.middlewareDecoding((effect) => Effect.flatMap(Decoder, () => effect)),
  Schema.middlewareEncoding((effect) => Effect.flatMap(Encoder, () => effect)),
)
const clockDocument = Document.define({
  kind: 'clock',
  scope: 'session',
  version: 1,
  schema: codec,
  initial: () => ({ instant: new Date() }),
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target

test('tool codecs are required by the handler side independently of application handlers', () => {
  const clock = Tool.make('clock', { parameters: codec, success: Schema.String })
  const clocks = Toolkit.make(clock)
  expect(
    clocks.toLayer({
      clock: ({ instant }) => {
        expect(instant).type.toBe<Date>()
        return Effect.succeed(instant.toISOString())
      },
    }),
  ).type.toBe<Layer.Layer<Toolkit.Handler<'clock'>, never, Decoder | Encoder>>()
  expect(HarnessRuntime.layer({ tools: clocks })).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | HarnessBackend,
      Harness.HarnessError,
      Storage | LanguageModel.LanguageModel | Toolkit.Handler<'clock'> | Decoder | Encoder
    >
  >()
})

test('model descriptors keep codec and configuration services exclusively on the runtime', () => {
  const model = Model.make({
    definition: {
      ref: { provider: 'example', modelId: 'local' },
      capabilities: { tools: true, reasoning: false, images: false, structuredOutput: false },
    },
    languageModel: nativeModel,
    options: codec,
    configure: Effect.fnUntraced(function* ({ instant }) {
      expect(instant).type.toBe<Date>()
      yield* Credentials
      return Context.empty()
    }),
  })
  expect<Model.Requirements<typeof model>>().type.toBe<Decoder | Encoder | Credentials>()
  expect(HarnessRuntime.layer({ models: [model], tools: toolkit })).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | HarnessBackend,
      Harness.HarnessError,
      Storage | Toolkit.Handler<'search_issues'> | Decoder | Encoder | Credentials
    >
  >()
})

test('invocation transaction callbacks stay local; client document decoding stays client-side', () => {
  const change = (_tx: Transaction.Transaction) => Effect.as(Audit, 'saved' as const)
  expect(execution.commit(change)).type.toBe<Effect.Effect<'saved', Failure, Audit>>()
  expect(execution.snapshot(clockDocument, target)).type.toBe<
    Effect.Effect<Option.Option<Document.Snapshot<typeof codec>>, Failure, Decoder>
  >()
  expect(conversation.pipe(Conversation.snapshot(clockDocument, target))).type.toBe<
    Effect.Effect<
      Option.Option<Document.Snapshot<typeof codec>>,
      Harness.HarnessError | Schema.SchemaError,
      Decoder
    >
  >()
  expect(conversation.pipe(Conversation.watch(clockDocument, target))).type.toBe<
    Stream.Stream<
      Document.Snapshot<typeof codec>,
      Harness.HarnessError | Schema.SchemaError,
      Decoder
    >
  >()
})

test('the same application program depends only on the Harness client', () => {
  const job = { type: 'input', content: 'Fix the flaky login test', requestId: 'job-42' } as const
  expect(conversation.pipe(Conversation.submit(job))).type.toBe<
    Effect.Effect<Submission.Submission, Harness.HarnessError>
  >()
  expect(Submission.wait(submission)).type.toBe<
    Effect.Effect<Submission.Settled, Harness.HarnessError>
  >()
  expect(submission.pipe(Submission.wait())).type.toBe<
    Effect.Effect<Submission.Settled, Harness.HarnessError>
  >()
  expect(conversation.pipe(Conversation.fork())).type.toBe<
    Effect.Effect<Conversation.Conversation, Harness.HarnessError>
  >()
  expect(Harness.make).type.toBe<Effect.Effect<Harness.HarnessService, never, HarnessBackend>>()
  expect(Harness.layer).type.toBe<Layer.Layer<Harness.Harness, never, HarnessBackend>>()
  const program = Effect.gen(function* () {
    const harness = yield* Harness.Harness
    const root = yield* harness.root
    return yield* Submission.wait(yield* root.pipe(Conversation.submit(job)))
  })
  expect(program).type.toBe<
    Effect.Effect<Submission.Settled, Harness.HarnessError, Harness.Harness>
  >()
  expect(Harness.layerLocal()).type.toBe<
    Layer.Layer<Harness.Harness, Harness.HarnessError, Storage | LanguageModel.LanguageModel>
  >()
})

declare const runtime: HarnessRuntime.HarnessRuntimeService
test('only the runtime exposes its Session value', () => {
  expect(runtime.session).type.toBe<Session.Session>()
})
