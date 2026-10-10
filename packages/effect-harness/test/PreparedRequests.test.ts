import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
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
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as NativeTool from 'effect/ai/Tool'
import * as Extension from 'effect-harness/Extension'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Model from 'effect-harness/Model'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolWrapper from 'effect-harness/ToolWrapper'
import * as FileReader from 'effect-harness/FileReader'
import * as Images from 'effect-harness/ImagePreparation'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as NodeImages from 'effect-harness/images-node'
import * as Read from 'effect-harness/coding/Read'
import * as ImageRead from 'effect-harness/coding/ImageRead'
import * as ImageReadPolicy from 'effect-harness/coding/ImageReadPolicy'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import type * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as HttpClient from 'effect/http/HttpClient'

const root = Record.ROOT_CONVERSATION_ID
const finish = (reason: 'stop' | 'tool-calls'): Response.PartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const answer: Array<Response.PartEncoded> = [{ type: 'text', text: 'answer' }, finish('stop')]
const intent = (name = 'work', params = {}): Array<Response.PartEncoded> => [
  { type: 'tool-call', id: `call-${name}`, name, params, providerExecuted: false },
  finish('tool-calls'),
]
class NativeConfig extends Context.Service<NativeConfig, { readonly value: string }>()(
  'test/prepared/NativeConfig',
) {}
const configuration = Model.makeConfiguration({
  id: 'test/prepared/configuration',
  version: 1,
  schema: Schema.Struct({ value: Schema.String }),
  tags: [NativeConfig],
  capture: (context) => Effect.succeed(Option.getOrThrow(Context.getOption(context, NativeConfig))),
  restore: (value) => Effect.succeed(Context.make(NativeConfig, value)),
})
const descriptor = (
  native: typeof LanguageModel.LanguageModel.Service,
  config: Model.Configuration,
  configure: (value: string) => void = () => {},
  modelId = 'fixture',
) =>
  Model.make({
    definition: {
      ref: { provider: 'offline', modelId },
      capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
    },
    languageModel: native,
    options: Schema.Struct({ value: Schema.optionalKey(Schema.String) }),
    configuration: config,
    configure: ({ value }, policy) => {
      const hint = policy.settings.stream.metadata?.['value']
      const selected = value ?? (typeof hint === 'string' ? hint : 'initial')
      configure(selected)
      return Effect.succeed(Context.make(NativeConfig, { value: selected }))
    },
  })
const saved = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanTasks(runtime.session, { conversationId: root, kind: 'harness.run' }).pipe(
    Stream.runCollect,
    Effect.map((tasks) => tasks.at(-1)!),
  )
const preparation = (record: Record.Task) =>
  Schema.decodeUnknownEffect(
    Schema.Struct({ preparation: Schema.toCodecJson(Prepared.SnapshotSchema) }),
  )(record.state.checkpoint).pipe(Effect.map((state) => state.preparation))
const entries = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root, order: 'ascending' }).pipe(
    Stream.runCollect,
  )
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sqlite = Storage.layerSql.pipe(
    Layer.provide(SqliteClient.layer({ filename: `${directory}/prepared.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/prepared.jsonl` })
  return {
    open: (backend: 'memory' | 'sqlite' | 'jsonl') =>
      backend === 'memory'
        ? Effect.succeed(memory)
        : Layer.build(Layer.fresh(backend === 'sqlite' ? sqlite : jsonl)),
  }
})
const texts = (prompt: Prompt.Prompt) =>
  prompt.content.flatMap((message) =>
    message.role === 'system'
      ? [message.content]
      : message.content.flatMap((part) => (part.type === 'text' ? [part.text] : [])),
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} canonical prepared requests`, () => {
    it.live(
      'commits final hook prompt/config before call and cold recovery never renders newer callbacks',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const entered = yield* Deferred.make<void>()
            const policy = yield* Settings.make({ stream: { metadata: { value: 'first' } } })
            const config = yield* configuration
            let renders = 0
            let hooks = 0
            let configurations = 0
            const requests: Prepared.Snapshot[] = []
            const values: string[] = []
            const native = yield* LanguageModel.make({
              generateText: () =>
                Effect.gen(function* () {
                  requests.push(
                    Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
                      .current,
                  )
                  values.push(Option.getOrThrow(yield* Effect.serviceOption(NativeConfig)).value)
                  if (requests.length === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }
                  return answer
                }),
              streamText: () => Stream.empty,
            })
            const extension = Extension.make({
              name: 'prompt',
              sections: [
                PromptSection.make({
                  key: 'section',
                  render: () => Effect.sync(() => `render-${++renders}`),
                }),
              ],
              hooks: [
                Hook.make({
                  event: 'beforeRequest',
                  execute: ({ prompt }) =>
                    Effect.sync(() =>
                      Prompt.concat(
                        prompt,
                        Prompt.make([
                          Prompt.userMessage({
                            content: [Prompt.textPart({ text: `hook-${++hooks}` })],
                          }),
                        ]),
                      ),
                    ),
                }),
              ],
            })
            const model = descriptor(native, config, () => {
              configurations++
            })
            const open = Effect.gen(function* () {
              const context = yield* store.open(backend)
              const runtime = yield* HarnessRuntime.make({
                models: [model],
                extensions: [extension],
              }).pipe(
                Effect.provideContext(context),
                Effect.provideService(Settings.Settings, policy),
              )
              yield* runtime.backend.root
              return { runtime, context }
            })
            const firstScope = yield* Scope.make()
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            const job = yield* first.runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            const stored = yield* preparation(yield* saved(first.runtime))
            assert.deepEqual(stored, requests[0])
            assert.deepEqual(texts(stored.prompt), ['render-1', 'go', 'hook-1'])
            assert.deepEqual(stored.configuration, { value: 'first' })
            assert.strictEqual(stored.ordinal, 1)
            assert.strictEqual(stored.attempt, 0)
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(first.context)))
                .minimumReaderVersion,
              5,
            )
            yield* first.runtime.backend.configure({
              conversationId: root,
              change: {
                instructions: 'new instructions',
                model: { ref: model.definition.ref, options: { value: 'new options' } },
              },
            })
            yield* policy.update({ stream: { metadata: { value: 'new settings' } } })
            yield* Scope.close(firstScope, Exit.void)
            const second = yield* open
            assert.strictEqual((yield* second.runtime.backend.wait(job.id)).status, 'done')
            assert.deepEqual(requests[1], stored)
            assert.deepEqual(values, ['first', 'first'])
            assert.strictEqual(renders, 1)
            assert.strictEqual(hooks, 1)
            assert.strictEqual(configurations, 1)
            const next = yield* second.runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'next' },
            })
            assert.strictEqual((yield* second.runtime.backend.wait(next.id)).status, 'done')
            assert.notStrictEqual(requests[2]?.requestId, stored.requestId)
            assert.strictEqual(requests[2]?.ordinal, 1)
            assert.strictEqual(requests[2]?.settings.revision, 1)
            assert.strictEqual(values[2], 'new options')
            assert.strictEqual(renders, 2)
            assert.strictEqual(hooks, 2)
            assert.strictEqual(configurations, 2)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'old tool lease survives replacement; live agent/model/settings affect the same run’s next request',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const context = yield* store.open(backend)
            const policy = yield* Settings.make()
            const registry = yield* Registry.make
            const config = yield* configuration
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const tools = Toolkit.make(
              Tool.make('work', { success: Schema.String, replay: 'safe' }),
              Tool.make('nested', { success: Schema.String, callers: ['tools'] }),
            )
            const old = tools.toLayer({
              work: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as('old'),
                ),
              nested: () => Effect.succeed('nested'),
            })
            yield* registry.install(
              Registry.provide(Registry.toolkit({ id: 'owner', version: 1 }, tools), old),
            )
            let changed = 0
            const wrapper = ToolWrapper.make({
              name: 'work',
              wrap: (item) =>
                Effect.succeed({
                  ...item,
                  invoke: (call) =>
                    item.invoke(call).pipe(
                      Effect.tap(() =>
                        Effect.sync(() => {
                          changed++
                        }),
                      ),
                    ),
                }),
            })
            yield* registry.install(Registry.toolWrapper({ id: 'wrapper', version: 1 }, wrapper))
            const requests: Prepared.Snapshot[] = []
            const firstModel = yield* LanguageModel.make({
              generateText: () =>
                Effect.gen(function* () {
                  requests.push(
                    Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
                      .current,
                  )
                  return intent()
                }),
              streamText: () => Stream.empty,
            })
            const secondModel = yield* LanguageModel.make({
              generateText: () =>
                Effect.gen(function* () {
                  requests.push(
                    Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
                      .current,
                  )
                  assert.strictEqual(
                    Option.getOrThrow(yield* Effect.serviceOption(NativeConfig)).value,
                    'new',
                  )
                  return answer
                }),
              streamText: () => Stream.empty,
            })
            const first = descriptor(firstModel, config, undefined, 'one')
            const second = descriptor(secondModel, config, undefined, 'two')
            const runtime = yield* HarnessRuntime.make({ models: [first, second], registry }).pipe(
              Effect.provideContext(context),
              Effect.provideService(Settings.Settings, policy),
            )
            yield* runtime.backend.root
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            assert.deepEqual(
              requests[0]?.tools.map((tool) => tool.declaration.name),
              ['work'],
            )
            assert.deepEqual(
              requests[0]?.allTools.map((tool) => tool.declaration.name),
              ['work', 'nested'],
            )
            yield* registry.install(
              Registry.provide(
                Registry.toolkit({ id: 'owner', version: 2 }, tools),
                tools.toLayer({
                  work: () => Effect.succeed('new'),
                  nested: () => Effect.succeed('new nested'),
                }),
              ),
            )
            yield* registry.uninstall('wrapper')
            yield* policy.update({ stream: { metadata: { value: 'settings' } } })
            yield* runtime.backend.configure({
              conversationId: root,
              change: {
                model: { ref: second.definition.ref, options: { value: 'new' } },
                instructions: 'changed',
                modelTools: [],
              },
            })
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
            assert.strictEqual(changed, 1)
            assert.strictEqual(requests[1]?.ordinal, 2)
            assert.notStrictEqual(requests[1]?.requestId, requests[0]?.requestId)
            assert.strictEqual(requests[1]?.settings.revision, 1)
            assert.deepEqual(requests[1]?.tools, [])
            assert.strictEqual(requests[1]?.model._tag, 'descriptor')
            assert.strictEqual(
              requests[1]?.model._tag === 'descriptor' && requests[1].model.definition.ref.modelId,
              'two',
            )
            const history = yield* entries(runtime)
            const result = history.find((entry) => entry.kind === 'tool.result')
            assert.isDefined(result)
            assert.isTrue(JSON.stringify(result?.model).includes('old'))
            assert.strictEqual(history.filter((entry) => entry.kind === 'context.change').length, 2)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'missing or incompatible saved registrations block cold recovery and compatible reload wakes it',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const registry = yield* Registry.make
            const config = yield* configuration
            const tools = Toolkit.make(Tool.make('work', { success: Schema.String }))
            const registration = (version: number) =>
              Registry.provide(
                Registry.toolkit({ id: 'required', version }, tools),
                tools.toLayer({ work: () => Effect.succeed('work') }),
              )
            yield* registry.install(registration(1))
            const entered = yield* Deferred.make<void>()
            let calls = 0
            const requests: Prepared.Snapshot[] = []
            const native = yield* LanguageModel.make({
              generateText: () =>
                Effect.gen(function* () {
                  requests.push(
                    Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest))
                      .current,
                  )
                  calls++
                  return yield* calls === 1
                    ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
                    : Effect.succeed(answer)
                }),
              streamText: () => Stream.empty,
            })
            const model = descriptor(native, config)
            const open = store
              .open(backend)
              .pipe(
                Effect.flatMap((context) =>
                  HarnessRuntime.make({ registry, models: [model] }).pipe(
                    Effect.provideContext(context),
                  ),
                ),
              )
            const firstScope = yield* Scope.make()
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* first.backend.root
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            const task = yield* saved(first)
            const stored = yield* preparation(task)
            yield* Scope.close(firstScope, Exit.void)
            yield* registry.uninstall('required')
            const second = yield* open
            yield* second.scheduler.resume
            assert.strictEqual((yield* second.scheduler.inspect(task.id)).scheduling, 'blocked')
            assert.strictEqual(calls, 1)
            yield* registry.install(registration(2))
            yield* second.scheduler.resume
            assert.strictEqual((yield* second.scheduler.inspect(task.id)).scheduling, 'blocked')
            assert.strictEqual(calls, 1)
            yield* registry.install(registration(1))
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
            assert.strictEqual(calls, 2)
            assert.strictEqual(requests[1]?.requestId, stored.requestId)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  })
}

it.effect(
  'unsupported ambient configuration fails before prepared facts or reader5 activation',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const native = yield* LanguageModel.make({
          generateText: () => Effect.die('Provider must not run'),
          streamText: () => Stream.empty,
        })
        const model = Model.make({
          definition: {
            ref: { provider: 'offline', modelId: 'unsupported' },
            capabilities: {
              tools: false,
              images: false,
              reasoning: false,
              structuredOutput: false,
            },
          },
          languageModel: native,
          options: Schema.Struct({}),
          configure: () => Effect.succeed(Context.make(NativeConfig, { value: 'ambient' })),
        })
        const runtime = yield* HarnessRuntime.make({ models: [model] })
        yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
        assert.isFalse((yield* entries(runtime)).some((entry) => entry.kind === 'context.change'))
        assert.strictEqual((yield* StorageFormat.inspect).formatVersion, 4)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.live(
  'selected model policy overrides fixed policy per read while toolkit roots and preparers stay isolated',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const left = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
        const right = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
        const png = yield* fs.readFile(
          new URL('./fixtures/images-native/opaque.png', import.meta.url).pathname,
        )
        yield* fs.writeFile(`${left}/same.png`, png)
        yield* fs.writeFile(`${right}/same.png`, png)
        const tools = Read.withImages({ maxSourceBytes: 100000 })
        const registry = yield* Registry.make
        const widths: number[] = []
        const invoked: string[] = []
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const install = (id: string, directory: string) => {
          const files = FileReader.layer.pipe(
            Layer.provideMerge(ToolkitFileSystem.layer({ root: directory })),
          )
          const preparer = Layer.effect(
            Images.ImagePreparation,
            Effect.map(Images.ImagePreparation, (original) => ({
              prepare: (bytes, mime, limits) =>
                Effect.gen(function* () {
                  invoked.push(id)
                  widths.push(limits.maxWidth)
                  if (id === 'left') {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                  }
                  return yield* original.prepare(bytes, mime, limits)
                }),
            })),
          ).pipe(Layer.provide(NodeImages.layer))
          return registry.install(
            Registry.provide(
              Registry.toolkit({ id, version: 1 }, tools.toolkit),
              tools.layer.pipe(
                Layer.provideMerge(
                  Layer.mergeAll(files, preparer, ImageReadPolicy.layer({ vision: false })),
                ),
              ),
            ),
          )
        }
        yield* install('left', left)
        yield* install('right', right)
        let calls = 0
        const results: unknown[] = []
        yield* registry.install(
          Registry.hook(
            { id: 'observe', version: 1 },
            Hook.make({
              event: 'afterTool',
              execute: ({ result }) =>
                Effect.sync(() => {
                  results.push(result.structuredOutput)
                }),
            }),
          ),
        )
        const native = yield* LanguageModel.make({
          generateText: () =>
            Effect.sync(() => (++calls < 4 ? intent('read', { path: 'same.png' }) : answer)),
          streamText: () => Stream.empty,
        })
        const limits = { maxWidth: 1, maxHeight: 30, maxBytes: 100000, maxInputPixels: 100 }
        const model = (modelId: string, images: boolean, maxWidth: number) =>
          Model.make({
            definition: {
              ref: { provider: 'offline', modelId },
              capabilities: { tools: true, images, reasoning: false, structuredOutput: false },
              imageLimits: { ...limits, maxWidth },
            },
            languageModel: native,
            options: Schema.Struct({}),
            configure: () => Effect.succeed(Context.empty()),
          })
        const one = model('one', true, 1)
        const two = model('two', true, 2)
        const text = model('text', false, 3)
        const runtime = yield* HarnessRuntime.make({
          registry,
          models: [one, two, text],
          agent: {
            toolkits: {
              inherit: false,
              registrations: ['left'],
              bindings: { read: { binding: 'left', version: 1 } },
            },
          },
        })
        yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'images' },
        })
        yield* Deferred.await(entered)
        yield* runtime.backend.configure({
          conversationId: root,
          change: {
            model: { ref: two.definition.ref, options: {} },
            toolkits: {
              inherit: false,
              registrations: ['right'],
              bindings: { read: { binding: 'right', version: 1 } },
            },
          },
        })
        // The right invocation changes the following request, never the saved first invocation.
        yield* registry.install(
          Registry.hook(
            { id: 'switch-text', version: 1 },
            Hook.make({
              event: 'afterTool',
              execute: () =>
                runtime.backend.configure({
                  conversationId: root,
                  change: { model: { ref: text.definition.ref, options: {} } },
                }),
            }),
          ),
        )
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        assert.deepEqual(invoked, ['left', 'right'])
        assert.deepEqual(widths, [1, 2])
        const first = yield* Schema.decodeUnknownEffect(ImageRead.Outcome)(results[0])
        const second = yield* Schema.decodeUnknownEffect(ImageRead.Outcome)(results[1])
        assert.strictEqual(first.path, `${left}/same.png`)
        assert.strictEqual(second.path, `${right}/same.png`)
        assert.strictEqual(first.dimensions.width, 1)
        assert.strictEqual(second.dimensions.width, 2)
        assert.strictEqual(
          (yield* Schema.decodeUnknownEffect(ImageRead.Placeholder)(results[2])).reason,
          'modelDoesNotSupportImages',
        )
      }),
    ).pipe(Effect.provide(Layer.merge(NodeServices.layer, Storage.layerMemory))),
)

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  it.live(
    `${backend} native OpenAI wire keeps saved options, image bytes, offered schema and explicit latest fallback after cold reopen`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          const entered = yield* Deferred.make<void>()
          let configured = 0
          let renders = 0
          let temperature = 0.7
          const requests: Array<typeof OpenAiSchema.CreateResponse.Encoded> = []
          const preparations: Prepared.Snapshot[] = []
          const client = OpenAiClient.OpenAiClient.of({
            client: HttpClient.make(() => Effect.die('Unexpected HTTP work')),
            createResponse: (input) =>
              Effect.gen(function* () {
                requests.push(input)
                preparations.push(
                  Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest)).current,
                )
                if (requests.length === 1) {
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }
                return yield* Effect.die('captured native wire')
              }),
            createResponseStream: () => Effect.die('Unexpected streaming'),
            createEmbedding: () => Effect.die('Unexpected embedding'),
          })
          const native = yield* OpenAiLanguageModel.make({
            model: 'fixture',
            config: { temperature: 0.2, max_output_tokens: 10 },
          }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, client))
          const configSchema = Schema.Struct({
            temperature: Schema.Finite,
            max_output_tokens: Schema.Int,
            reasoning: Schema.Struct({ effort: Schema.Literal('high') }),
            store: Schema.Boolean,
          })
          const config = yield* Model.makeConfiguration({
            id: 'native/OpenAI-request-config',
            version: 1,
            tags: [OpenAiLanguageModel.Config],
            schema: configSchema,
            capture: (context) =>
              Schema.decodeUnknownEffect(configSchema, { onExcessProperty: 'error' })(
                Option.getOrThrow(Context.getOption(context, OpenAiLanguageModel.Config)),
              ),
            restore: (value) => Effect.succeed(Context.make(OpenAiLanguageModel.Config, value)),
          })
          const model = Model.make({
            definition: {
              ref: { provider: 'openai', modelId: 'fixture' },
              capabilities: { tools: true, images: true, reasoning: true, structuredOutput: false },
            },
            languageModel: native,
            options: Schema.Struct({}),
            configuration: config,
            configure: () => {
              configured++
              return Effect.succeed(
                Context.make(OpenAiLanguageModel.Config, {
                  temperature,
                  max_output_tokens: 20,
                  reasoning: { effort: 'high' },
                  store: false,
                }),
              )
            },
          })
          const toolkit = Toolkit.make(
            Tool.make('work', {
              description: 'Exact schema',
              parameters: Schema.Struct({ value: Schema.String }),
              success: Schema.String,
            }).annotate(NativeTool.Strict, false),
          )
          const extension = Extension.make({
            name: 'wire',
            tools: [toolkit.tools.work],
            sections: [
              PromptSection.make({
                key: 'system',
                render: () =>
                  Effect.sync(() => {
                    renders++
                    return 'saved system'
                  }),
              }),
            ],
          })
          const open = Effect.gen(function* () {
            const context = yield* store.open(backend)
            return yield* HarnessRuntime.make({
              models: [model],
              extensions: [extension],
            }).pipe(
              Effect.provideContext(context),
              Effect.provide(toolkit.toLayer({ work: ({ value }) => Effect.succeed(value) })),
            )
          })
          const firstScope = yield* Scope.make()
          const runtime = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
          yield* runtime.backend.root
          const bytes = Uint8Array.from([1, 2, 3])
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: {
              type: 'input',
              content: [
                Prompt.textPart({ text: 'media' }),
                Prompt.filePart({
                  mediaType: 'image/png',
                  data: bytes,
                  options: { openai: { imageDetail: 'high' } },
                }),
              ],
            },
          })
          yield* Deferred.await(entered)
          bytes.fill(9)
          temperature = 0.9
          const stored = yield* preparation(yield* saved(runtime))
          yield* Scope.close(firstScope, Exit.void)
          const reopened = yield* open
          assert.strictEqual((yield* reopened.backend.wait(job.id)).status, 'unanswered')
          assert.deepEqual(preparations[1], stored)
          assert.deepEqual(requests[1], requests[0])
          assert.strictEqual(configured, 1)
          assert.strictEqual(renders, 1)
          const sent = requests[0]!
          assert.strictEqual(sent.temperature, 0.7)
          assert.strictEqual(sent.max_output_tokens, 20)
          assert.deepEqual(sent.reasoning, { effort: 'high' })
          assert.strictEqual(sent.store, false)
          assert.strictEqual(sent.prompt_cache_key, preparations[0]?.affinity)
          assert.isTrue(JSON.stringify(sent.input).includes('data:image/png;base64,AQID'))
          assert.isTrue(JSON.stringify(sent.input).includes('"detail":"high"'))
          assert.isTrue(JSON.stringify(sent.input).includes('saved system'))
          assert.strictEqual(preparations[0]?.translation.positioning.tools, 'latest')
          assert.isTrue(JSON.stringify(sent.tools).includes('Exact schema'))
          assert.isTrue(JSON.stringify(sent.tools).includes('"strict":false'))
          assert.isTrue(JSON.stringify(sent.tools).includes('value'))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
