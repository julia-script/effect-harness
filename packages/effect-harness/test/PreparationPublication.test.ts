import * as ScriptedStream from './fixtures/ScriptedStream.js'
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
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Extension from 'effect-harness/Extension'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import { HarnessError } from 'effect-harness/HarnessError'
import * as Hook from 'effect-harness/Hook'
import * as Model from 'effect-harness/Model'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

const root = Record.ROOT_CONVERSATION_ID
const answer: Array<Response.PartEncoded> = [
  { type: 'text', text: 'answer' },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 1, text: 1, reasoning: undefined },
    },
    response: undefined,
  },
]
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sqlite = Storage.layerSql.pipe(
    Layer.provide(SqliteClient.layer({ filename: `${directory}/publication.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/publication.jsonl` })
  return (backend: 'memory' | 'sqlite' | 'jsonl') =>
    backend === 'memory'
      ? Effect.succeed(memory)
      : Layer.build(Layer.fresh(backend === 'sqlite' ? sqlite : jsonl))
})
const tasks = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanTasks(runtime.session, { conversationId: root, kind: 'harness.run' }).pipe(
    Stream.runCollect,
  )
const entries = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root, order: 'ascending' }).pipe(
    Stream.runCollect,
  )
const descriptor = (native: typeof LanguageModel.LanguageModel.Service) =>
  Model.make({
    definition: {
      ref: { provider: 'offline', modelId: 'publication' },
      capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
    },
    languageModel: native,
    options: Schema.Struct({}),
    configure: () => Effect.succeed(Context.empty()),
    positioning: { sections: 'positional', tools: 'positional' },
  })

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} validated preparation publication`, () => {
    for (const failure of [
      'invalid-prompt',
      'hook-error',
      'interrupt',
      'snapshot',
      'encode',
    ] as const)
      it.live(`${failure} publishes neither preparation facts nor a prepared checkpoint`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const open = yield* fixture
            const context = yield* open(backend)
            const interrupted = yield* Deferred.make<void>()
            const runtimeScope = yield* Scope.make()
            let calls = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  calls++
                  return answer
                }),
            })
            const model = descriptor(native)
            if (failure === 'encode') {
              const configuration = yield* Model.makeConfiguration({
                id: 'test/publication/failing-encode',
                version: 1,
                tags: [],
                schema: Schema.Null,
                restore: () => Effect.succeed(Context.empty()),
                capture: () =>
                  Effect.fail(
                    new HarnessError({
                      reason: 'invalid',
                      operation: 'codec.capture',
                      message: 'rejected',
                    }),
                  ),
              })
              Object.assign(model, { configuration })
            }
            const extension = Extension.make({
              name: 'publication',
              sections: [
                PromptSection.make({
                  key: 'proposed',
                  render: () => Effect.succeed('section-marker'),
                }),
              ],
              hooks: [
                Hook.make({
                  event: 'beforeRequest',
                  execute: ({ prompt }) => {
                    if (failure === 'hook-error')
                      return Schema.decodeUnknownEffect(Schema.String)(123).pipe(Effect.as(prompt))
                    if (failure === 'interrupt')
                      return Deferred.succeed(interrupted, undefined).pipe(
                        Effect.andThen(Effect.never),
                      )
                    if (failure === 'snapshot') Object.assign(model, { version: 0 })
                    // Deliberately simulate an untyped application returning malformed native data.
                    return Effect.succeed(
                      failure === 'invalid-prompt'
                        ? ({ content: 'invalid' } as unknown as Prompt.Prompt)
                        : prompt,
                    )
                  },
                }),
              ],
            })
            const runtime = yield* HarnessRuntime.make({
              models: [model],
              extensions: [extension],
            }).pipe(
              Effect.provideContext(context),
              Effect.provideService(Scope.Scope, runtimeScope),
            )
            yield* runtime.backend.root
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            if (failure === 'interrupt') {
              yield* Deferred.await(interrupted)
              yield* Scope.close(runtimeScope, Exit.void)
            } else {
              assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
              yield* Scope.close(runtimeScope, Exit.void)
            }
            assert.strictEqual(calls, 0)
            const inspection = {
              ...runtime,
              session: yield* Session.make().pipe(Effect.provideContext(context)),
            }
            assert.deepEqual(
              (yield* entries(inspection)).map((entry) => entry.kind),
              ['input'],
            )
            assert.isTrue(
              (yield* tasks(inspection)).every(
                (task) =>
                  !Schema.is(Schema.Struct({ preparation: Schema.Unknown }))(task.state.checkpoint),
              ),
            )
            assert.deepEqual(yield* StorageFormat.inspect.pipe(Effect.provideContext(context)), {
              formatVersion: 12,
              minimumReaderVersion: 12,
            })
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    it.live(
      'prefix offsets and final native prompt survive cold recovery without rerunning hooks',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const open = yield* fixture
            const entered = yield* Deferred.make<void>()
            const observed: Prepared.Snapshot[] = []
            let hooks = 0
            let renders = 0
            const native = yield* ScriptedStream.make({
              generateText: ({ prompt }) =>
                Effect.gen(function* () {
                  const current = Option.getOrThrow(
                    yield* Effect.serviceOption(Prepared.PreparedRequest),
                  ).current
                  observed.push(current)
                  assert.deepEqual(prompt.content, current.prompt.content)
                  if (observed.length === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }
                  return answer
                }),
            })
            const toolkit = Toolkit.make(
              Tool.make('work', { parameters: Schema.Struct({}), success: Schema.String }),
            )
            const extension = Extension.make({
              name: 'publication',
              tools: [toolkit.tools.work],
              sections: [
                PromptSection.make({
                  key: 'section',
                  render: () =>
                    Effect.sync(() => {
                      renders++
                      return 'section-marker'
                    }),
                }),
              ],
              hooks: [
                Hook.make({
                  event: 'beforeRequest',
                  execute: ({ prompt }) =>
                    Effect.sync(() => {
                      hooks++
                      return Prompt.concat(
                        Prompt.make([Prompt.systemMessage({ content: 'prefix' })]),
                        prompt,
                      )
                    }),
                }),
              ],
            })
            const model = descriptor(native)
            const make = Effect.gen(function* () {
              const context = yield* open(backend)
              const storage = Context.get(context, Storage.Storage)
              const observedStorage = Storage.Storage.of({
                ...storage,
                commit: (writes) => {
                  const batch = [...writes]
                  return Effect.sync(() => {
                    const facts = batch.filter(
                      (write) => write._tag === 'entry' && write.value.kind === 'context.change',
                    )
                    if (facts.length > 0) {
                      const checkpoints = batch.flatMap((write) =>
                        write._tag === 'task' &&
                        Schema.is(Schema.Struct({ preparation: Schema.Unknown }))(
                          write.value.state.checkpoint,
                        )
                          ? [write.value.state.checkpoint]
                          : [],
                      )
                      assert.strictEqual(
                        checkpoints.length,
                        1,
                        'facts and immutable preparation share one physical native commit',
                      )
                    }
                  }).pipe(Effect.andThen(storage.commit(batch)))
                },
              })
              const observedContext = Context.add(
                Context.add(context, Storage.Storage, observedStorage),
                StorageFormat.StorageFormat,
                { ...Context.get(context, StorageFormat.StorageFormat), storage: observedStorage },
              )
              return yield* HarnessRuntime.make({ models: [model], extensions: [extension] }).pipe(
                Effect.provideContext(observedContext),
                Effect.provide(toolkit.toLayer({ work: () => Effect.succeed('work') })),
              )
            })
            const scope = yield* Scope.make()
            const first = yield* make.pipe(Effect.provideService(Scope.Scope, scope))
            yield* first.backend.root
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: {
                type: 'input',
                content: [
                  Prompt.filePart({
                    mediaType: 'image/png',
                    data: Uint8Array.from([1, 2, 3]),
                    options: { openai: { imageDetail: 'high' } },
                  }),
                ],
              },
            })
            yield* Deferred.await(entered)
            const current = observed[0]
            assert.isDefined(current)
            assert.deepEqual(current?.translation.prompt.content, current?.prompt.content)
            assert.deepEqual(
              current?.translation.toolPositions.map((slot) => slot.messageIndex),
              [1],
            )
            assert.strictEqual(current?.prompt.content[1]?.role, 'system')
            assert.strictEqual(current?.prompt.content[1]?.content, 'section-marker')
            const facts = (yield* entries(first)).filter((entry) => entry.kind === 'context.change')
            assert.strictEqual(facts.length, 1)
            assert.strictEqual(observed[0]?.translation.frames[0]?.entryId, facts[0]?.id)
            yield* Scope.close(scope, Exit.void)
            const second = yield* make
            assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
            assert.deepEqual(observed[1], observed[0])
            assert.strictEqual(hooks, 1)
            assert.strictEqual(renders, 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    for (const transformation of ['missing', 'reordered'] as const)
      it.live(
        `a later ${transformation} marker rejection retains accepted facts and checkpoints`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const open = yield* fixture
              const context = yield* open(backend)
              let calls = 0
              let hooks = 0
              const native = yield* ScriptedStream.make({
                generateText: () =>
                  Effect.sync(() => {
                    calls++
                    return answer
                  }),
              })
              const extension = Extension.make({
                name: 'publication',
                sections: [
                  PromptSection.make({
                    key: 'section',
                    render: () => Effect.succeed(`section-${hooks}`),
                  }),
                ],
                hooks: [
                  Hook.make({
                    event: 'beforeRequest',
                    execute: ({ prompt }) =>
                      Effect.sync(() => {
                        if (++hooks === 1) return prompt
                        if (transformation === 'missing')
                          return Prompt.fromMessages(
                            prompt.content.filter((message) => message.role !== 'system'),
                          )
                        const markers = prompt.content
                          .filter((message) => message.role === 'system')
                          .toReversed()
                        return Prompt.fromMessages(
                          prompt.content.map((message) =>
                            message.role === 'system' ? (markers.shift() ?? message) : message,
                          ),
                        )
                      }),
                  }),
                ],
              })
              const runtime = yield* HarnessRuntime.make({
                models: [descriptor(native)],
                extensions: [extension],
              }).pipe(Effect.provideContext(context))
              yield* runtime.backend.root
              const first = yield* runtime.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'first' },
              })
              assert.strictEqual((yield* runtime.backend.wait(first.id)).status, 'done')
              const beforeEntries = yield* entries(runtime)
              const beforeTasks = yield* tasks(runtime)
              const second = yield* runtime.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'second' },
              })
              assert.strictEqual((yield* runtime.backend.wait(second.id)).status, 'unanswered')
              assert.strictEqual(calls, 1)
              assert.deepEqual(
                (yield* entries(runtime)).filter((entry) => entry.kind === 'context.change'),
                beforeEntries.filter((entry) => entry.kind === 'context.change'),
              )
              const afterTasks = yield* tasks(runtime)
              assert.deepEqual(
                afterTasks.find((task) => task.id === beforeTasks[0]?.id),
                beforeTasks[0],
              )
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )

    for (const change of ['missing', 'duplicate', 'changed'] as const)
      it.live(`rejects ${change} positional markers before publication`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const open = yield* fixture
            const context = yield* open(backend)
            let calls = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  calls++
                  return answer
                }),
            })
            const extension = Extension.make({
              name: 'publication',
              sections: [
                PromptSection.make({
                  key: 'section',
                  render: () => Effect.succeed('section-marker'),
                }),
              ],
              hooks: [
                Hook.make({
                  event: 'beforeRequest',
                  execute: ({ prompt }) => {
                    let messages: ReadonlyArray<Prompt.Message>
                    if (change === 'missing')
                      messages = prompt.content.filter((message) => message.role !== 'system')
                    else if (change === 'duplicate')
                      messages = [
                        ...prompt.content,
                        Prompt.systemMessage({ content: 'section-marker' }),
                      ]
                    else
                      messages = prompt.content.map((message) =>
                        message.role === 'system'
                          ? Prompt.systemMessage({ content: 'changed-marker' })
                          : message,
                      )
                    return Effect.succeed(Prompt.fromMessages(messages))
                  },
                }),
              ],
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native)],
              extensions: [extension],
            }).pipe(Effect.provideContext(context))
            yield* runtime.backend.root
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
            assert.strictEqual(calls, 0)
            assert.deepEqual(
              (yield* entries(runtime)).map((entry) => entry.kind),
              ['input'],
            )
            assert.deepEqual(yield* StorageFormat.inspect.pipe(Effect.provideContext(context)), {
              formatVersion: 12,
              minimumReaderVersion: 12,
            })
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
  })
}
