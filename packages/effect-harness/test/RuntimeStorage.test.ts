import * as Registry from 'effect-harness/Registry'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as ToolInvocation from 'effect-harness/ToolInvocation'
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as Conversation from 'effect-harness/Conversation'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Tool from 'effect-harness/Tool'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as View from 'effect-harness/ConversationView'
import * as ScriptedStream from './fixtures/ScriptedStream.js'
import * as ResultEntry from '../src/internal/ToolResultEntry.js'
const finish: Response.PartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const images = Array.from({ length: 4 }, (_, index) =>
  Buffer.alloc(48 * 1024, index + 33).toString('base64'),
)
const occurrences = (value: unknown, image: string) => JSON.stringify(value).split(image).length - 1
const root = Record.ROOT_CONVERSATION_ID
for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const ending of ['complete', 'abort'] as const) {
    it.live(
      `${backend}: image-rich rounds retain one result and one pending request across cold recovery and ${ending}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
            const layer = (() => {
              if (backend === 'sqlite')
                return Storage.layerSql.pipe(
                  Layer.provide(SqliteClient.layer({ filename: `${directory}/store.db` })),
                )
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })
              return Storage.layerMemory
            })()
            const storageScope = yield* Scope.make()
            const store = yield* Layer.build(Layer.fresh(layer)).pipe(
              Effect.provideService(Scope.Scope, storageScope),
            )
            const storage = Context.get(store, Storage.Storage)
            let activeStore = store
            const ready = yield* Deferred.make<void>()
            const resumed = yield* Deferred.make<void>()
            let recovering = false
            let pinned: Prepared.Snapshot | undefined
            let calls = 0
            const toolkit = Toolkit.make(
              Tool.makeResult('image', {
                parameters: Schema.Struct({ index: Schema.Natural }),
                success: Schema.Struct({ index: Schema.Natural }),
              }),
            )
            const handlers = toolkit.toLayer({
              image: ({ index }) =>
                Effect.sync(() => {
                  calls++
                  return {
                    content: [
                      Prompt.filePart({
                        mediaType: 'image/png',
                        data: images[index]!,
                        options: { openai: { imageDetail: 'high' } },
                      }),
                    ],
                    structuredOutput: { index },
                  }
                }),
            })
            const model = yield* ScriptedStream.make({
              generateText: ({ prompt }) =>
                Effect.gen(function* () {
                  assert.deepStrictEqual(
                    yield* StorageFormat.inspect.pipe(
                      Effect.provideContext(activeStore),
                      Effect.orDie,
                    ),
                    { formatVersion: 11, minimumReaderVersion: 11 },
                  )
                  const results = prompt.content.flatMap((message) =>
                    message.role === 'tool'
                      ? message.content.filter((part) => part.type === 'tool-result')
                      : [],
                  )
                  for (const [index, part] of results.entries()) {
                    const envelope = yield* Schema.decodeUnknownEffect(
                      Schema.toCodecJson(ToolResult.Envelope),
                    )(part.result).pipe(Effect.orDie)
                    assert.deepStrictEqual(envelope.content, [
                      Prompt.filePart({
                        mediaType: 'image/png',
                        data: images[index]!,
                        options: { openai: { imageDetail: 'high' } },
                      }),
                    ])
                  }
                  if (results.length < 4)
                    return [
                      {
                        type: 'tool-call',
                        id: `call-${results.length}`,
                        name: 'image',
                        params: { index: results.length },
                        providerExecuted: false,
                      },
                      finish,
                    ]
                  const request = Option.getOrThrow(
                    yield* Effect.serviceOption(Prepared.PreparedRequest),
                  ).current
                  if (!recovering) {
                    pinned = request
                    yield* Deferred.succeed(ready, undefined)
                    return yield* Effect.never
                  }
                  assert.deepStrictEqual(request, pinned)
                  assert.deepStrictEqual(prompt, pinned!.prompt)
                  yield* Deferred.succeed(resumed, undefined)
                  if (ending === 'abort') return yield* Effect.never
                  return [{ type: 'text', text: 'done' }, finish]
                }),
            })
            const open = (
              context: Context.Context<Storage.Storage | StorageFormat.StorageFormat>,
            ) =>
              HarnessRuntime.make({ tools: toolkit, documentRetentionMs: 0 }).pipe(
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provide(handlers),
                Effect.provideContext(context),
              )
            const firstScope = yield* Scope.make()
            const first = yield* open(store).pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* first.backend.root
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'four images' },
            })
            yield* Deferred.await(ready)
            const tasks = yield* storage.scanTasks({ conversationId: root }).pipe(Stream.runCollect)
            const requests = yield* Effect.forEach(tasks, (task) =>
              storage
                .scanDocuments({
                  scope: { _tag: 'task', taskId: task.id },
                  kind: 'harness.request',
                })
                .pipe(Stream.runCollect),
            )
            assert.strictEqual(requests.flat().length, 1)
            const pending = Option.getOrThrow(yield* storage.document(requests.flat()[0]!.id))
            const entries = yield* storage
              .scanEntries({ conversationId: root })
              .pipe(Stream.runCollect)
            const rounds = yield* storage
              .scanDocuments({
                scope: { _tag: 'conversation', conversationId: root },
                kind: 'harness.tool.round',
              })
              .pipe(
                Stream.runCollect,
                Effect.flatMap((docs) => Effect.forEach(docs, (doc) => storage.document(doc.id))),
              )
            for (const image of images) {
              assert.strictEqual(
                occurrences(tasks, image),
                0,
                'permanent tasks contain references, never conversation images',
              )
              assert.strictEqual(occurrences(rounds, image), 0, 'round receipts refer to entries')
              assert.strictEqual(
                occurrences(pending.value, image),
                1,
                'translation shares the one pinned prompt',
              )
              assert.strictEqual(occurrences(entries, image), 1, 'one canonical result content')
            }
            const bytes = new TextEncoder().encode(
              JSON.stringify({ tasks, rounds, pending, entries }),
            ).length
            assert.isBelow(
              bytes,
              images.reduce((size, image) => size + image.length, 0) * 2 + 120_000,
            )
            yield* Scope.close(firstScope, Exit.void)
            if (backend !== 'memory') yield* Scope.close(storageScope, Exit.void)
            else yield* Effect.addFinalizer(() => Scope.close(storageScope, Exit.void))
            recovering = true
            const coldStore = backend === 'memory' ? store : yield* Layer.build(Layer.fresh(layer))
            activeStore = coldStore
            const second = yield* open(coldStore)
            const settled = yield* second.backend.wait(job.id).pipe(Effect.forkChild)
            if (ending === 'abort') {
              yield* Deferred.await(resumed)
              yield* second.backend.abort(root)
            }
            assert.strictEqual(
              (yield* Fiber.join(settled)).status,
              ending === 'complete' ? 'done' : 'unanswered',
            )
            assert.strictEqual(calls, 4, 'committed tools are not replayed')
            const completed = yield* Session.scanTasks(second.session, {
              conversationId: root,
            }).pipe(Stream.runCollect)
            const coldStorage = Context.get(coldStore, Storage.Storage)
            for (const task of completed) {
              assert.strictEqual(task.state.status, 'terminal')
              assert.isUndefined(task.state.checkpoint)
              assert.deepStrictEqual(
                yield* coldStorage
                  .scanDocuments({
                    scope: { _tag: 'task', taskId: task.id },
                    kind: 'harness.request',
                  })
                  .pipe(Stream.runCollect),
                [],
              )
            }
            const client = yield* Harness.make.pipe(
              Effect.provideService(HarnessBackend, second.backend),
            )
            const conversation = yield* client.root
            const view = yield* View.snapshot(conversation)
            assert.strictEqual(view.value.rounds.length, 4)
            assert.isTrue(
              view.value.rounds.every(
                (round) => round.applied && round.slots[0]?.result !== undefined,
              ),
            )
            const fork = yield* Conversation.fork(conversation)
            const forkView = yield* View.snapshot(fork)
            assert.deepStrictEqual(forkView.value.context.prompt, view.value.context.prompt)
            const results = forkView.value.entries.filter((entry) => entry.kind === 'tool.result')
            assert.strictEqual(results.length, 4)
            for (const [index, entry] of results.entries()) {
              assert.isUndefined(entry.model)
              assert.deepStrictEqual((yield* ResultEntry.read(entry)).structuredOutput, { index })
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const payload of ['text', 'image', 'json'] as const) {
    for (const ending of ['complete', 'abort'] as const) {
      it.live(
        `nested ${backend} ${payload} values remain caller-owned through cold resume and ${ending}`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
              const layer = (() => {
                if (backend === 'sqlite')
                  return Storage.layerSql.pipe(
                    Layer.provide(SqliteClient.layer({ filename: `${directory}/store.db` })),
                  )
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })
                return Storage.layerMemory
              })()
              const storageScope = yield* Scope.make()
              let store = yield* Layer.build(Layer.fresh(layer)).pipe(
                Effect.provideService(Scope.Scope, storageScope),
              )
              yield* Effect.addFinalizer(() => Scope.close(storageScope, Exit.void))
              const storage = Context.get(store, Storage.Storage)
              const ready = yield* Deferred.make<void>()
              let recovering = false
              let executions = 0
              let value: Schema.Json = images[0]!
              if (payload === 'json')
                value = {
                  detailsFromContent: true,
                  valueFromDetails: true,
                  valueFromContent: true,
                  legacyFromDetails: true,
                  structuredOutput: images[0]!,
                  details: null,
                }
              if (payload === 'image')
                value = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
                  _tag: '@effect-harness/ToolContent',
                  content: [
                    Prompt.filePart({
                      mediaType: 'image/png',
                      data: images[0]!,
                      options: { openai: { imageDetail: 'high' } },
                    }),
                  ],
                })
              const child = Tool.make('echo', {
                parameters: Schema.Struct({ value: Schema.Json }),
                success: Schema.Json,
                callers: ['tools'],
                replay: 'safe',
              })
              const toolkit = Toolkit.make(child)
              const contract = {
                kind: 'storage.parent',
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
                success: Schema.String,
                failure: Schema.String,
              } as const
              const parent = Task.make({
                ...contract,
                initial: () => ({ phase: 'execute' as const }),
                phases: {
                  execute: () =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      const receipt = yield* ToolInvocation.invoke(
                        access,
                        child,
                        { value },
                        { key: 'echo', progress: false },
                      )
                      assert.strictEqual(receipt.status, 'succeeded')
                      if (receipt.status !== 'succeeded')
                        return yield* Effect.die('Expected successful nested result')
                      assert.deepStrictEqual(receipt.value, value)
                      assert.deepStrictEqual(receipt.details, value)
                      if (!recovering || ending === 'abort') {
                        yield* Deferred.succeed(ready, undefined)
                        return yield* Effect.never
                      }
                      yield* access.commit((tx) =>
                        access.progress(tx, contract, {
                          status: 'completing',
                          outcome: { status: 'completed', result: 'done' },
                        }),
                      )
                    }),
                },
              })
              const model = yield* ScriptedStream.make({
                generateText: () => Effect.die('Generic parent does not use a model'),
              })
              const open = Effect.gen(function* () {
                const runtime = yield* HarnessRuntime.make({
                  tools: toolkit,
                  documentRetentionMs: 0,
                }).pipe(
                  Effect.provideContext(store),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provide(
                    toolkit.toLayer({
                      echo: ({ value }) =>
                        Effect.sync(() => {
                          executions++
                          return value
                        }),
                    }),
                  ),
                )
                yield* runtime.registry.install(
                  Registry.withTask(
                    Registry.toolkit({ id: 'storage.parent', version: 1 }, Toolkit.make()),
                    parent,
                  ),
                )
                return runtime
              })
              const owner = yield* Scope.make()
              const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
              yield* first.backend.root
              const ref = yield* first.scheduler.start(parent, undefined, { conversationId: root })
              yield* Deferred.await(ready)
              const tasks = yield* storage
                .scanTasks({ conversationId: root })
                .pipe(Stream.runCollect)
              assert.strictEqual(occurrences(tasks, images[0]!), 0)
              const docs = yield* storage
                .scanDocuments({ scope: { _tag: 'task', taskId: ref.id } })
                .pipe(
                  Stream.runCollect,
                  Effect.flatMap((records) =>
                    Effect.forEach(records, (record) => storage.document(record.id)),
                  ),
                )
              for (const doc of docs) assert.isAtMost(occurrences(doc, images[0]!), 1)
              assert.strictEqual(
                occurrences(docs, images[0]!),
                2,
                'one invocation argument and one canonical result',
              )
              const observed = yield* storage
                .scanDocuments({
                  scope: { _tag: 'conversation', conversationId: root },
                  kind: 'harness.tool.nested.receipt',
                })
                .pipe(
                  Stream.runCollect,
                  Effect.flatMap((records) =>
                    Effect.forEach(records, (record) => storage.document(record.id)),
                  ),
                )
              assert.strictEqual(occurrences(observed, images[0]!), 0)
              yield* Scope.close(owner, Exit.void)
              if (backend !== 'memory') {
                yield* Scope.close(storageScope, Exit.void)
                store = yield* Layer.build(Layer.fresh(layer))
              }
              const coldStorage = Context.get(store, Storage.Storage)
              recovering = true
              const second = yield* open
              if (ending === 'abort') yield* second.scheduler.abort(ref.id)
              assert.strictEqual(
                (yield* second.scheduler.wait(ref)).status,
                ending === 'abort' ? 'aborted' : 'completed',
              )
              assert.strictEqual(
                executions,
                1,
                'the nested result survives recovery without reinvocation',
              )
              assert.deepStrictEqual(
                yield* coldStorage
                  .scanDocuments({ scope: { _tag: 'task', taskId: ref.id } })
                  .pipe(Stream.runCollect),
                [],
              )
              const terminal = yield* coldStorage
                .scanTasks({ conversationId: root })
                .pipe(Stream.runCollect)
              assert.strictEqual(occurrences(terminal, images[0]!), 0)
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )
    }
  }
}
