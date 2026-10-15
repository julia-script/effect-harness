import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Config from 'effect/Config'
import * as Console from 'effect/Console'
import * as Record from '../src/Record.js'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Conversation from '../src/Conversation.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Subagent from '../src/Subagent.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Usage from '../src/Usage.js'
import * as ToolkitFileSystem from '../src/ToolkitFileSystem.js'
import * as ToolkitProcess from '../src/ToolkitProcess.js'
import * as ImagePreparation from '../src/ImagePreparation.js'
import * as FileWatch from '../src/FileWatch.js'
import * as DirectoryReader from '../src/DirectoryReader.js'
import type { ImagePreparationError } from '../src/ImagePreparationError.js'
import { ExecutionError } from '../src/ExecutionError.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'
import { fixture, limits } from './fixtures/images-native/Conformance.js'

const nativeBun = 'Bun' in globalThis
const services = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : NodeServices.layer
const images: Layer.Layer<ImagePreparation.ImagePreparation, ImagePreparationError> = nativeBun
  ? (await import('../src/images-bun/index.js')).layer
  : (await import('../src/images-node/index.js')).layer

const nativeWatch = nativeBun
  ? (await import('../src/FileWatchBun.js')).layer
  : (await import('../src/FileWatchNode.js')).layer
const nativeDirectory = nativeBun
  ? (await import('../src/DirectoryReaderBun.js')).layer
  : (await import('../src/DirectoryReaderNode.js')).layer

class Binding extends Context.Service<Binding, { readonly label: string }>()(
  'subagent/test/Binding',
) {}
const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 2, uncached: 2, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}

describe('actual tool subagent binding', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    // Unsafe foreground cold replay is covered by ToolSubagentColdReplay on every backend.
    for (const [mode, replay] of [
      ['foreground', 'safe'],
      ['background', 'safe'],
      ['background', 'unsafe'],
    ] as const)
      it.live(
        `${backend}/${mode}/${replay}: cold tool caller retains paid child IDs and original accounting`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const coldDirectory = yield* Config.String(
                'HARNESS_TEST_SUBAGENT_COLD_DIRECTORY',
              ).pipe(Config.withDefault(''))
              const reopen = yield* Config.Boolean('HARNESS_TEST_SUBAGENT_COLD_REOPEN').pipe(
                Config.withDefault(false),
              )
              const directory =
                coldDirectory === '' ? yield* fs.makeTempDirectoryScoped() : coldDirectory
              const savedSchema = Schema.fromJsonString(
                Schema.Struct({
                  admissionId: Record.SubmissionId,
                  receipt: Subagent.ReceiptSchema,
                  handle: Subagent.HandleSchema,
                }),
              )
              const persisted = reopen
                ? yield* fs
                    .readFileString(`${directory}/fixture.json`)
                    .pipe(Effect.flatMap(Schema.decodeEffect(savedSchema)))
                : undefined
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const layer = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/cold.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/cold.sqlite` })),
                )
              })()
              const memory = backend === 'memory' ? yield* Layer.build(layer) : undefined
              const ready = yield* Deferred.make<Subagent.Receipt>()
              const toolkit = Toolkit.make(
                Tool.makeResult('cold-delegate', { success: Schema.String, replay }),
              )
              let warm = !reopen
              let requests = 0
              let handlers = 0
              let saved: Subagent.Handle | undefined = persisted?.handle
              const provider = yield* ScriptedStream.make({
                generateText: ({ prompt }) =>
                  Effect.sync(() => {
                    requests++
                    assert.isTrue(warm, 'Cold consumption must not repeat provider work')
                    const text = prompt.content
                      .flatMap((message) =>
                        message.role === 'user'
                          ? message.content
                              .filter((part) => part.type === 'text')
                              .map((part) => part.text)
                          : [],
                      )
                      .join(' ')
                    return text.includes('paid child')
                      ? [{ type: 'text' as const, text: 'paid' }, finish]
                      : [
                          {
                            type: 'tool-call' as const,
                            id: 'delegate',
                            name: 'cold-delegate',
                            params: {},
                            providerExecuted: false,
                          },
                          { ...finish, reason: 'tool-calls' as const },
                        ]
                  }),
              })
              const open = (scope: Scope.Scope) =>
                Effect.gen(function* () {
                  const storage = memory ?? (yield* Layer.buildWithScope(layer, scope))
                  const registry = yield* Registry.make.pipe(
                    Effect.provideService(Scope.Scope, scope),
                  )
                  yield* registry
                    .install(Registry.toolkit({ id: 'cold-delegate-owner', version: 1 }, toolkit))
                    .pipe(
                      Effect.provide(
                        toolkit.toLayer({
                          'cold-delegate': () =>
                            Effect.gen(function* () {
                              handlers++
                              const access = yield* ToolExecution
                              const handle = yield* Subagent.spawn(access, {
                                draft: { type: 'input', content: 'paid child' },
                                options: { key: 'saved', mode },
                              })
                              if (saved !== undefined) assert.deepStrictEqual(handle, saved)
                              saved = handle
                              const result = yield* Subagent.wait(access, handle)
                              if (result.receipt === undefined)
                                return yield* Effect.die('Missing paid receipt')
                              if (warm) {
                                yield* Deferred.succeed(ready, result.receipt)
                                return yield* Effect.never
                              }
                              return {
                                content: [Prompt.textPart({ text: 'reattached' })],
                                structuredOutput: 'reattached',
                                control: { terminate: true as const },
                              }
                            }).pipe(
                              Effect.mapError(
                                (cause) =>
                                  new ExecutionError({
                                    reason: 'invalid',
                                    operation: 'subagent.cold',
                                    message: 'Cold subagent failed',
                                    cause,
                                  }),
                              ),
                            ),
                        }),
                      ),
                    )
                  return yield* HarnessRuntime.make({
                    registry,
                    agent: {
                      tools: ['cold-delegate'],
                      toolkits: { inherit: false, registrations: ['cold-delegate-owner'] },
                    },
                  }).pipe(
                    Effect.provideService(Scope.Scope, scope),
                    Effect.provideContext(storage),
                    Effect.provideService(LanguageModel.LanguageModel, provider),
                  )
                })
              const firstScope = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
              const state =
                persisted ??
                (yield* Effect.gen(function* () {
                  const first = yield* open(firstScope)
                  const admission = yield* first.backend.submit({
                    conversationId: yield* first.backend.root,
                    draft: { type: 'input', content: 'parent delegate' },
                  })
                  const paid = yield* Deferred.await(ready)
                  assert.strictEqual(paid.submission.status, 'done')
                  assert.strictEqual(requests, 2)
                  assert.strictEqual(handlers, 1)
                  if (saved === undefined) return yield* Effect.die('Missing original handle')
                  const state = { admissionId: admission.id, receipt: paid, handle: saved }
                  if (coldDirectory !== '') {
                    yield* fs.writeFileString(
                      `${directory}/fixture.json`,
                      yield* Schema.encodeEffect(savedSchema)(state),
                    )
                    yield* Console.log(
                      `READY ${process.pid} provider=${requests} handler=${handlers}`,
                    )
                    return yield* Effect.never
                  }
                  yield* Scope.close(firstScope, Exit.void)
                  warm = false
                  return state
                }))
              const paid = state.receipt
              const secondScope = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
              const second = yield* open(secondScope)
              const outcome = yield* second.backend.wait(state.admissionId)
              assert.strictEqual(outcome.status, replay === 'safe' ? 'done' : 'unanswered')
              assert.strictEqual(handlers, (replay === 'safe' ? 1 : 0) + (reopen ? 0 : 1))
              assert.strictEqual(requests, reopen ? 0 : 2)
              assert.deepStrictEqual(yield* second.backend.wait(paid.submissionId), paid.submission)
              assert.lengthOf(
                yield* Session.commit(second.session, (tx) =>
                  Usage.receipts(tx, paid.conversationId),
                ),
                1,
              )
              const assistant = yield* Session.scanEntries(second.session, {
                conversationId: paid.conversationId,
              }).pipe(
                Stream.filter((entry) => entry.kind === 'assistant'),
                Stream.runCollect,
              )
              assert.lengthOf(assistant, 1)
              if (paid.submission._tag !== 'InputDone')
                return yield* Effect.die('Expected original paid input')
              assert.strictEqual(assistant[0]?.id, paid.submission.answer)
              assert.isTrue(Option.isNone(yield* second.scheduler.failure))
              if (reopen)
                yield* Console.log(
                  `COLD provider=${requests} handler=${handlers} original=${state.handle.taskId}`,
                )
            }),
          ).pipe(Effect.provide(services)),
      )
  // RuntimeBindingParity owns native-service reconstruction across durable backends.
  for (const mode of ['foreground', 'background'] as const)
    it.live(`${mode}: borrowed tool inherits independently bound native child services`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* fs.makeTempDirectoryScoped().pipe(Effect.flatMap(fs.realPath))
          const parentRoot = path.join(directory, 'parent')
          const leafRoot = path.join(directory, 'leaf')
          yield* fs.makeDirectory(parentRoot)
          yield* fs.makeDirectory(leafRoot)
          const nativeImages = yield* Layer.build(images).pipe(
            Effect.map((context) => Context.get(context, ImagePreparation.ImagePreparation)),
          )
          const imageCalls = { parent: 0, child: 0 }
          const imageService = (owner: 'parent' | 'child'): ImagePreparation.Service => ({
            prepare: (bytes, mime, limits) =>
              Effect.sync(() => imageCalls[owner]++).pipe(
                Effect.andThen(nativeImages.prepare(bytes, mime, limits)),
              ),
          })
          const bound = (root: string, label: string) =>
            Layer.mergeAll(
              ToolkitFileSystem.layer({ root, pathPolicy: 'within-root' }),
              ToolkitProcess.layer({ root, env: { SUBAGENT_BINDING: label } }),
              nativeWatch({ root, pathPolicy: 'within-root' }),
              nativeDirectory({ root, pathPolicy: 'within-root' }),
            )
          const parentServices = yield* Layer.build(bound(parentRoot, 'parent toolkit'))
          const leafServices = yield* Layer.build(bound(leafRoot, 'child toolkit'))
          const probe = (label: string, root: string) =>
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const path = yield* Path.Path
              const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
              const image = yield* ImagePreparation.ImagePreparation
              const watcher = yield* FileWatch.FileWatch
              const directoryReader = yield* DirectoryReader.DirectoryReader
              yield* fs.makeDirectory('owned-directory')
              yield* fs.writeFileString('owned-directory/value.txt', label)
              assert.deepStrictEqual(yield* fs.readDirectory('owned-directory'), ['value.txt'])
              const reader = yield* directoryReader.open('owned-directory', {
                requireOpenedIdentity: true,
                requireBoundedEnumeration: true,
              })
              assert.strictEqual(reader.path, `${root}/owned-directory`)
              assert.strictEqual(reader.capabilities.identity, 'opened-directory')
              assert.strictEqual(reader.capabilities.allocation, 'native-buffer')
              const page = yield* reader.next(1)
              assert.deepStrictEqual(
                page.entries.map((entry) => entry.name),
                ['value.txt'],
              )
              yield* reader.close
              assert.isTrue(Exit.isFailure(yield* reader.next(1).pipe(Effect.exit)))
              assert.isTrue(Exit.isFailure(yield* directoryReader.open('..').pipe(Effect.exit)))
              assert.isTrue(Exit.isFailure(yield* watcher.open([{ path: '..' }]).pipe(Effect.exit)))
              assert.isTrue(
                Exit.isFailure(yield* fs.readFileString('../outside.txt').pipe(Effect.exit)),
              )
              const watch = yield* watcher.open([{ path: 'owned-directory' }])
              assert.strictEqual(watch.coverage.mode, 'native')
              const events = yield* Stream.toPull(watch.events)
              yield* fs.writeFileString('owned-directory/watch.txt', label)
              const event = yield* events.pipe(Effect.timeout('5 seconds'))
              assert.isTrue(
                event.some((item) => {
                  const paths = item._tag === 'Change' ? [item.path] : item.paths
                  return paths.some(
                    (value) =>
                      value === `${root}/owned-directory` ||
                      value.startsWith(`${root}/owned-directory/`),
                  )
                }),
                'Actual native invalidation must stay in the selected toolkit watch root',
              )
              yield* watch.close
              yield* watch.close
              assert.deepStrictEqual(yield* watch.events.pipe(Stream.runCollect), [])
              assert.strictEqual(path.resolve('owned-directory'), `${root}/owned-directory`)
              assert.strictEqual(
                yield* spawner.string(
                  ChildProcess.make(process.execPath, [
                    '-e',
                    "process.stdout.write(require('node:fs').readFileSync('owned-directory/value.txt','utf8') + ':' + process.env.SUBAGENT_BINDING + ':' + process.cwd())",
                  ]),
                ),
                `${label}:${label}:${root}`,
              )
              assert.deepStrictEqual(
                (yield* image.prepare(fixture('opaque.png'), 'image/png', limits)).dimensions,
                { width: 3, height: 2 },
              )
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ExecutionError({
                    reason: 'invalid',
                    operation: 'subagent.native-probe',
                    message: 'Native probe failed',
                    cause,
                  }),
              ),
            )
          const delegate = Toolkit.make(
            Tool.makeResult('delegate', { success: Schema.String, replay: 'safe' })
              .addDependency(Binding)
              .addDependency(FileSystem.FileSystem)
              .addDependency(Path.Path)
              .addDependency(ChildProcessSpawner.ChildProcessSpawner)
              .addDependency(ImagePreparation.ImagePreparation)
              .addDependency(FileWatch.FileWatch)
              .addDependency(DirectoryReader.DirectoryReader),
          )
          const leaf = Toolkit.make(
            Tool.makeResult('bound-leaf', {
              success: Schema.String,
              replay: 'safe',
            })
              .addDependency(Binding)
              .addDependency(FileSystem.FileSystem)
              .addDependency(Path.Path)
              .addDependency(ChildProcessSpawner.ChildProcessSpawner)
              .addDependency(ImagePreparation.ImagePreparation)
              .addDependency(FileWatch.FileWatch)
              .addDependency(DirectoryReader.DirectoryReader),
          )
          const registry = yield* Registry.make
          let retained: ToolExecution['Service'] | undefined
          let retainedHandle: Subagent.Handle | undefined
          let leafCalls = 0
          let childId: Conversation.Conversation['id'] | undefined
          yield* registry.install(Registry.toolkit({ id: 'leaf-owner', version: 1 }, leaf)).pipe(
            Effect.provide(
              leaf.toLayer({
                'bound-leaf': () =>
                  Effect.gen(function* () {
                    leafCalls++
                    const binding = yield* Binding
                    const access = yield* ToolExecution
                    assert.strictEqual(binding.label, 'child toolkit')
                    yield* probe(binding.label, leafRoot)
                    childId = access.conversationId
                    return {
                      content: [Prompt.textPart({ text: binding.label })],
                      structuredOutput: binding.label,
                    }
                  }),
              }),
            ),
            Effect.provideService(Binding, { label: 'child toolkit' }),
            Effect.provideContext(leafServices),
            Effect.provideService(ImagePreparation.ImagePreparation, imageService('child')),
          )
          yield* registry
            .install(Registry.toolkit({ id: 'delegate-owner', version: 1 }, delegate))
            .pipe(
              Effect.provide(
                delegate.toLayer({
                  delegate: () =>
                    Effect.gen(function* () {
                      const binding = yield* Binding
                      assert.strictEqual(binding.label, 'parent toolkit')
                      yield* probe(binding.label, parentRoot)
                      const access = yield* ToolExecution
                      retained = access
                      const handle = yield* Subagent.spawn(access, {
                        draft: { type: 'input', content: 'child' },
                        options: { key: 'child', mode, agent: { tools: ['bound-leaf'] } },
                      })
                      retainedHandle = handle
                      const result = yield* Subagent.wait(access, handle)
                      if (result.receipt === undefined)
                        return yield* Effect.die('Missing actual delegated receipt')
                      assert.strictEqual(childId, result.receipt.conversationId)
                      const conversation = yield* Subagent.open(access, handle)
                      assert.strictEqual(
                        (yield* Conversation.agent(conversation)).instructions,
                        'parent stored instructions',
                      )
                      return {
                        content: [Prompt.textPart({ text: 'delegation finished' })],
                        structuredOutput: 'delegation finished',
                        control: { terminate: true as const },
                      }
                    }).pipe(
                      Effect.mapError(
                        (cause) =>
                          new ExecutionError({
                            reason: 'invalid',
                            operation: 'delegate.subagent',
                            message: 'Subagent failed',
                            cause,
                          }),
                      ),
                    ),
                }),
              ),
              Effect.provideService(Binding, { label: 'parent toolkit' }),
              Effect.provideContext(parentServices),
              Effect.provideService(ImagePreparation.ImagePreparation, imageService('parent')),
            )
          const model = yield* ScriptedStream.make({
            generateText: ({ prompt, tools }) =>
              Effect.sync(() => {
                const text = prompt.content
                  .flatMap((message) =>
                    message.role === 'user'
                      ? message.content
                          .filter((part) => part.type === 'text')
                          .map((part) => part.text)
                      : [],
                  )
                  .join(' ')
                const hadTool = prompt.content.some((message) => message.role === 'tool')
                if (hadTool) return [{ type: 'text' as const, text: 'child complete' }, finish]
                const name = text.includes('child') ? 'bound-leaf' : 'delegate'
                if (name === 'bound-leaf') {
                  const ids = tools.map((tool) => tool.name)
                  assert.deepStrictEqual(ids, ['bound-leaf'])
                }
                return [
                  {
                    type: 'tool-call' as const,
                    id: `${name}-call`,
                    name,
                    params: {},
                    providerExecuted: false,
                  },
                  { ...finish, reason: 'tool-calls' as const },
                ]
              }),
          })
          const storage = yield* Layer.build(Storage.layerMemory)
          const runtime = yield* HarnessRuntime.make({
            registry,
            agent: {
              instructions: 'parent stored instructions',
              tools: ['delegate', 'bound-leaf'],
              toolkits: { inherit: false, registrations: ['delegate-owner', 'leaf-owner'] },
            },
          }).pipe(
            Effect.provideContext(storage),
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideService(Binding, { label: 'outer ambient' }),
          )
          const root = yield* runtime.backend.root
          const admission = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'parent' },
          })
          const outcome = yield* runtime.backend.wait(admission.id)
          assert.strictEqual(outcome.status, 'done')
          assert.strictEqual(leafCalls, 1)
          assert.deepStrictEqual(imageCalls, { parent: 1, child: 1 })
          assert.strictEqual(
            yield* fs.readFileString(`${parentRoot}/owned-directory/value.txt`),
            'parent toolkit',
          )
          assert.strictEqual(
            yield* fs.readFileString(`${leafRoot}/owned-directory/value.txt`),
            'child toolkit',
          )
          if (retained === undefined || retainedHandle === undefined)
            return yield* Effect.die('Missing actual borrowed handle')
          assert.strictEqual(
            (yield* Subagent.read(retained, retainedHandle).pipe(Effect.exit))._tag,
            'Failure',
          )
          if (childId === undefined) return yield* Effect.die('Missing actual child conversation')
          const receiptConversation = childId
          assert.lengthOf(
            yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, receiptConversation)),
            3,
          )
          assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
        }),
      ).pipe(Effect.provide(services)),
    )
})
