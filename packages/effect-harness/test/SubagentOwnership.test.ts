import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Exit from 'effect/Exit'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as ConversationInitializer from '../src/ConversationInitializer.js'
import * as Conversation from '../src/Conversation.js'
import * as CompactionTask from '../src/CompactionTask.js'
import { HarnessBackend, type HarnessBackendService } from '../src/HarnessBackend.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Subagent from '../src/Subagent.js'
import * as Document from '../src/Document.js'
import * as Submission from '../src/Submission.js'
import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'
import * as Model from '../src/Model.js'
import * as Settings from '../src/Settings.js'
import * as Usage from '../src/Usage.js'
import { CompactionRequest } from '../src/CompactionRequest.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const nativeBun = 'Bun' in globalThis
const services = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : NodeServices.layer

class DecoderBinding extends Context.Service<
  DecoderBinding,
  {
    readonly label: string
    readonly touch: Effect.Effect<void>
  }
>()('subagent/test/DecoderBinding') {}

describe('subagent native ownership', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const observation of ['snapshot', 'watch'] as const)
      for (const cancellation of ['interruptible', 'held', 'held-fault'] as const)
        it.live(
          `${backend}/${observation}/${cancellation}: revoked decoding joins callbacks before releasing scopes and registration leases`,
          () =>
            Effect.scoped(
              Effect.gen(function* () {
                const ready = yield* Deferred.make<Conversation.Conversation>()
                const finishCaller = yield* Deferred.make<void>()
                const decoding = yield* Deferred.make<void>()
                const finishDecode = yield* Deferred.make<void>()
                const leaseClosed = yield* Deferred.make<void>()
                const constructorScope = yield* Scope.make()
                yield* Scope.close(constructorScope, Exit.void)
                let alive = true
                let decoderClosed = 0
                let leaseReleases = 0
                let callbackSettled = false
                let decoderCalls = 0
                const stored = Document.define({
                  kind: 'subagent.decoder',
                  version: 1,
                  scope: 'conversation',
                  history: 'latest',
                  fork: 'current',
                  schema: Schema.Struct({ value: Schema.Finite }),
                  initial: () => ({ value: 1 }),
                })
                const reading = Document.define({
                  ...stored.definition,
                  schema: Schema.Struct({
                    value: Schema.Finite.pipe(
                      Schema.middlewareDecoding((decode) =>
                        Effect.gen(function* () {
                          decoderCalls++
                          const service = yield* DecoderBinding
                          assert.strictEqual(service.label, 'consumer')
                          assert.notStrictEqual(yield* Scope.Scope, constructorScope)
                          yield* service.touch
                          yield* Effect.addFinalizer(() =>
                            Effect.sync(() => {
                              if (cancellation !== 'interruptible') assert.isTrue(callbackSettled)
                              assert.isTrue(alive)
                              decoderClosed++
                            }),
                          )
                          const paused = Deferred.succeed(decoding, undefined).pipe(
                            Effect.andThen(Deferred.await(finishDecode)),
                            Effect.tap(() =>
                              Effect.sync(() => {
                                callbackSettled = true
                              }),
                            ),
                          )
                          const waiting =
                            cancellation === 'held-fault'
                              ? paused.pipe(
                                  Effect.andThen(
                                    Effect.failCause(
                                      Cause.fromReasons([
                                        ...Cause.die('decoder fault one').reasons,
                                        ...Cause.die('decoder fault two').reasons,
                                      ]),
                                    ),
                                  ),
                                )
                              : paused
                          yield* cancellation !== 'interruptible'
                            ? Effect.uninterruptible(waiting)
                            : waiting
                          yield* service.touch
                          return yield* decode
                        }),
                      ),
                    ),
                  }),
                })
                const contract = {
                  kind: 'subagent.decoder-owner',
                  version: 1,
                  input: Schema.Void,
                  checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                  success: Schema.Void,
                  failure: Schema.String,
                } as const
                let captured: Context.Service.Shape<typeof DecoderBinding> | undefined
                const program = Task.make({
                  ...contract,
                  initial: () => ({ phase: 'run' as const }),
                  phases: {
                    run: () =>
                      Effect.gen(function* () {
                        captured = yield* DecoderBinding
                        assert.strictEqual(captured.label, 'parent')
                        const access = yield* TaskExecution
                        const handle = yield* Subagent.spawn(access, {
                          draft: { type: 'write', entry: { kind: 'decoder.source' } },
                          options: { key: 'decoder' },
                        })
                        yield* Subagent.wait(access, handle)
                        const child = yield* Subagent.open(access, handle)
                        yield* access.commit((tx) =>
                          Transaction.ensureDocument(tx, stored, {
                            scope: { _tag: 'conversation', conversationId: child.id },
                          }),
                        )
                        yield* Deferred.succeed(ready, child)
                        yield* Deferred.await(finishCaller)
                        yield* access.commit((tx) =>
                          access.progress(tx, contract, {
                            status: 'completing',
                            outcome: { status: 'completed', result: undefined },
                          }),
                        )
                      }),
                  },
                })
                const registry = yield* Registry.make
                yield* registry.install(
                  Registry.provide(
                    Registry.withTask(
                      Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                      program,
                    ),
                    Layer.effect(
                      DecoderBinding,
                      Effect.acquireRelease(
                        Effect.succeed({
                          label: 'parent',
                          touch: Effect.sync(() => assert.isTrue(alive)),
                        }),
                        () =>
                          Effect.sync(() => {
                            alive = false
                            leaseReleases++
                          }).pipe(Effect.andThen(Deferred.succeed(leaseClosed, undefined))),
                      ),
                    ),
                  ),
                )
                const fs = yield* FileSystem.FileSystem
                const directory = yield* fs.makeTempDirectoryScoped()
                const sqlite = nativeBun
                  ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient')))
                      .layer
                  : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                      .layer
                const layer = (() => {
                  if (backend === 'memory') return Storage.layerMemory
                  if (backend === 'jsonl')
                    return Storage.layerJsonl({ filePath: `${directory}/decode.jsonl` })
                  return Storage.layerJournal().pipe(
                    Layer.provide(SqlEventJournal.layer()),
                    Layer.provide(sqlite({ filename: `${directory}/decode.sqlite` })),
                  )
                })()
                const storage = yield* Layer.build(layer)
                const model = yield* LanguageModel.make({
                  generateText: () => Effect.die('No provider'),
                  streamText: () => Stream.die('No provider'),
                })
                const runtime = yield* HarnessRuntime.make({ registry }).pipe(
                  Effect.provideContext(storage),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                )
                const ref = yield* runtime.scheduler.start(program, undefined, {
                  conversationId: yield* runtime.backend.root,
                })
                const child = yield* Effect.raceFirst(
                  Deferred.await(ready),
                  runtime.scheduler.wait(ref).pipe(Effect.flatMap(Effect.die)),
                )
                if (captured === undefined)
                  return yield* Effect.die('Missing captured registration service')
                const target = {
                  scope: { _tag: 'conversation' as const, conversationId: child.id },
                }
                const run =
                  observation === 'snapshot'
                    ? Conversation.snapshot(child, reading, target).pipe(Effect.asVoid)
                    : Conversation.watch(child, reading, target).pipe(
                        Stream.take(1),
                        Stream.runDrain,
                      )
                const foreign = yield* runtime.backend.create()
                const denied =
                  observation === 'snapshot'
                    ? Conversation.snapshot(child, reading, {
                        scope: { _tag: 'conversation', conversationId: foreign },
                      }).pipe(Effect.asVoid)
                    : Conversation.watch(child, reading, {
                        scope: { _tag: 'conversation', conversationId: foreign },
                      }).pipe(Stream.take(1), Stream.runDrain)
                assert.strictEqual(
                  (yield* denied.pipe(
                    Effect.provideService(DecoderBinding, { ...captured, label: 'consumer' }),
                    Effect.exit,
                  ))._tag,
                  'Failure',
                )
                assert.strictEqual(decoderCalls, 0)
                const observer = yield* run.pipe(
                  Effect.provideService(DecoderBinding, { ...captured, label: 'consumer' }),
                  Effect.provideService(Scope.Scope, constructorScope),
                  Effect.exit,
                  Effect.forkChild,
                )
                yield* Deferred.await(decoding)
                yield* registry.uninstall(contract.kind)
                assert.strictEqual(leaseReleases, 0)
                yield* Deferred.succeed(finishCaller, undefined)
                assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
                if (cancellation !== 'interruptible') {
                  assert.strictEqual(decoderClosed, 0)
                  assert.strictEqual(leaseReleases, 0)
                }
                yield* Deferred.succeed(finishDecode, undefined)
                const decoded = yield* Fiber.join(observer)
                assert.strictEqual(decoded._tag, 'Failure')
                if (cancellation === 'held-fault' && Exit.isFailure(decoded)) {
                  const detail = Cause.pretty(decoded.cause)
                  assert.include(detail, 'decoder fault one')
                  assert.include(detail, 'decoder fault two')
                }
                assert.strictEqual(decoderClosed, 1)
                yield* Deferred.await(leaseClosed)
                assert.strictEqual(leaseReleases, 1)
                assert.isFalse(alive)
                let definitionReads = 0
                const staleDocument = {
                  ...reading,
                  get definition() {
                    definitionReads++
                    return reading.definition
                  },
                }
                const stale =
                  observation === 'snapshot'
                    ? Conversation.snapshot(child, staleDocument, target).pipe(Effect.asVoid)
                    : Conversation.watch(child, staleDocument, target).pipe(
                        Stream.take(1),
                        Stream.runDrain,
                      )
                assert.strictEqual(
                  (yield* stale.pipe(
                    Effect.provideService(DecoderBinding, { ...captured, label: 'consumer' }),
                    Effect.exit,
                  ))._tag,
                  'Failure',
                )
                assert.strictEqual(definitionReads, 0)
                assert.strictEqual(decoderCalls, 1)
              }),
            ).pipe(Effect.provide(services)),
        )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: bound cross-conversation configure, reset and fork retain authority until the caller ends`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            let retained: Conversation.Conversation | undefined
            let resetReceipt: Submission.Submission | undefined
            const contract = {
              kind: 'subagent.bound-controls',
              version: 1,
              input: Schema.Void,
              checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
              success: Schema.Void,
              failure: Schema.String,
            } as const
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const handle = yield* Subagent.spawn(access, {
                      draft: { type: 'write', entry: { kind: 'control.source' } },
                      options: { key: 'child' },
                    })
                    yield* Subagent.wait(access, handle)
                    const child = yield* Subagent.open(access, handle)
                    retained = child
                    yield* Conversation.configure(child, { instructions: 'owned configuration' })
                    assert.strictEqual(
                      (yield* Conversation.agent(child)).instructions,
                      'owned configuration',
                    )
                    const reset = yield* Conversation.reset(child, {
                      handoff: 'native gated reset',
                      requestId: 'reset',
                    })
                    resetReceipt = reset
                    assert.strictEqual((yield* Submission.wait(reset)).status, 'done')
                    const fork = yield* Conversation.fork(child)
                    assert.strictEqual(
                      (yield* Conversation.agent(fork)).instructions,
                      'owned configuration',
                    )
                    assert.strictEqual(
                      (yield* Submission.wait(
                        yield* Conversation.reset(fork, { handoff: 'fork reset' }),
                      )).status,
                      'done',
                    )
                    yield* Conversation.abort(fork)
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: undefined },
                      }),
                    )
                  }),
              },
            })
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const layer = (() => {
              if (backend === 'memory') return Storage.layerMemory
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/controls.jsonl` })
              return Storage.layerJournal().pipe(
                Layer.provide(SqlEventJournal.layer()),
                Layer.provide(sqlite({ filename: `${directory}/controls.sqlite` })),
              )
            })()
            const storage = yield* Layer.build(layer)
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const runtime = yield* HarnessRuntime.make({ registry }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const ref = yield* runtime.scheduler.start(program, undefined, {
              conversationId: yield* runtime.backend.root,
            })
            assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
            if (retained === undefined || resetReceipt === undefined)
              return yield* Effect.die('Missing actual returned handles')
            let reads = 0
            const options = {
              get handoff() {
                reads++
                return 'late'
              },
            }
            assert.strictEqual(
              (yield* Conversation.reset(retained, options).pipe(Effect.exit))._tag,
              'Failure',
            )
            assert.strictEqual(reads, 0)
            assert.strictEqual(
              (yield* Submission.read(resetReceipt).pipe(Effect.exit))._tag,
              'Failure',
            )
            assert.strictEqual(
              (yield* Conversation.entries(retained).pipe(Stream.runCollect, Effect.exit))._tag,
              'Failure',
            )
          }),
        ).pipe(Effect.provide(services)),
    )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(`${backend}: live compaction identities cannot fall back after caller revocation`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          let retained: CompactionTask.Handle | undefined
          let ambient: HarnessBackendService | undefined
          const contract = {
            kind: 'subagent.bound-compaction',
            version: 1,
            input: Schema.Void,
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
            success: Schema.Void,
            failure: Schema.String,
          } as const
          const program = Task.make({
            ...contract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  if (ambient === undefined) return yield* Effect.die('Missing ambient backend')
                  const child = yield* Subagent.open(
                    access,
                    yield* Subagent.spawn(access, {
                      draft: { type: 'write', entry: { kind: 'compact.source' } },
                      options: { key: 'compact' },
                    }),
                  )
                  const handle = yield* Conversation.compact(child)
                  retained = handle
                  const read = CompactionTask.read(handle).pipe(
                    Effect.provideService(HarnessBackend, ambient),
                  )
                  assert.strictEqual((yield* read).id, handle.id)
                  const id = handle.id
                  Object.assign(handle, { id: access.taskId })
                  assert.strictEqual((yield* read.pipe(Effect.exit))._tag, 'Failure')
                  Object.assign(handle, { id })
                  const conversationId = handle.conversationId
                  Object.assign(handle, { conversationId: access.conversationId })
                  assert.strictEqual((yield* read.pipe(Effect.exit))._tag, 'Failure')
                  Object.assign(handle, { conversationId })
                  assert.strictEqual(
                    (yield* CompactionTask.wait(handle).pipe(
                      Effect.provideService(HarnessBackend, ambient),
                    )).status,
                    'completed',
                  )
                  yield* CompactionTask.abort(handle).pipe(
                    Effect.provideService(HarnessBackend, ambient),
                  )
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: undefined },
                    }),
                  )
                }),
            },
          })
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const sqlite = nativeBun
            ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
            : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
          const layer = (() => {
            if (backend === 'memory') return Storage.layerMemory
            if (backend === 'jsonl')
              return Storage.layerJsonl({ filePath: `${directory}/content.jsonl` })
            return Storage.layerJournal().pipe(
              Layer.provide(SqlEventJournal.layer()),
              Layer.provide(sqlite({ filename: `${directory}/content.sqlite` })),
            )
          })()
          const storage = yield* Layer.build(layer)
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No provider'),
            streamText: () => Stream.die('No provider'),
          })
          const runtime = yield* HarnessRuntime.make({ registry }).pipe(
            Effect.provideContext(storage),
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          ambient = runtime.backend
          const ref = yield* runtime.scheduler.start(program, undefined, {
            conversationId: yield* runtime.backend.root,
          })
          assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
          if (retained === undefined) return yield* Effect.die('No live compaction handle')
          for (const operation of [CompactionTask.read, CompactionTask.wait, CompactionTask.abort])
            assert.strictEqual(
              (yield* operation(retained).pipe(
                Effect.provideService(HarnessBackend, runtime.backend),
                Effect.exit,
              ))._tag,
              'Failure',
            )
          assert.strictEqual(
            (yield* CompactionTask.read({ ...retained }).pipe(
              Effect.provideService(HarnessBackend, runtime.backend),
            )).state.status,
            'terminal',
          )
        }),
      ).pipe(Effect.provide(services)),
    )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const observerState of ['current', 'revoked'] as const)
      it.live(
        `${backend}/${observerState}: actual bound compaction queues one paid durable summary`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const ready = yield* Deferred.make<CompactionTask.Handle>()
              const requested = yield* Deferred.make<void>()
              const releaseProvider = yield* Deferred.make<void>()
              const finishCaller = yield* Deferred.make<void>()
              let requests = 0
              const contract = {
                kind: 'subagent.paid-compaction',
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                success: Schema.Void,
                failure: Schema.String,
              } as const
              const program = Task.make({
                ...contract,
                initial: () => ({ phase: 'run' as const }),
                phases: {
                  run: () =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      const draft = (text: string) =>
                        Effect.gen(function* () {
                          const model = yield* Schema.encodeEffect(
                            Schema.Array(Schema.toCodecJson(Prompt.Message)),
                          )([Prompt.userMessage({ content: [Prompt.textPart({ text })] })])
                          return { type: 'write' as const, entry: { kind: 'input', model } }
                        })
                      const supervisor = yield* Subagent.spawn(access, {
                        draft: yield* draft('old '.repeat(40)),
                        options: { key: 'summary' },
                      })
                      yield* Subagent.wait(access, supervisor)
                      const child = yield* Subagent.open(access, supervisor)
                      yield* Submission.wait(
                        yield* Conversation.submit(child, yield* draft('recent '.repeat(12))),
                      )
                      const compaction = yield* Conversation.compact(child)
                      yield* Deferred.succeed(ready, compaction)
                      yield* Deferred.await(finishCaller)
                      yield* access.commit((tx) =>
                        access.progress(tx, contract, {
                          status: 'completing',
                          outcome: { status: 'completed', result: undefined },
                        }),
                      )
                    }),
                },
              })
              const registry = yield* Registry.make
              yield* registry.install(
                Registry.withTask(
                  Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                  program,
                ),
              )
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const layer = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/compact.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/compact.sqlite` })),
                )
              })()
              const storage = yield* Layer.build(layer)
              const provider = yield* ScriptedStream.make({
                generateText: () =>
                  Effect.gen(function* () {
                    requests++
                    assert.strictEqual(
                      Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
                        .maxOutputTokens,
                      9,
                    )
                    yield* Deferred.succeed(requested, undefined)
                    yield* Deferred.await(releaseProvider)
                    return [
                      { type: 'text' as const, text: 'Remember the old conversation.' },
                      {
                        type: 'finish' as const,
                        reason: 'stop' as const,
                        usage: {
                          inputTokens: {
                            total: 12,
                            uncached: 12,
                            cacheRead: undefined,
                            cacheWrite: undefined,
                          },
                          outputTokens: { total: 3, text: 3, reasoning: undefined },
                        },
                        response: undefined,
                      },
                    ]
                  }),
              })
              const capability = yield* Model.makeCompaction({
                binding: { id: 'subagent/summary', version: 1 },
                configure: (context) => Effect.succeed(context),
              })
              const descriptor = Model.make({
                definition: {
                  ref: { provider: 'subagent', modelId: 'summary' },
                  maxOutputTokens: 9,
                  capabilities: {
                    tools: true,
                    images: false,
                    reasoning: false,
                    structuredOutput: false,
                  },
                },
                languageModel: provider,
                options: Schema.Struct({}),
                configure: () => Effect.succeed(Context.empty()),
                compaction: capability,
              })
              const runtime = yield* HarnessRuntime.make({ registry, models: [descriptor] }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(
                  Settings.Settings,
                  yield* Settings.make({
                    compaction: {
                      enabled: false,
                      reserveTokens: 20,
                      keepRecentTokens: 10,
                      backgroundTokens: 0,
                    },
                  }),
                ),
              )
              const parent = yield* runtime.scheduler.start(program, undefined, {
                conversationId: yield* runtime.backend.root,
              })
              const handle = yield* Effect.raceFirst(
                Deferred.await(ready),
                runtime.scheduler.wait(parent).pipe(Effect.flatMap(Effect.die)),
              )
              yield* Deferred.await(requested)
              const waiting = yield* CompactionTask.wait(handle).pipe(
                Effect.provideService(HarnessBackend, runtime.backend),
                Effect.exit,
                Effect.forkChild,
              )
              if (observerState === 'revoked') {
                yield* Deferred.succeed(finishCaller, undefined)
                assert.strictEqual((yield* Fiber.join(waiting))._tag, 'Failure')
                assert.notStrictEqual(
                  (yield* CompactionTask.read({ ...handle }).pipe(
                    Effect.provideService(HarnessBackend, runtime.backend),
                  )).state.status,
                  'terminal',
                )
              }
              yield* Deferred.succeed(releaseProvider, undefined)
              const outcome = yield* CompactionTask.wait({ ...handle }).pipe(
                Effect.provideService(HarnessBackend, runtime.backend),
              )
              if (outcome.status !== 'completed' || outcome.result._tag !== 'Queued')
                return yield* Effect.die(JSON.stringify(outcome))
              assert.strictEqual(
                (yield* runtime.backend.wait(outcome.result.submissionId)).status,
                'done',
              )
              assert.lengthOf(
                (yield* Session.scanEntries(runtime.session, {
                  conversationId: handle.conversationId,
                }).pipe(Stream.runCollect)).filter((entry) => entry.kind === 'compaction'),
                1,
              )
              assert.lengthOf(
                (yield* Session.commit(runtime.session, (tx) =>
                  Usage.receipts(tx, handle.conversationId),
                )).filter(
                  (receipt) =>
                    receipt.identity._tag === 'requestAttempt' &&
                    receipt.identity.purpose === 'compaction',
                ),
                1,
              )
              assert.strictEqual(requests, 1)
              assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
              if (observerState === 'current') {
                const observed = yield* Fiber.join(waiting)
                assert.strictEqual(observed._tag, 'Success')
                yield* Deferred.succeed(finishCaller, undefined)
              }
              assert.strictEqual((yield* runtime.scheduler.wait(parent)).status, 'completed')
            }),
          ).pipe(Effect.provide(services)),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const rejection of ['typed', 'defect', 'mixed', 'interruption'] as const)
      it.live(
        `${backend}: caught ${rejection} initializer leaves no admitted supervisor in the caller transaction`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const observed = yield* Deferred.make<number>()
              const contract = {
                kind: 'subagent.caught-initializer',
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                success: Schema.Void,
                failure: Schema.String,
              } as const
              const program = Task.make({
                ...contract,
                initial: () => ({ phase: 'run' as const }),
                phases: {
                  run: () =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      if (access.subagent === undefined)
                        return yield* Effect.die('Missing native subagent')
                      const subagent = access.subagent
                      const count = yield* access.commit((tx) =>
                        Effect.gen(function* () {
                          yield* Transaction.appendEntry(tx, access.conversationId, {
                            kind: 'caller.prior',
                          })
                          const result = yield* subagent
                            .admit(tx, {
                              draft: { type: 'write', entry: { kind: 'will-not-admit' } },
                              options: { key: 'rejected' },
                            })
                            .pipe(Effect.exit)
                          assert.strictEqual(result._tag, 'Failure')
                          assert.lengthOf(
                            yield* Transaction.scanConversations(tx).pipe(Stream.runCollect),
                            1,
                          )
                          assert.lengthOf(
                            yield* Transaction.scanSubmissions(tx).pipe(Stream.runCollect),
                            0,
                          )
                          assert.lengthOf(
                            yield* Transaction.scanDocuments(tx, {
                              scope: { _tag: 'task', taskId: access.taskId },
                              kind: 'harness.subagent.slots',
                            }).pipe(Stream.runCollect),
                            0,
                          )
                          assert.lengthOf(
                            yield* Transaction.scanEntries(tx, {
                              conversationId: access.conversationId,
                            }).pipe(Stream.runCollect),
                            1,
                          )
                          return (yield* Transaction.scanTasks(tx, {
                            kind: 'harness.subagent',
                          }).pipe(Stream.runCollect)).length
                        }),
                      )
                      yield* Deferred.succeed(observed, count)
                      return yield* Effect.never
                    }),
                },
              })
              const registry = yield* Registry.make
              yield* registry.install(
                Registry.withTask(
                  Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                  program,
                ),
              )
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const layer = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/content.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/content.sqlite` })),
                )
              })()
              const storage = yield* Layer.build(layer)
              const model = yield* LanguageModel.make({
                generateText: () => Effect.die('No provider'),
                streamText: () => Stream.die('No provider'),
              })
              const rejected = (() => {
                if (rejection === 'typed') return Effect.fail('initializer rejection')
                if (rejection === 'defect') return Effect.die('initializer defect')
                if (rejection === 'interruption') return Effect.interrupt
                return Effect.failCause(
                  Cause.fromReasons([
                    ...Cause.fail('initializer rejection').reasons,
                    ...Cause.die('initializer defect').reasons,
                  ]),
                )
              })()
              const runtime = yield* HarnessRuntime.make({
                registry,
                initializers: [
                  ConversationInitializer.make({
                    execute: (_tx, conversation) =>
                      conversation.owner === undefined ? Effect.void : rejected,
                  }),
                ],
              }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(LanguageModel.LanguageModel, model),
              )
              const root = yield* runtime.backend.root
              yield* runtime.scheduler.start(program, undefined, { conversationId: root })
              assert.strictEqual(yield* Deferred.await(observed), 0)
              assert.lengthOf(
                yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
                  Stream.runCollect,
                ),
                1,
              )
            }),
          ).pipe(Effect.provide(services)),
      )
})
