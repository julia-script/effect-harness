import * as ResultEntry from '../src/internal/ToolResultEntry.js'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Record from '../src/Record.js'
import * as Identity from '../src/Identity.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as StorageFormat from '../src/StorageFormat.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as ToolProgress from '../src/ToolProgress.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Usage from '../src/Usage.js'
import * as Round from '../src/internal/ToolRound.js'
import * as Settings from '../src/Settings.js'
import * as Hook from '../src/Hook.js'
import { HookExecution } from '../src/HookExecution.js'
import * as AgentState from '../src/internal/AgentState.js'
import * as Transaction from '../src/Transaction.js'
import * as Fiber from 'effect/Fiber'
import * as Cause from 'effect/Cause'
import * as Registry from '../src/Registry.js'
import * as ToolWrapper from '../src/ToolWrapper.js'
import { StorageError } from '../src/StorageError.js'
import * as ProgressInternal from '../src/internal/ToolProgress.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const calls = [
  { type: 'tool-call' as const, id: 'a', name: 'first', params: {}, providerExecuted: false },
  { type: 'tool-call' as const, id: 'b', name: 'second', params: {}, providerExecuted: false },
]

describe('durable native tool rounds', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: overlap, reverse completion, ordered controls and atomic final progress`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const storage = yield* Effect.gen(function* () {
              if (backend === 'memory') return yield* Layer.build(Storage.layerMemory)
              if (backend === 'sqlite')
                return yield* Layer.build(
                  Storage.layerSql.pipe(
                    Layer.provide(SqliteClient.layer({ filename: `${directory}/round.sqlite` })),
                  ),
                )
              return yield* Layer.build(
                Storage.layerJsonl({ filePath: `${directory}/round.jsonl` }),
              )
            })
            const actual = Context.get(storage, Storage.Storage)
            const format = Context.get(storage, StorageFormat.StorageFormat)
            const batches: ReadonlyArray<Record.StorageWrite>[] = []
            const captured: Storage.Storage['Service'] = {
              ...actual,
              commit: (input) => {
                const writes = [...input]
                return actual.commit(writes).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      batches.push(writes)
                    }),
                  ),
                )
              },
            }
            const context = storage.pipe(
              Context.add(Storage.Storage, captured),
              Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
            )
            const firstEntered = yield* Deferred.make<void>()
            const secondEntered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const childIds: Record.TaskId[] = []
            let requests = 0
            let late: ToolExecution['Service'] | undefined
            const toolkit = Toolkit.make(
              Tool.makeResult('first', {
                success: Schema.String,
                executionMode: 'parallel',
                replay: 'safe',
                outputRetention: { retain: 'tail', maxBytes: 12, maxLines: 2 },
              }),
              Tool.makeResult('second', {
                success: Schema.String,
                executionMode: 'parallel',
                replay: 'safe',
              }),
            )
            const model = yield* ScriptedStream.make({
              generateText: () => {
                requests++
                return Effect.succeed([...calls, { ...finish, reason: 'tool-calls' as const }])
              },
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  first: () =>
                    Effect.gen(function* () {
                      const access = yield* ToolExecution
                      late = access
                      childIds[0] = access.taskId
                      yield* access.output('α\nprefix\n')
                      yield* access.details({ phase: 'entered' })
                      yield* Deferred.succeed(firstEntered, undefined)
                      yield* Deferred.await(secondEntered)
                      yield* Deferred.await(release)
                      yield* access.output('last 😀\n')
                      return {
                        content: [Prompt.textPart({ text: 'first' })],
                        structuredOutput: 'private-a',
                        control: { terminate: true },
                      }
                    }),
                  second: () =>
                    Effect.gen(function* () {
                      const access = yield* ToolExecution
                      childIds[1] = access.taskId
                      yield* Deferred.succeed(secondEntered, undefined)
                      yield* Deferred.await(firstEntered)
                      return {
                        content: [Prompt.textPart({ text: 'second' })],
                        structuredOutput: 'private-b',
                        control: { terminate: true },
                      }
                    }),
                }),
              ),
            )
            const root = yield* runtime.backend.root
            const submission = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(firstEntered)
            yield* Deferred.await(secondEntered)
            const secondChild = childIds[1]!
            yield* runtime.scheduler.waitForTask(secondChild)
            const tasks = yield* Session.scanTasks(runtime.session, { conversationId: root }).pipe(
              Stream.runCollect,
            )
            const parent = tasks.find((task) => task.kind === 'harness.run')!
            assert.strictEqual(parent.state.status, 'waiting')
            assert.notInclude(childIds, parent.id)
            assert.strictEqual(new Set(childIds).size, 2)
            const wire = parent.state.checkpoint as Schema.JsonObject
            const round = yield* Session.commit(runtime.session, (tx) =>
              Round.receipt(tx, root, Schema.decodeUnknownSync(Schema.String)(wire.roundKey)),
            )
            assert.isUndefined(round.slots[0]?.result)
            assert.strictEqual(round.slots[1]?.receipt?.status, 'succeeded')
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
            assert.strictEqual(requests, 1)
            const progress = Option.getOrThrow(
              yield* Session.snapshot(
                runtime.session,
                ToolProgress.document,
                Round.target(root, Round.progressKey(childIds[0]!)),
              ),
            ).value
            assert.strictEqual(progress.status, 'completed')
            assert.strictEqual(progress.output.text, 'last 😀\n')
            assert.isTrue(progress.output.truncated)
            const finalRound = yield* Session.commit(runtime.session, (tx) =>
              Round.receipt(tx, root, Schema.decodeUnknownSync(Schema.String)(wire.roundKey)),
            )
            assert.isTrue(finalRound.applied)
            assert.deepStrictEqual(
              finalRound.slots.map((slot) => slot.call.id),
              ['a', 'b'],
            )
            const receipts = yield* Session.commit(runtime.session, (tx) =>
              Usage.receipts(tx, root),
            )
            assert.strictEqual(
              receipts.filter((receipt) => receipt.identity._tag === 'toolCall').length,
              2,
            )
            const finalBatch = batches.find((batch) =>
              batch.some(
                (write) =>
                  write._tag === 'entry' && write.value.id === finalRound.slots[0]?.entryId,
              ),
            )!
            assert.isDefined(finalBatch)
            assert.isTrue(
              finalBatch.some(
                (write) =>
                  write._tag === 'task' &&
                  write.value.id === childIds[0] &&
                  write.value.state.status === 'terminal' &&
                  Schema.is(Schema.Struct({ status: Schema.tag('completed') }))(
                    write.value.state.outcome,
                  ),
              ),
            )
            assert.isAtLeast(
              finalBatch.filter(
                (write) => write._tag === 'document.change' || write._tag === 'document.create',
              ).length,
              3,
            )
            const rejected = yield* late!.output('late').pipe(Effect.flip)
            assert.strictEqual(rejected._tag, 'ExecutionError')
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(context)))
                .minimumReaderVersion,
              12,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  for (const reason of ['global', 'offered', 'called'] as const)
    it.live(
      `${reason}: the entire round is sequential, including an uncalled offered constraint`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            let secondRuns = 0
            const toolkit = Toolkit.make(
              Tool.makeResult('first', {
                success: Schema.String,
                executionMode: reason === 'called' ? 'sequential' : 'parallel',
              }),
              Tool.makeResult('second', { success: Schema.String, executionMode: 'parallel' }),
              Tool.make('unused', {
                success: Schema.String,
                executionMode: reason === 'offered' ? 'sequential' : 'parallel',
              }),
            )
            const model = yield* ScriptedStream.make({
              generateText: () =>
                Effect.succeed([...calls, { ...finish, reason: 'tool-calls' as const }]),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                Layer.merge(
                  toolkit.toLayer({
                    first: () =>
                      Effect.gen(function* () {
                        yield* Deferred.succeed(entered, undefined)
                        yield* Deferred.await(release)
                        return {
                          content: [],
                          structuredOutput: 'first',
                          control: { terminate: true },
                        }
                      }),
                    second: () =>
                      Effect.sync(() => {
                        secondRuns++
                        return {
                          content: [],
                          structuredOutput: 'second',
                          control: { terminate: true },
                        }
                      }),
                    unused: () => Effect.die('Uncalled tool was invoked'),
                  }),
                  Settings.layer({
                    toolExecution: reason === 'global' ? 'sequential' : 'parallel',
                  }),
                ),
              ),
            )
            const root = yield* runtime.backend.root
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            const records = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
            assert.strictEqual(records.filter((record) => record.kind === 'harness.tool').length, 1)
            assert.strictEqual(secondRuns, 0)
            const parent = records.find((record) => record.kind === 'harness.run')!
            const round = yield* Session.commit(runtime.session, (tx) =>
              Round.receipt(
                tx,
                root,
                Schema.decodeUnknownSync(Schema.Struct({ roundKey: Schema.String }))(
                  parent.state.checkpoint,
                ).roundKey,
              ),
            )
            assert.strictEqual(round.mode, 'sequential')
            assert.isUndefined(round.slots[1]?.taskId)
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
            assert.strictEqual(secondRuns, 1)
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )

  it.live(
    'sequential unadmitted tool cannot acquire an uninstalled binding or terminate the round',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let secondRuns = 0
          let requests = 0
          const first = Toolkit.make(Tool.makeResult('first', { success: Schema.String }))
          const second = Toolkit.make(Tool.make('second', { success: Schema.String }))
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'first', version: 1 }, first),
              first.toLayer({
                first: () =>
                  Effect.gen(function* () {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                    return { content: [], structuredOutput: 'first', control: { terminate: true } }
                  }),
              }),
            ),
          )
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'second', version: 1 }, second),
              second.toLayer({
                second: () =>
                  Effect.sync(() => {
                    secondRuns++
                    return 'unexpected'
                  }),
              }),
            ),
          )
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.succeed(
                ++requests === 1
                  ? [...calls, { ...finish, reason: 'tool-calls' as const }]
                  : [{ type: 'text' as const, text: 'done' }, finish],
              ),
          })
          const runtime = yield* HarnessRuntime.make({ registry }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          yield* Deferred.await(entered)
          const parent = (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)).find(
            (task) => task.kind === 'harness.run',
          )!
          yield* registry.uninstall('second')
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.strictEqual(secondRuns, 0)
          assert.strictEqual(requests, 2)
          const round = yield* Session.commit(runtime.session, (tx) =>
            Round.receipt(
              tx,
              root,
              Schema.decodeUnknownSync(Schema.Struct({ roundKey: Schema.String }))(
                parent.state.checkpoint,
              ).roundKey,
            ),
          )
          assert.strictEqual(round.slots[1]?.receipt?.status, 'denied')
          assert.isTrue(round.slots[1]?.result?.isError)
          const child = Option.getOrThrow(
            yield* Session.task(runtime.session, round.slots[1]!.taskId!),
          )
          assert.strictEqual(child.kind, 'harness.tool')
          assert.strictEqual(child.owner, parent.id)
          const receipt = (yield* Session.commit(runtime.session, (tx) =>
            Usage.receipts(tx, root),
          )).find(
            (receipt) =>
              receipt.identity._tag === 'toolCall' && receipt.identity.taskId === child.id,
          )!
          assert.isUndefined(receipt.usage)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live('a failed child does not cancel its sibling and vetoes termination', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let requests = 0
        const toolkit = Toolkit.make(
          Tool.makeResult('first', {
            success: Schema.String,
            failure: Schema.String,
            executionMode: 'parallel',
          }),
          Tool.makeResult('second', { success: Schema.String, executionMode: 'parallel' }),
        )
        const model = yield* ScriptedStream.make({
          generateText: () =>
            Effect.succeed(
              ++requests === 1
                ? [...calls, { ...finish, reason: 'tool-calls' as const }]
                : [{ type: 'text' as const, text: 'done' }, finish],
            ),
        })
        const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(
            toolkit.toLayer({
              first: () =>
                Effect.fail({
                  content: [Prompt.textPart({ text: 'ordinary failure' })],
                  isError: true,
                  structuredOutput: 'ordinary failure',
                }),
              second: () =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  return { content: [], structuredOutput: 'ok', control: { terminate: true } }
                }),
            }),
          ),
        )
        const root = yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        yield* Deferred.await(entered)
        const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
        const first = tasks.find(
          (task) => task.kind === 'harness.tool' && (task.input as Schema.JsonObject).index === 0,
        )!
        yield* runtime.scheduler.waitForTask(first.id)
        assert.strictEqual(requests, 1)
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        assert.strictEqual(requests, 2)
        const parent = tasks.find((task) => task.kind === 'harness.run')!
        const round = yield* Session.commit(runtime.session, (tx) =>
          Round.receipt(
            tx,
            root,
            Schema.decodeUnknownSync(Schema.Struct({ roundKey: Schema.String }))(
              parent.state.checkpoint,
            ).roundKey,
          ),
        )
        assert.deepStrictEqual(
          round.slots.map((slot) => slot.receipt?.status),
          ['failed', 'succeeded'],
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'sequential abort flushes started progress and cancels unadmitted slots without handler or spend',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          let secondRuns = 0
          let late: ToolExecution['Service'] | undefined
          const toolkit = Toolkit.make(
            Tool.make('first', { success: Schema.String }),
            Tool.make('second', { success: Schema.String }),
          )
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.succeed([...calls, { ...finish, reason: 'tool-calls' as const }]),
          })
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provide(
              toolkit.toLayer({
                first: () =>
                  Effect.gen(function* () {
                    late = yield* ToolExecution
                    yield* late.output('cancel tail 😀')
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }),
                second: () =>
                  Effect.sync(() => {
                    secondRuns++
                    return 'unexpected'
                  }),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          yield* Deferred.await(entered)
          const parent = (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)).find(
            (task) => task.kind === 'harness.run',
          )!
          yield* runtime.backend.abort(root)
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
          assert.strictEqual(secondRuns, 0)
          const round = yield* Session.commit(runtime.session, (tx) =>
            Round.receipt(
              tx,
              root,
              Schema.decodeUnknownSync(Schema.Struct({ roundKey: Schema.String }))(
                parent.state.checkpoint,
              ).roundKey,
            ),
          )
          assert.isTrue(round.applied)
          assert.deepStrictEqual(
            round.slots.map((slot) => slot.receipt?.status),
            ['cancelled', 'cancelled'],
          )
          assert.isUndefined(round.slots[1]?.taskId)
          const progress = Option.getOrThrow(
            yield* Session.snapshot(
              runtime.session,
              ToolProgress.document,
              Round.target(root, Round.progressKey(late!.taskId)),
            ),
          ).value
          assert.strictEqual(progress.status, 'cancelled')
          assert.strictEqual(progress.output.text, 'cancel tail 😀')
          const receipts = (yield* Session.commit(runtime.session, (tx) =>
            Usage.receipts(tx, root),
          )).filter((receipt) => receipt.identity._tag === 'toolCall')
          assert.strictEqual(receipts.length, 1)
          assert.isUndefined(receipts[0]?.usage)
          assert.deepInclude(yield* late!.commit(() => Effect.void).pipe(Effect.flip), {
            _tag: 'ExecutionError',
            reason: 'revoked',
          })
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const replay of ['safe', 'unsafe'] as const)
      it.live(
        `${backend}/${replay}: cold nonprefix recovery keeps completed work and accepted arguments`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const memory = yield* Layer.build(Storage.layerMemory)
              const open = (scope: Scope.Scope) =>
                Effect.gen(function* () {
                  if (backend === 'memory') return memory
                  if (backend === 'sqlite')
                    return yield* Layer.buildWithScope(
                      Storage.layerSql.pipe(
                        Layer.provide(SqliteClient.layer({ filename: `${directory}/cold.sqlite` })),
                      ),
                      scope,
                    )
                  return yield* Layer.buildWithScope(
                    Storage.layerJsonl({ filePath: `${directory}/cold.jsonl` }),
                    scope,
                  )
                })
              const entered = yield* Deferred.make<void>()
              let firstRuns = 0
              let secondRuns = 0
              let transforms = 0
              let requests = 0
              let late: ToolExecution['Service'] | undefined
              const toolkit = Toolkit.make(
                Tool.makeResult('first', {
                  parameters: Schema.Struct({ value: Schema.Finite }),
                  success: Schema.String,
                  replay,
                  executionMode: 'parallel',
                }),
                Tool.makeResult('second', {
                  success: Schema.String,
                  replay: 'safe',
                  executionMode: 'parallel',
                }),
              )
              const model = yield* ScriptedStream.make({
                generateText: () =>
                  Effect.succeed(
                    ++requests === 1
                      ? [
                          { ...calls[0]!, params: { value: 1 } },
                          calls[1]!,
                          { ...finish, reason: 'tool-calls' as const },
                        ]
                      : [{ type: 'text' as const, text: 'done' }, finish],
                  ),
              })
              const hook = Hook.make({
                event: 'beforeTool',
                execute: ({ call }) =>
                  Effect.sync(() => {
                    if (call.name !== 'first') return
                    transforms++
                    return {
                      _tag: 'replaceArguments' as const,
                      arguments: { value: Number(call.arguments.value) + 1 },
                    }
                  }),
              })
              const firstScope = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
              const firstContext = yield* open(firstScope)
              const first = yield* HarnessRuntime.make({ tools: toolkit, hooks: [hook] }).pipe(
                Effect.provideContext(firstContext),
                Effect.provideService(Scope.Scope, firstScope),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provide(
                  toolkit.toLayer({
                    first: ({ value }) =>
                      Effect.gen(function* () {
                        assert.strictEqual(value, 2)
                        firstRuns++
                        late = yield* ToolExecution
                        yield* late.output('prior output')
                        yield* Deferred.succeed(entered, undefined)
                        return yield* Effect.never
                      }),
                    second: () =>
                      Effect.sync(() => {
                        secondRuns++
                        return { content: [], structuredOutput: 'second' }
                      }),
                  }),
                ),
              )
              const root = yield* first.backend.root
              const job = yield* first.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'go' },
              })
              yield* Deferred.await(entered)
              const children = (yield* Session.scanTasks(first.session).pipe(
                Stream.runCollect,
              )).filter((task) => task.kind === 'harness.tool')
              const secondChild = children.find(
                (task) => (task.input as Schema.JsonObject).index === 1,
              )!
              yield* first.scheduler.waitForTask(secondChild.id)
              yield* Scope.close(firstScope, Exit.void)
              const secondContext = yield* open(yield* Effect.scope)
              const second = yield* HarnessRuntime.make({ tools: toolkit, hooks: [hook] }).pipe(
                Effect.provideContext(secondContext),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provide(
                  toolkit.toLayer({
                    first: ({ value }) =>
                      Effect.sync(() => {
                        assert.strictEqual(value, 2)
                        firstRuns++
                        return { content: [], structuredOutput: 'recovered' }
                      }),
                    second: () =>
                      Effect.sync(() => {
                        secondRuns++
                        return { content: [], structuredOutput: 'repeated' }
                      }),
                  }),
                ),
              )
              yield* second.backend.root
              assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
              assert.strictEqual(firstRuns, replay === 'safe' ? 2 : 1)
              assert.strictEqual(secondRuns, 1)
              assert.strictEqual(transforms, 1)
              assert.strictEqual(requests, 2)
              assert.deepInclude(yield* late!.output('late').pipe(Effect.flip), {
                _tag: 'ExecutionError',
                reason: 'revoked',
              })
              const receipts = (yield* Session.commit(second.session, (tx) =>
                Usage.receipts(tx, root),
              )).filter((receipt) => receipt.identity._tag === 'toolCall')
              assert.strictEqual(receipts.length, replay === 'safe' ? 3 : 2)
              const progress = Option.getOrThrow(
                yield* Session.snapshot(
                  second.session,
                  ToolProgress.document,
                  Round.target(root, Round.progressKey(late!.taskId)),
                ),
              ).value
              assert.strictEqual(progress.attempt, replay === 'safe' ? 1 : 0)
              assert.strictEqual(progress.status, replay === 'safe' ? 'completed' : 'interrupted')
              if (replay === 'unsafe') assert.strictEqual(progress.output.text, 'prior output')
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )

  it.live('original order owns last handoff and tool additions despite reverse completion', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const releaseFirst = yield* Deferred.make<void>()
        const secondDone = yield* Deferred.make<void>()
        const toolkit = Toolkit.make(
          Tool.makeResult('first', { success: Schema.String, executionMode: 'parallel' }),
          Tool.makeResult('second', { success: Schema.String, executionMode: 'parallel' }),
          Tool.make('lookup', { success: Schema.String, executionMode: 'parallel' }),
          Tool.make('inspect', { success: Schema.String, executionMode: 'parallel' }),
        )
        let requests = 0
        const inventories: string[][] = []
        const model = yield* ScriptedStream.make({
          generateText: ({ tools }) => {
            inventories.push(tools.map((tool) => tool.name))
            return Effect.succeed(
              ++requests === 1
                ? [...calls, { ...finish, reason: 'tool-calls' as const }]
                : [{ type: 'text' as const, text: 'next' }, finish],
            )
          },
        })
        const runtime = yield* HarnessRuntime.make({
          tools: toolkit,
          agent: { modelTools: { mode: 'explicit', names: ['first', 'second'] } },
        }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(
            toolkit.toLayer({
              first: () =>
                Effect.gen(function* () {
                  yield* Deferred.await(releaseFirst)
                  return {
                    content: [],
                    structuredOutput: 'first',
                    control: { handoff: 'first handoff', addTools: ['lookup', 'inspect'] },
                  }
                }),
              second: () =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(secondDone, undefined)
                  return {
                    content: [],
                    structuredOutput: 'second',
                    control: { handoff: 'last handoff', addTools: ['inspect', 'lookup'] },
                  }
                }),
              lookup: () => Effect.succeed('lookup'),
              inspect: () => Effect.succeed('inspect'),
            }),
          ),
        )
        const root = yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        yield* Deferred.await(secondDone)
        const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
        const second = tasks.find(
          (task) => task.kind === 'harness.tool' && (task.input as Schema.JsonObject).index === 1,
        )!
        yield* runtime.scheduler.waitForTask(second.id)
        assert.strictEqual(requests, 1)
        yield* Deferred.succeed(releaseFirst, undefined)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        const entries = yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
          Stream.runCollect,
        )
        const resets = entries.filter((entry) => entry.kind === 'reset')
        assert.strictEqual(resets.length, 1)
        assert.strictEqual(resets[0]?.head, resets[0]?.id)
        const resetMessages = yield* Schema.decodeUnknownEffect(
          Schema.Array(Schema.toCodecJson(Prompt.Message)),
        )(resets[0]?.model)
        assert.deepStrictEqual(resetMessages, [
          Prompt.userMessage({ content: [Prompt.textPart({ text: 'last handoff' })] }),
        ])
        assert.deepStrictEqual(
          Option.getOrThrow(
            yield* Session.snapshot(runtime.session, AgentState.document, AgentState.target(root)),
          ).value.modelTools?.names,
          ['first', 'second', 'lookup', 'inspect'],
        )
        const next = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'next' },
        })
        yield* runtime.backend.wait(next.id)
        assert.deepStrictEqual(inventories, [
          ['first', 'second'],
          ['first', 'second', 'lookup', 'inspect'],
        ])
        assert.strictEqual(
          (yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
            Stream.runCollect,
          )).filter((entry) => entry.kind === 'reset').length,
          1,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'repeated provider call IDs in distinct rounds have distinct children, progress and accounting',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let requests = 0
          const ids: Record.TaskId[] = []
          const toolkit = Toolkit.make(
            Tool.makeResult('first', { success: Schema.String, executionMode: 'parallel' }),
          )
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.succeed(
                ++requests <= 2
                  ? [calls[0]!, { ...finish, reason: 'tool-calls' as const }]
                  : [{ type: 'text' as const, text: 'done' }, finish],
              ),
          })
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provide(
              toolkit.toLayer({
                first: () =>
                  Effect.gen(function* () {
                    const access = yield* ToolExecution
                    ids.push(access.taskId)
                    yield* access.output(`round ${ids.length}`)
                    return {
                      content: [],
                      structuredOutput: String(ids.length),
                      spend: { amount: 2, currency: 'USD', source: 'observed' },
                    }
                  }),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.strictEqual(new Set(ids).size, 2)
          const receipts = (yield* Session.commit(runtime.session, (tx) =>
            Usage.receipts(tx, root),
          )).filter((receipt) => receipt.identity._tag === 'toolCall')
          assert.strictEqual(receipts.length, 2)
          assert.deepStrictEqual(
            receipts.map((receipt) =>
              receipt.identity._tag === 'toolCall' ? receipt.identity.callId : undefined,
            ),
            ['a', 'a'],
          )
          for (const [index, id] of ids.entries())
            assert.strictEqual(
              Option.getOrThrow(
                yield* Session.snapshot(
                  runtime.session,
                  ToolProgress.document,
                  Round.target(root, Round.progressKey(id)),
                ),
              ).value.output.text,
              `round ${index + 1}`,
            )
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root,
          }).pipe(Stream.runCollect)
          assert.strictEqual(
            Usage.aggregate(entries, receipts).costs.find((cost) => cost.currency === 'USD')?.tool,
            4,
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live('each hook callback and handler has its own access epoch', () =>
    Effect.scoped(
      Effect.gen(function* () {
        let beforeAccess: HookExecution['Service'] | undefined
        let handlerAccess: ToolExecution['Service'] | undefined
        const toolkit = Toolkit.make(Tool.makeResult('first', { success: Schema.String }))
        const model = yield* ScriptedStream.make({
          generateText: () =>
            Effect.succeed([calls[0]!, { ...finish, reason: 'tool-calls' as const }]),
        })
        const runtime = yield* HarnessRuntime.make({
          tools: toolkit,
          hooks: [
            Hook.make({
              event: 'beforeTool',
              execute: () =>
                Effect.gen(function* () {
                  beforeAccess = yield* HookExecution
                }),
            }),
            Hook.make({
              event: 'beforeTool',
              execute: () =>
                Effect.gen(function* () {
                  assert.deepInclude(
                    yield* beforeAccess!.commit(() => Effect.void).pipe(Effect.flip),
                    { reason: 'revoked' },
                  )
                }),
            }),
            Hook.make({
              event: 'afterTool',
              execute: ({ result }) =>
                Effect.gen(function* () {
                  assert.deepInclude(
                    yield* beforeAccess!.commit(() => Effect.void).pipe(Effect.flip),
                    { reason: 'revoked' },
                  )
                  assert.deepInclude(
                    yield* handlerAccess!.commit(() => Effect.void).pipe(Effect.flip),
                    { reason: 'revoked' },
                  )
                  assert.deepInclude(yield* handlerAccess!.output('late').pipe(Effect.flip), {
                    reason: 'revoked',
                  })
                  return { ...result, control: { terminate: true } }
                }),
            }),
          ],
        }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(
            toolkit.toLayer({
              first: () =>
                Effect.gen(function* () {
                  handlerAccess = yield* ToolExecution
                  return { content: [], structuredOutput: 'ok' }
                }),
            }),
          ),
        )
        const root = yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'go' },
        })
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const operation of ['abort', 'cold'] as const)
    it.live(
      `post-handler ${operation} preserves reported spend without repeating unsafe work`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* Layer.build(Storage.layerMemory)
            let executions = 0
            let requests = 0
            const entered = yield* Deferred.make<void>()
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const toolkit = Toolkit.make(
              Tool.makeResult('first', { success: Schema.String, replay: 'unsafe' }),
            )
            const handler = toolkit.toLayer({
              first: () =>
                Effect.sync(() => {
                  executions++
                  return {
                    content: [],
                    structuredOutput: 'known',
                    spend: { amount: 7, currency: 'USD', source: 'handler' },
                  }
                }),
            })
            const model = yield* ScriptedStream.make({
              generateText: () =>
                Effect.succeed(
                  ++requests === 1
                    ? [calls[0]!, { ...finish, reason: 'tool-calls' as const }]
                    : [{ type: 'text' as const, text: 'done' }, finish],
                ),
            })
            const first = yield* HarnessRuntime.make({
              tools: toolkit,
              hooks: [
                Hook.make({
                  event: 'afterTool',
                  execute: () =>
                    Effect.gen(function* () {
                      yield* Deferred.succeed(entered, undefined)
                      return yield* Effect.never
                    }),
                }),
              ],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Scope.Scope, firstScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(handler),
            )
            const root = yield* first.backend.root
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            let session = first.session
            if (operation === 'abort') {
              yield* first.backend.abort(root)
              assert.strictEqual((yield* first.backend.wait(job.id)).status, 'unanswered')
            } else {
              yield* Scope.close(firstScope, Exit.void)
              const second = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provide(handler),
              )
              yield* second.backend.root
              assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
              session = second.session
            }
            assert.strictEqual(executions, 1)
            const receipts = (yield* Session.commit(session, (tx) =>
              Usage.receipts(tx, root),
            )).filter((receipt) => receipt.identity._tag === 'toolCall')
            assert.strictEqual(receipts.length, 1)
            assert.deepStrictEqual(receipts[0]?.usage, {
              _tag: 'tool',
              name: 'first',
              cost: { amount: 7, currency: 'USD', source: 'handler' },
            })
            if (operation === 'abort') assert.isUndefined(receipts[0]?.linkedEntryId)
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: cancellation freezes accepted throttled UTF8/details/diagnostics without acknowledging a pending reporter`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const storage = yield* Effect.gen(function* () {
              if (backend === 'memory') return yield* Layer.build(Storage.layerMemory)
              if (backend === 'sqlite')
                return yield* Layer.build(
                  Storage.layerSql.pipe(
                    Layer.provide(SqliteClient.layer({ filename: `${directory}/cancel.sqlite` })),
                  ),
                )
              return yield* Layer.build(
                Storage.layerJsonl({ filePath: `${directory}/cancel.jsonl` }),
              )
            })
            const entered = yield* Deferred.make<void>()
            const pending =
              yield* Deferred.make<Fiber.Fiber<void, import('../src/ExecutionError.js').Failure>>()
            let childId: Record.TaskId | undefined
            const toolkit = Toolkit.make(
              Tool.make('first', {
                success: Schema.String,
                outputRetention: { retain: 'tail', maxBytes: 12, maxLines: 2 },
              }),
            )
            const model = yield* ScriptedStream.make({
              generateText: () =>
                Effect.succeed([calls[0]!, { ...finish, reason: 'tool-calls' as const }]),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                Layer.merge(
                  toolkit.toLayer({
                    first: () =>
                      Effect.gen(function* () {
                        const access = yield* ToolExecution
                        childId = access.taskId
                        yield* access.details({ phase: 'committed' })
                        for (let index = 0; index < 200; index++) yield* access.output('flood\n')
                        yield* access.output('😀')
                        yield* access.diagnostic({
                          severity: 'warning',
                          message: 'accepted warning',
                        })
                        const report = yield* access
                          .details({ phase: 'pending' })
                          .pipe(Effect.forkChild({ startImmediately: true }))
                        yield* Deferred.succeed(pending, report)
                        yield* Deferred.succeed(entered, undefined)
                        return yield* Effect.never
                      }),
                  }),
                  Settings.layer({ progress: { outputIntervalMs: 100000 } }),
                ),
              ),
            )
            const root = yield* runtime.backend.root
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered)
            assert.isUndefined((yield* Deferred.await(pending)).pollUnsafe())
            yield* runtime.backend.abort(root)
            assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
            assert.isTrue(Exit.isFailure(yield* Fiber.await(yield* Deferred.await(pending))))
            const value = Option.getOrThrow(
              yield* Session.snapshot(
                runtime.session,
                ToolProgress.document,
                Round.target(root, Round.progressKey(childId!)),
              ),
            ).value
            assert.strictEqual(value.status, 'cancelled')
            assert.strictEqual(value.output.text, 'flood\n😀')
            assert.strictEqual(value.output.totalBytes, 1204)
            assert.strictEqual(value.output.totalLines, 201)
            assert.deepStrictEqual(value.details, { phase: 'pending' })
            assert.deepStrictEqual(value.diagnostics, [
              { severity: 'warning', message: 'accepted warning' },
            ])
            assert.isTrue(value.output.truncated)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  it.live(
    'outer codecs, original handlers and hook services retain separate same-tag captures and fresh scopes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          class Codec extends Context.Service<Codec, string>()('test/tool-round/Codec') {}
          const checks: string[] = []
          const scopes: Scope.Scope[] = []
          const checked = (label: string) =>
            Schema.String.pipe(
              Schema.middlewareDecoding((effect) =>
                Effect.gen(function* () {
                  assert.strictEqual(yield* Codec, label)
                  checks.push(label)
                  return yield* effect
                }),
              ),
              Schema.middlewareEncoding((effect) =>
                Effect.gen(function* () {
                  assert.strictEqual(yield* Codec, label)
                  return yield* effect
                }),
              ),
            )
          const registry = yield* Registry.make
          const original = Toolkit.make(
            Tool.makeResult('first', {
              parameters: Schema.Struct({ payload: checked('original') }),
              success: checked('original'),
              resultValidation: 'strict',
            }).addDependency(Codec),
          )
          yield* registry.install(
            Registry.provide(
              Registry.toolkit({ id: 'owner', version: 1 }, original),
              original
                .toLayer({
                  first: ({ payload }) =>
                    Effect.gen(function* () {
                      assert.strictEqual(yield* Codec, 'original')
                      scopes.push(yield* Scope.Scope)
                      return {
                        content: [Prompt.textPart({ text: 'visible' })],
                        structuredOutput: payload,
                      }
                    }),
                })
                .pipe(Layer.provideMerge(Layer.succeed(Codec, 'original'))),
            ),
          )
          const outer = Tool.makeResult('first', {
            parameters: Schema.Struct({ count: Schema.Int }),
            success: checked('outer'),
            resultValidation: 'strict',
          })
          yield* registry.install(
            Registry.toolWrapper(
              { id: 'wrapper', version: 1 },
              ToolWrapper.bind(outer, (item) =>
                Effect.succeed({
                  ...item,
                  invoke: (call) =>
                    Effect.gen(function* () {
                      assert.strictEqual(yield* Codec, 'outer')
                      return yield* item.invoke({
                        ...call,
                        arguments: {
                          payload: String(
                            yield* Schema.decodeUnknownEffect(Schema.Int)(call.arguments.count),
                          ),
                        },
                      })
                    }),
                }),
              ).pipe(ToolWrapper.provide(Layer.succeed(Codec, 'outer'))),
            ),
          )
          yield* registry.install(
            Registry.provide(
              Registry.hook(
                { id: 'hook', version: 1 },
                Hook.make({
                  event: 'beforeTool',
                  execute: () =>
                    Effect.gen(function* () {
                      assert.strictEqual(yield* Codec, 'hook')
                      scopes.push(yield* Scope.Scope)
                      return { _tag: 'replaceArguments' as const, arguments: { count: 2 } }
                    }),
                }),
              ),
              Layer.succeed(Codec, 'hook'),
            ),
          )
          yield* registry.install(
            Registry.provide(
              Registry.hook(
                { id: 'after', version: 1 },
                Hook.make({
                  event: 'afterTool',
                  execute: ({ result }) =>
                    Effect.gen(function* () {
                      assert.strictEqual(yield* Codec, 'after')
                      scopes.push(yield* Scope.Scope)
                      return {
                        ...result,
                        structuredOutput: 'after:2',
                        control: { terminate: true },
                      }
                    }),
                }),
              ),
              Layer.succeed(Codec, 'after'),
            ),
          )
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.succeed([
                { ...calls[0]!, params: { count: 1 } },
                { ...finish, reason: 'tool-calls' as const },
              ]),
          })
          const runtime = yield* HarnessRuntime.make({ registry }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideService(Codec, 'ambient'),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.include(checks, 'outer')
          assert.include(checks, 'original')
          assert.strictEqual(scopes.length, 3)
          assert.strictEqual(new Set(scopes).size, 3)
          const entry = (yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
            Stream.runCollect,
          )).find((entry) => entry.kind === 'tool.result')!
          assert.strictEqual((yield* ResultEntry.read(entry)).structuredOutput, 'after:2')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const boundary of ['result', 'controls', 'cancel'] as const)
      for (const uncertain of [false, true])
        it.live(
          `${backend}: native ${boundary} batch ${uncertain ? 'uncertain acknowledgement' : 'rollback'} preserves honest checkpoint recovery and once-only result`,
          () =>
            Effect.scoped(
              Effect.gen(function* () {
                const fs = yield* FileSystem.FileSystem
                const directory = yield* fs.makeTempDirectoryScoped()
                const storage = yield* Effect.gen(function* () {
                  if (backend === 'memory') return yield* Layer.build(Storage.layerMemory)
                  if (backend === 'sqlite')
                    return yield* Layer.build(
                      Storage.layerSql.pipe(
                        Layer.provide(
                          SqliteClient.layer({ filename: `${directory}/fault.sqlite` }),
                        ),
                      ),
                    )
                  return yield* Layer.build(
                    Storage.layerJsonl({ filePath: `${directory}/fault.jsonl` }),
                  )
                })
                const actual = Context.get(storage, Storage.Storage)
                const format = Context.get(storage, StorageFormat.StorageFormat)
                let injected = false
                const broken: Storage.Storage['Service'] = {
                  ...actual,
                  commit: (writes) => {
                    const batch = [...writes]
                    if (
                      injected ||
                      !batch.some((write) => {
                        if (boundary === 'cancel')
                          return (
                            write._tag === 'task' &&
                            write.value.kind === 'harness.tool' &&
                            write.value.state.status === 'terminal' &&
                            Schema.is(Schema.Struct({ status: Schema.tag('aborted') }))(
                              write.value.state.outcome,
                            )
                          )
                        if (boundary === 'result')
                          return write._tag === 'entry' && write.value.kind === 'tool.result'
                        return (
                          write._tag === 'document.change' &&
                          write.content._tag === 'base' &&
                          Schema.is(
                            Schema.Struct({
                              round: Schema.Struct({ applied: Schema.Literal(true) }),
                            }),
                          )(write.content.value)
                        )
                      })
                    )
                      return actual.commit(batch)
                    injected = true
                    const failure = Effect.fail(
                      new StorageError({
                        reason: uncertain ? 'uncertain' : 'io',
                        operation: 'test.final',
                        message: 'injected final batch',
                      }),
                    )
                    return uncertain ? actual.commit(batch).pipe(Effect.andThen(failure)) : failure
                  },
                }
                const brokenContext = storage.pipe(
                  Context.add(Storage.Storage, broken),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: broken }),
                )
                const toolkit = Toolkit.make(
                  Tool.makeResult('first', { success: Schema.String, replay: 'unsafe' }),
                )
                let handlers = 0
                let requests = 0
                const entered = yield* Deferred.make<void>()
                const handler = toolkit.toLayer({
                  first: () =>
                    Effect.sync(() => {
                      handlers++
                      return {
                        content: [],
                        structuredOutput: 'observed',
                        spend: { amount: 2, currency: 'USD', source: 'observed' },
                        control: { terminate: true },
                      }
                    }),
                })
                const model = yield* ScriptedStream.make({
                  generateText: () => {
                    requests++
                    return Effect.succeed([calls[0]!, { ...finish, reason: 'tool-calls' as const }])
                  },
                })
                const firstScope = yield* Scope.make()
                yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
                const first = yield* HarnessRuntime.make({
                  tools: toolkit,
                  hooks:
                    boundary === 'cancel'
                      ? [
                          Hook.make({
                            event: 'afterTool',
                            execute: () =>
                              Deferred.succeed(entered, undefined).pipe(
                                Effect.andThen(Effect.never),
                              ),
                          }),
                        ]
                      : [],
                }).pipe(
                  Effect.provideContext(brokenContext),
                  Effect.provideService(Scope.Scope, firstScope),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provide(handler),
                )
                const root = yield* first.backend.root
                const job = yield* first.backend.submit({
                  conversationId: root,
                  draft: { type: 'input', content: 'go' },
                })
                if (boundary === 'cancel') {
                  yield* Deferred.await(entered)
                  yield* first.backend.abort(root).pipe(Effect.result)
                }
                assert.strictEqual(
                  boundary === 'controls' && !uncertain
                    ? (yield* first.backend.wait(job.id)).status
                    : (yield* first.backend.wait(job.id).pipe(Effect.flip))._tag,
                  boundary === 'controls' && !uncertain ? 'unanswered' : 'HarnessError',
                )
                const entries = yield* actual
                  .scanEntries({
                    conversationId: root,
                  })
                  .pipe(Stream.runCollect)
                assert.strictEqual(
                  entries.filter((entry) => entry.kind === 'tool.result').length,
                  Number(boundary !== 'cancel' && (boundary === 'controls' || uncertain)),
                )
                const tasks = yield* actual.scanTasks({}).pipe(Stream.runCollect)
                const child = tasks.find((task) => task.kind === 'harness.tool')!
                assert.strictEqual(
                  child.state.status,
                  boundary === 'controls' || uncertain ? 'terminal' : 'running',
                )
                yield* Scope.close(firstScope, Exit.void)
                const second = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
                  Effect.provideContext(storage),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provide(handler),
                )
                yield* second.backend.root
                assert.strictEqual(
                  (yield* second.backend.wait(job.id)).status,
                  boundary === 'cancel' || (boundary === 'controls' && !uncertain)
                    ? 'unanswered'
                    : 'done',
                )
                assert.strictEqual(handlers, 1)
                assert.strictEqual(requests, 1)
                assert.strictEqual(
                  (yield* Session.scanEntries(second.session, { conversationId: root }).pipe(
                    Stream.runCollect,
                  )).filter((entry) => entry.kind === 'tool.result').length,
                  Number(boundary !== 'cancel'),
                )
                assert.strictEqual(
                  (yield* Session.usage(second.session)).costs.find(
                    (cost) => cost.currency === 'USD',
                  )?.tool,
                  2,
                )
              }),
            ).pipe(Effect.provide(NodeServices.layer)),
        )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['parallel', 'sequential'] as const)
      for (const fault of ['none', 'rollback', 'uncertain'] as const)
        it.live(
          `${backend}: ${mode} fatal codec joins success, preserves spend, and ${fault} settlement cold recovery`,
          () =>
            Effect.scoped(
              Effect.gen(function* () {
                const fs = yield* FileSystem.FileSystem
                const directory = yield* fs.makeTempDirectoryScoped()
                const storage = yield* Effect.gen(function* () {
                  if (backend === 'memory') return yield* Layer.build(Storage.layerMemory)
                  if (backend === 'sqlite')
                    return yield* Layer.build(
                      Storage.layerSql.pipe(
                        Layer.provide(
                          SqliteClient.layer({ filename: `${directory}/fatal.sqlite` }),
                        ),
                      ),
                    )
                  return yield* Layer.build(
                    Storage.layerJsonl({ filePath: `${directory}/fatal.jsonl` }),
                  )
                })
                const actual = Context.get(storage, Storage.Storage)
                const format = Context.get(storage, StorageFormat.StorageFormat)
                let injected = false
                const batches: ReadonlyArray<Record.StorageWrite>[] = []
                const capture: Storage.Storage['Service'] = {
                  ...actual,
                  commit: (input) => {
                    const batch = [...input]
                    const failed = batch.some(
                      (write) =>
                        write._tag === 'task' &&
                        write.value.kind === 'harness.tool' &&
                        write.value.state.status === 'terminal' &&
                        Schema.is(Schema.Struct({ status: Schema.tag('failed') }))(
                          write.value.state.outcome,
                        ),
                    )
                    if (fault !== 'none' && failed && !injected) {
                      injected = true
                      const failure = Effect.fail(
                        new StorageError({
                          reason: fault === 'uncertain' ? 'uncertain' : 'io',
                          operation: 'test.fatal',
                          message: 'fatal settlement rejected',
                        }),
                      )
                      return fault === 'uncertain'
                        ? actual.commit(batch).pipe(Effect.andThen(failure))
                        : failure
                    }
                    return actual.commit(batch).pipe(
                      Effect.tap(() =>
                        Effect.sync(() => {
                          batches.push(batch)
                        }),
                      ),
                    )
                  },
                }
                const captured = storage.pipe(
                  Context.add(Storage.Storage, capture),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: capture }),
                )
                const secondDone = yield* Deferred.make<void>()
                const ids: Record.TaskId[] = []
                let firstCalls = 0,
                  secondCalls = 0,
                  requests = 0
                const toolkit = Toolkit.make(
                  Tool.makeResult('first', {
                    success: Schema.String,
                    executionMode: mode,
                    replay: 'unsafe',
                  }),
                  Tool.makeResult('second', {
                    success: Schema.String,
                    executionMode: mode,
                    replay: 'safe',
                  }),
                )
                const handlers = toolkit.toLayer({
                  first: () =>
                    Effect.gen(function* () {
                      const access = yield* ToolExecution
                      ids[0] = access.taskId
                      firstCalls++
                      yield* access.output('fatal accepted 😀')
                      return {
                        content: [],
                        structuredOutput: 'observed',
                        spend: { amount: 3, currency: 'USD', source: 'observed' },
                      }
                    }),
                  second: () =>
                    Effect.gen(function* () {
                      const access = yield* ToolExecution
                      ids[1] = access.taskId
                      secondCalls++
                      yield* Deferred.succeed(secondDone, undefined)
                      return {
                        content: [],
                        structuredOutput: 'success',
                        control: { terminate: true },
                      }
                    }),
                })
                const hooks = [
                  Hook.make({
                    event: 'afterTool',
                    execute: ({ call, result }) =>
                      call.name === 'first'
                        ? Effect.gen(function* () {
                            if (mode === 'parallel') {
                              yield* Deferred.await(secondDone)
                              yield* actual.task(ids[1]!).pipe(
                                Effect.repeat({
                                  while: (task) =>
                                    Option.isNone(task) || task.value.state.status !== 'terminal',
                                }),
                              )
                            }
                            return { ...result, structuredOutput: 42 }
                          })
                        : Effect.succeed(result),
                  }),
                ]
                const model = yield* ScriptedStream.make({
                  generateText: () => {
                    requests++
                    return Effect.succeed([...calls, { ...finish, reason: 'tool-calls' as const }])
                  },
                })
                const firstScope = yield* Scope.make()
                yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
                const first = yield* HarnessRuntime.make({ tools: toolkit, hooks }).pipe(
                  Effect.provideContext(captured),
                  Scope.provide(firstScope),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provide(handlers),
                )
                const root = yield* first.backend.root
                const job = yield* first.backend.submit({
                  conversationId: root,
                  draft: { type: 'input', content: 'go' },
                })
                if (fault === 'none')
                  assert.strictEqual((yield* first.backend.wait(job.id)).status, 'unanswered')
                else
                  assert.strictEqual(
                    (yield* first.backend.wait(job.id).pipe(Effect.flip))._tag,
                    'HarnessError',
                  )
                yield* Scope.close(firstScope, Exit.void)
                const next = yield* HarnessRuntime.make({ tools: toolkit, hooks }).pipe(
                  Effect.provideContext(storage),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provide(handlers),
                )
                const settled = yield* next.backend.wait(job.id)
                assert.strictEqual(settled.status, 'unanswered')
                assert.include(JSON.stringify(settled), 'Expected string')
                assert.include(JSON.stringify(settled), 'structuredOutput')
                assert.strictEqual(firstCalls, 1)
                assert.strictEqual(secondCalls, 1)
                assert.strictEqual(requests, 1)
                const tasks = yield* Session.scanTasks(next.session).pipe(Stream.runCollect)
                const parent = tasks.find((task) => task.kind === 'harness.run')!
                const entries = yield* Session.scanEntries(next.session, {
                  conversationId: root,
                }).pipe(Stream.runCollect)
                assert.strictEqual(
                  entries.filter((entry) => entry.kind === 'tool.result').length,
                  1,
                )
                assert.strictEqual(
                  entries.find((entry) => entry.kind === 'tool.result')?.byTaskId,
                  ids[1],
                )
                const receipts = (yield* Session.commit(next.session, (tx) =>
                  Usage.receipts(tx, root),
                )).filter((receipt) => receipt.identity._tag === 'toolCall')
                const fatalReceipt = receipts.find(
                  (receipt) =>
                    receipt.identity._tag === 'toolCall' && receipt.identity.taskId === ids[0],
                )!
                assert.strictEqual(fatalReceipt.outcome._tag, 'failed')
                assert.isUndefined(fatalReceipt.linkedEntryId)
                assert.strictEqual(fatalReceipt.usage?.cost?.amount, 3)
                assert.strictEqual(
                  (yield* Session.usage(next.session)).costs.find((cost) => cost.currency === 'USD')
                    ?.tool,
                  3,
                )
                const round = yield* Session.commit(next.session, (tx) =>
                  Round.receipt(
                    tx,
                    root,
                    `tool-round:${parent.id}:${Schema.decodeUnknownSync(Round.InputSchema)(tasks.find((task) => task.id === ids[0])!.input).preparation.requestId}`,
                  ),
                )
                assert.isTrue(round.applied)
                assert.isTrue(round.slots[0]?.fatal)
                assert.strictEqual(round.slots[0]?.receipt?.status, 'failed')
                assert.isUndefined(round.slots[0]?.entryId)
                if (fault === 'none') {
                  const batch = batches.find((batch) =>
                    batch.some(
                      (write) =>
                        write._tag === 'task' &&
                        write.value.id === ids[0] &&
                        write.value.state.status === 'terminal',
                    ),
                  )!
                  assert.strictEqual(batch.filter((write) => write._tag === 'entry').length, 0)
                  assert.isAtLeast(
                    batch.filter((write) => write._tag.startsWith('document.')).length,
                    3,
                  )
                }
              }),
            ).pipe(Effect.provide(NodeServices.layer)),
        )

  it.effect(
    'TestClock cancellation capability finishes an incomplete UTF8 suffix once and never acknowledges accepted details',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const root = (yield* Session.commit(session, Transaction.ensureRoot)).id
          const gate: ToolProgress.Gate = { commit: (change) => Session.commit(session, change) }
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'partial',
            attempt: 0,
            policy: { minInterval: 100000, bytesPerSecond: 1024 },
          })
          yield* writer.details({ phase: 'committed' })
          yield* writer.output(new Uint8Array([0xf0, 0x9f]))
          const details = yield* writer
            .details({ phase: 'accepted' })
            .pipe(Effect.forkChild({ startImmediately: true }))
          assert.isUndefined(details.pollUnsafe())
          const first = yield* ProgressInternal.freezeCancellation(writer)
          const again = yield* ProgressInternal.freezeCancellation(writer)
          assert.deepStrictEqual(first, again)
          assert.strictEqual(first.output.text, '�')
          assert.strictEqual(first.output.totalBytes, 3)
          assert.deepStrictEqual(first.details, { phase: 'accepted' })
          assert.isTrue(Exit.isFailure(yield* Fiber.await(details)))
          assert.strictEqual(
            Option.getOrThrow(
              yield* Session.snapshot(
                session,
                ToolProgress.document,
                Round.target(root, 'partial'),
              ),
            ).value.output.text,
            '',
          )
          const abortWriter = yield* ProgressInternal.make(
            gate,
            { conversationId: root, callId: 'partial', attempt: 0 },
            first,
          )
          yield* abortWriter.settle('cancelled', () => Effect.void)
          const final = Option.getOrThrow(
            yield* Session.snapshot(session, ToolProgress.document, Round.target(root, 'partial')),
          ).value
          assert.strictEqual(final.output.totalBytes, 3)
          assert.strictEqual(final.output.text, '�')
          assert.strictEqual(final.status, 'cancelled')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['parallel', 'sequential'] as const)
      it.live(
        `${backend}: ${mode} preserves every handler compound branch and finishes its sibling`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const secondDone = yield* Deferred.make<void>()
              let firstCalls = 0,
                secondCalls = 0
              const toolkit = Toolkit.make(
                Tool.make('first', {
                  success: Schema.String,
                  failure: Schema.String,
                  executionMode: mode,
                }),
                Tool.make('second', { success: Schema.String, executionMode: mode }),
              )
              const model = yield* ScriptedStream.make({
                generateText: () =>
                  Effect.succeed([...calls, { ...finish, reason: 'tool-calls' as const }]),
              })
              const storage = yield* Effect.gen(function* () {
                if (backend === 'memory') return yield* Layer.build(Storage.layerMemory)
                if (backend === 'sqlite')
                  return yield* Layer.build(
                    Storage.layerSql.pipe(
                      Layer.provide(
                        SqliteClient.layer({ filename: `${directory}/compound.sqlite` }),
                      ),
                    ),
                  )
                return yield* Layer.build(
                  Storage.layerJsonl({ filePath: `${directory}/compound.jsonl` }),
                )
              })
              const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provide(
                  toolkit.toLayer({
                    first: () =>
                      Effect.gen(function* () {
                        firstCalls++
                        if (mode === 'parallel') yield* Deferred.await(secondDone)
                        return yield* Effect.failCause(
                          Cause.combine(
                            Cause.fail('fatal-domain-branch'),
                            Cause.die('fatal-cleanup-branch'),
                          ),
                        )
                      }),
                    second: () =>
                      Effect.gen(function* () {
                        secondCalls++
                        yield* Deferred.succeed(secondDone, undefined)
                        return 'success'
                      }),
                  }),
                ),
              )
              const root = yield* runtime.backend.root
              const job = yield* runtime.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'go' },
              })
              const result = yield* runtime.backend.wait(job.id)
              assert.strictEqual(result.status, 'unanswered')
              assert.include(JSON.stringify(result), 'fatal-domain-branch')
              assert.include(JSON.stringify(result), 'fatal-cleanup-branch')
              assert.strictEqual(firstCalls, 1)
              assert.strictEqual(secondCalls, 1)
              const receipts = (yield* Session.commit(runtime.session, (tx) =>
                Usage.receipts(tx, root),
              )).filter((receipt) => receipt.identity._tag === 'toolCall')
              const fatal = receipts.find(
                (receipt) =>
                  receipt.identity._tag === 'toolCall' && receipt.identity.callId === 'a',
              )!
              assert.deepInclude(fatal.outcome, { _tag: 'failed' })
              const branches = (yield* Schema.decodeUnknownEffect(
                Schema.Struct({
                  error: Schema.Struct({
                    branches: Schema.Array(
                      Schema.Struct({ kind: Schema.String, message: Schema.String }),
                    ),
                  }),
                }),
              )(fatal.outcome)).error.branches
              assert.deepStrictEqual(
                branches.map((branch) => branch.kind),
                ['Fail', 'Die'],
              )
              assert.isUndefined(fatal.linkedEntryId)
              assert.strictEqual(
                (yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
                  Stream.runCollect,
                )).filter((entry) => entry.kind === 'tool.result').length,
                1,
              )
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )

  for (const mode of ['single', 'defect', 'multiple', 'interrupt'] as const)
    it.live(`critical ${mode} hook failures preserve full native health cause`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const critical = Cause.fail(
            new StorageError({
              reason: 'io',
              operation: 'tool.critical',
              message: 'typed-critical-sentinel',
            }),
          )
          const causes = {
            single: critical,
            defect: Cause.combine(critical, Cause.die('critical-defect-sentinel')),
            multiple: Cause.combine(
              critical,
              Cause.fail(
                new StorageError({
                  reason: 'invalid',
                  operation: 'tool.second',
                  message: 'second-critical-sentinel',
                }),
              ),
            ),
            interrupt: Cause.combine(critical, Cause.interrupt()),
          }
          let handlers = 0
          const toolkit = Toolkit.make(Tool.make('first', { success: Schema.String }))
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.succeed([calls[0]!, { ...finish, reason: 'tool-calls' as const }]),
          })
          const runtime = yield* HarnessRuntime.make({
            tools: toolkit,
            hooks: [
              Hook.make({ event: 'beforeTool', execute: () => Effect.failCause(causes[mode]) }),
            ],
          }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provide(
              toolkit.toLayer({
                first: () =>
                  Effect.sync(() => {
                    handlers++
                    return 'unexpected'
                  }),
              }),
            ),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          const error = yield* runtime.backend.wait(job.id).pipe(Effect.flip)
          assert.include(error.message, 'typed-critical-sentinel')
          if (mode === 'defect') assert.include(error.message, 'critical-defect-sentinel')
          if (mode === 'multiple') assert.include(error.message, 'second-critical-sentinel')
          if (mode === 'interrupt') assert.include(error.message, 'Interrupt')
          assert.strictEqual(handlers, 0)
          assert.isTrue(Option.isSome(yield* runtime.scheduler.failure))
          assert.strictEqual(
            (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)).find(
              (task) => task.kind === 'harness.tool',
            )?.state.status,
            'running',
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
    )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: public tool-call record and settle enforce protection without mutation`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const filename = `${directory}/usage.${backend}`
            const context = yield* Layer.build(
              yield* Effect.sync(() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'sqlite')
                  return Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))
                return Storage.layerJsonl({ filePath: filename })
              }),
            )
            const storage = Context.get(context, Storage.Storage)
            const format = Context.get(context, StorageFormat.StorageFormat)
            let inspected = { formatVersion: 1, minimumReaderVersion: 1 }
            const legacyCapability = Context.add(context, StorageFormat.StorageFormat, {
              ...format,
              inspect: Effect.sync(() => inspected),
            })
            const session = yield* Session.make().pipe(Effect.provideContext(legacyCapability))
            const root = (yield* Session.commit(session, Transaction.ensureRoot)).id
            const task = yield* Session.commit(session, (tx) =>
              Transaction.createTask(tx, {
                conversationId: root,
                kind: 'public.usage.fixture',
                version: 1,
                input: null,
                background: false,
                abortRequested: false,
                state: { status: 'running', checkpoint: { phase: 'fixture' } },
              }),
            )
            const receipt: Usage.Receipt = {
              conversationId: root,
              identity: {
                _tag: 'toolCall',
                taskId: task.id,
                callId: 'original-provider-id',
                attempt: Usage.AttemptId.make(0),
              },
              outcome: { _tag: 'completed', value: 'result' },
            }
            const old: Usage.Receipt[] = [
              {
                ...receipt,
                identity: {
                  _tag: 'nestedCall',
                  taskId: task.id,
                  callId: Usage.NestedCallId.make('nested'),
                  attempt: Usage.AttemptId.make(0),
                },
              },
              {
                ...receipt,
                identity: {
                  _tag: 'requestAttempt',
                  requestId: Identity.RequestId.make('request'),
                  purpose: 'generation',
                  attempt: Usage.AttemptId.make(0),
                },
              },
            ]
            for (const prior of old) yield* Session.commit(session, (tx) => Usage.record(tx, prior))
            assert.deepStrictEqual(yield* format.inspect, {
              formatVersion: 12,
              minimumReaderVersion: 12,
            })
            let callback = 0
            for (const version of [1, 8]) {
              inspected = { formatVersion: version, minimumReaderVersion: version }
              const before = yield* format.inspect
              const bytes = backend === 'memory' ? undefined : yield* fs.readFile(filename)
              const documents = yield* storage
                .scanDocuments({ scope: { _tag: 'conversation', conversationId: root } })
                .pipe(Stream.runCollect)
              for (const method of ['record', 'settle'] as const) {
                const failure = yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.putTask(tx, {
                      ...task,
                      state: { status: 'running', checkpoint: { phase: 'staged' } },
                    })
                    return yield* method === 'record'
                      ? Usage.record(tx, receipt)
                      : Usage.settle(tx, receipt, () =>
                          Effect.sync(() => {
                            callback++
                          }),
                        )
                  }),
                ).pipe(Effect.flip)
                assert.deepInclude(failure, {
                  _tag: 'SessionError',
                  reason: 'invalid',
                  operation: 'usage.format',
                })
                assert.deepStrictEqual(yield* format.inspect, before)
                if (bytes !== undefined) assert.deepStrictEqual(yield* fs.readFile(filename), bytes)
                assert.deepStrictEqual(
                  yield* storage
                    .scanDocuments({ scope: { _tag: 'conversation', conversationId: root } })
                    .pipe(Stream.runCollect),
                  documents,
                )
                assert.deepStrictEqual(Option.getOrThrow(yield* storage.task(task.id)), task)
                assert.deepStrictEqual(
                  yield* Session.commit(session, (tx) => Usage.receipts(tx, root)),
                  old,
                )
              }
            }
            assert.strictEqual(callback, 0)
            for (const prior of old) yield* Session.commit(session, (tx) => Usage.record(tx, prior))
            assert.deepStrictEqual(yield* format.inspect, {
              formatVersion: 12,
              minimumReaderVersion: 12,
            })
            inspected = { formatVersion: 12, minimumReaderVersion: 12 }
            const committed = yield* Session.commit(session, (tx) =>
              Usage.settle(tx, receipt, () =>
                Effect.sync(() => {
                  callback++
                }),
              ),
            )
            assert.isTrue(committed.inserted)
            assert.isFalse(
              (yield* Session.commit(session, (tx) => Usage.record(tx, receipt))).inserted,
            )
            assert.strictEqual(callback, 1)
            assert.strictEqual(
              (yield* Session.commit(session, (tx) => Usage.receipts(tx, root))).length,
              3,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  for (const mode of [
    'missing',
    'mismatched',
    'lower',
    'pair',
    'io',
    'corrupt',
    'unsupported',
    'uncertain',
    'mixed',
  ] as const)
    it.live(`public tool-call check preserves ${mode} capability failure`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Storage.layerMemory)
          const storage = Context.get(context, Storage.Storage)
          const format = Context.get(context, StorageFormat.StorageFormat)
          const original = new StorageError({
            reason: (() => {
              if (mode === 'uncertain') return 'uncertain'
              if (mode === 'corrupt') return 'corrupt'
              if (mode === 'unsupported') return 'unsupportedFormat'
              return 'io'
            })(),
            operation: 'usage.inspect.fixture',
            message: mode,
          })
          const defect = new Error('mixed-inspect-defect')
          let reads = 0
          const inspect = Effect.suspend(() => {
            reads++
            if (mode === 'mixed')
              return Effect.failCause(Cause.combine(Cause.fail(original), Cause.die(defect)))
            if (['io', 'corrupt', 'unsupported', 'uncertain'].includes(mode))
              return Effect.fail(original)
            return Effect.succeed({
              formatVersion: mode === 'pair' || mode === 'lower' ? 8 : 9,
              minimumReaderVersion: mode === 'lower' ? 8 : 9,
            })
          })
          const captured =
            mode === 'missing'
              ? Context.make(Storage.Storage, storage)
              : Context.add(context, StorageFormat.StorageFormat, {
                  ...format,
                  storage: mode === 'mismatched' ? { ...storage } : storage,
                  inspect,
                })
          const session = yield* Session.make().pipe(Effect.provideContext(captured))
          const root = (yield* Session.commit(session, Transaction.ensureRoot)).id
          const documentsBefore = yield* storage
            .scanDocuments({ scope: { _tag: 'conversation', conversationId: root } })
            .pipe(Stream.runCollect)
          const receipt: Usage.Receipt = {
            conversationId: root,
            identity: {
              _tag: 'toolCall',
              taskId: Record.TaskId.make(99),
              callId: 'original',
              attempt: Usage.AttemptId.make(0),
            },
            outcome: { _tag: 'failed', error: null },
          }
          const result = yield* Session.commit(session, (tx) => Usage.record(tx, receipt)).pipe(
            Effect.exit,
          )
          assert.isTrue(Exit.isFailure(result))
          if (!Exit.isFailure(result)) return
          if (mode === 'mixed') {
            assert.deepStrictEqual(
              result.cause.reasons.map((r) => r._tag),
              ['Fail', 'Die'],
            )
            assert.strictEqual(
              result.cause.reasons[0]?._tag === 'Fail' ? result.cause.reasons[0].error : undefined,
              original,
            )
            assert.strictEqual(
              result.cause.reasons[1]?._tag === 'Die' ? result.cause.reasons[1].defect : undefined,
              defect,
            )
          } else {
            const failure = Cause.findErrorOption(result.cause)
            assert.isTrue(Option.isSome(failure))
            if (!Option.isSome(failure)) return
            if (['io', 'corrupt', 'unsupported', 'uncertain'].includes(mode))
              assert.strictEqual(failure.value, original)
            else if (mode === 'pair')
              assert.deepInclude(failure.value, {
                _tag: 'StorageError',
                reason: 'unsupportedFormat',
              })
            else
              assert.deepInclude(failure.value, {
                _tag: 'SessionError',
                operation: 'usage.format',
                reason: 'invalid',
              })
          }
          assert.strictEqual(reads, mode === 'missing' || mode === 'mismatched' ? 0 : 1)
          assert.deepStrictEqual(yield* format.inspect, {
            formatVersion: 12,
            minimumReaderVersion: 12,
          })
          assert.deepStrictEqual(
            yield* storage
              .scanDocuments({ scope: { _tag: 'conversation', conversationId: root } })
              .pipe(Stream.runCollect),
            documentsBefore,
          )
          if (mode === 'uncertain')
            assert.strictEqual(
              yield* Session.commit(session, () => Effect.void).pipe(Effect.flip),
              original,
            )
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const fault of ['rollback', 'uncertain'] as const)
      it.live(
        `${backend}: direct public settlement ${fault} preserves the native receipt boundary`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const filename = `${directory}/fault.${backend}`
              const layer = yield* Effect.sync(() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'sqlite')
                  return Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))
                return Storage.layerJsonl({ filePath: filename })
              })
              const context = yield* Layer.build(layer)
              const actual = Context.get(context, Storage.Storage)
              const format = Context.get(context, StorageFormat.StorageFormat)
              yield* format.requireSupportedFormat({ formatVersion: 9, minimumReaderVersion: 9 })
              let armed = false
              const original = new StorageError({
                reason: fault === 'uncertain' ? 'uncertain' : 'io',
                operation: 'commit',
                message: `public-${fault}`,
              })
              const captured: Storage.Storage['Service'] = {
                ...actual,
                commit: (writes) =>
                  Effect.gen(function* () {
                    if (!armed) return yield* actual.commit(writes)
                    if (fault === 'uncertain') yield* actual.commit(writes)
                    return yield* original
                  }),
              }
              const services = Context.add(
                Context.add(context, Storage.Storage, captured),
                StorageFormat.StorageFormat,
                { ...format, storage: captured },
              )
              const state = yield* Effect.scoped(
                Effect.gen(function* () {
                  const session = yield* Session.make().pipe(Effect.provideContext(services))
                  const root = (yield* Session.commit(session, Transaction.ensureRoot)).id
                  const task = yield* Session.commit(session, (tx) =>
                    Transaction.createTask(tx, {
                      conversationId: root,
                      kind: 'public.usage',
                      version: 1,
                      input: null,
                      background: false,
                      abortRequested: false,
                      state: { status: 'running', checkpoint: { phase: 'before' } },
                    }),
                  )
                  const receipt: Usage.Receipt = {
                    conversationId: root,
                    identity: {
                      _tag: 'toolCall',
                      taskId: task.id,
                      callId: 'physical',
                      attempt: Usage.AttemptId.make(0),
                    },
                    outcome: { _tag: 'completed', value: 'result' },
                    usage: {
                      _tag: 'tool',
                      name: 'public.fixture',
                      cost: { amount: 3, currency: 'USD', source: 'observed' },
                    },
                  }
                  armed = true
                  assert.strictEqual(
                    yield* Session.commit(session, (tx) =>
                      Usage.settle(tx, receipt, (inner) =>
                        Transaction.putTask(inner, {
                          ...task,
                          state: { status: 'running', checkpoint: { phase: 'after' } },
                        }),
                      ),
                    ).pipe(Effect.flip),
                    original,
                  )
                  if (fault === 'uncertain')
                    assert.strictEqual(
                      yield* Session.commit(session, () => Effect.void).pipe(Effect.flip),
                      original,
                    )
                  return { root, task, receipt }
                }),
              )
              const reopened = yield* Session.make().pipe(Effect.provideContext(context))
              const receipts = yield* Session.commit(reopened, (tx) =>
                Usage.receipts(tx, state.root),
              )
              assert.deepStrictEqual(receipts, fault === 'uncertain' ? [state.receipt] : [])
              assert.deepStrictEqual(
                Option.getOrThrow(yield* actual.task(state.task.id)).state.checkpoint,
                { phase: fault === 'uncertain' ? 'after' : 'before' },
              )
              assert.deepStrictEqual(yield* format.inspect, {
                formatVersion: 12,
                minimumReaderVersion: 12,
              })
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )
})
