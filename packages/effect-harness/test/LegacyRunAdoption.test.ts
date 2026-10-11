import * as ScriptedStream from './fixtures/ScriptedStream.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Cause from 'effect/Cause'
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
import * as Document from 'effect-harness/Document'
import * as Extension from 'effect-harness/Extension'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import { HookExecution } from 'effect-harness/HookExecution'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageFormat } from 'effect-harness/StorageFormat'
import { StorageError } from 'effect-harness/StorageError'
import * as Task from 'effect-harness/Task'
import * as TaskHook from 'effect-harness/TaskHook'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolProgress from 'effect-harness/ToolProgress'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as Run from '../src/internal/RunState.js'
import * as Round from '../src/internal/ToolRound.js'

const root = Record.ROOT_CONVERSATION_ID
const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const plainModel = Layer.effect(
  LanguageModel.LanguageModel,
  ScriptedStream.make({
    generateText: () => Effect.succeed([{ type: 'text', text: 'answer' }, finish('stop')]),
  }),
)
const customContract = {
  kind: 'adoption.custom',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: Schema.Natural,
  failure: Schema.String,
}
const custom = Task.make({
  ...customContract,
  initial: () => ({ phase: 'run' as const }),
  phases: {
    run: ({ input }) =>
      Effect.gen(function* () {
        const access = yield* TaskExecution
        yield* access.commit((tx) =>
          access.progress(tx, customContract, {
            status: 'completing',
            outcome: { status: 'completed', result: input },
          }),
        )
      }),
  },
})
const customRegistration = Registry.withTask(
  Registry.toolkit({ id: 'custom', version: 1 }, Toolkit.make()),
  custom,
)
const tool = (replay: Tool.Replay) =>
  Tool.make('work', {
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    replay,
  })
const doc = Document.define({
  kind: 'adoption.task',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ value: Schema.Natural }),
  initial: () => ({ value: 1 }),
})
const drain = Effect.forEach(Array.from({ length: 60 }), () => Effect.yieldNow, { discard: true })

describe('native legacy run adoption', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const stage of ['queued', 'request', 'unstarted', 'safe', 'unsafe', 'prefix'] as const)
      it.effect(
        `${backend}: cold reopen ${stage} preserves identity, prefixes and custom task dispatch`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const memory = yield* Layer.build(Storage.layerMemory)
              const persistent =
                backend === 'sqlite'
                  ? Storage.layerSql.pipe(
                      Layer.provide(
                        SqliteClient.layer({ filename: `${directory}/adoption.sqlite` }),
                      ),
                    )
                  : Storage.layerJsonl({ filePath: `${directory}/adoption.jsonl` })
              const use = <A, E>(
                program: Effect.Effect<A, E, Storage.Storage | StorageFormat | Scope.Scope>,
              ) =>
                backend === 'memory'
                  ? program.pipe(Effect.provideContext(memory))
                  : program.pipe(Effect.provide(Layer.fresh(persistent)))
              const saved = yield* use(
                Effect.scoped(
                  Effect.gen(function* () {
                    const session = yield* Session.make()
                    return yield* Session.commit(session, (tx) =>
                      Effect.gen(function* () {
                        yield* Transaction.ensureRoot(tx)
                        const submission = yield* Transaction.createSubmission(tx, {
                          _tag: 'InputQueued',
                          type: 'input',
                          status: 'queued',
                          conversationId: root,
                        })
                        const prompt = Prompt.make([
                          Prompt.userMessage({ content: [Prompt.textPart({ text: 'old' })] }),
                        ])
                        const inputEntry =
                          stage === 'queued'
                            ? undefined
                            : yield* Transaction.appendEntry(tx, root, {
                                kind: 'input',
                                model: yield* Schema.encodeEffect(Run.Messages)(prompt.content),
                              })
                        if (inputEntry !== undefined && submission._tag === 'InputQueued')
                          yield* Transaction.putSubmission(tx, {
                            ...submission,
                            _tag: 'InputPlaced',
                            status: 'placed',
                            entry: inputEntry.id,
                          })
                        const call = yield* Schema.decodeEffect(Tool.CallSchema)({
                          id: 'second',
                          name: 'work',
                          arguments: { text: 'remaining' },
                        })
                        const first = {
                          ...call,
                          id: yield* Schema.decodeEffect(Tool.CallSchema)({
                            id: 'first',
                            name: 'work',
                            arguments: {},
                          }).pipe(Effect.map((item) => item.id)),
                        }
                        const result = {
                          content: [Prompt.textPart({ text: 'committed' })],
                          isError: false,
                          diagnostics: [],
                        }
                        const prefixMessage = Prompt.toolMessage({
                          content: [
                            Prompt.toolResultPart({
                              id: 'first',
                              name: 'work',
                              providerExecuted: false,
                              isFailure: false,
                              result: 'committed',
                            }),
                          ],
                        })
                        const savedPrompt =
                          stage === 'prefix'
                            ? Prompt.concat(prompt, Prompt.make([prefixMessage]))
                            : prompt
                        if (stage === 'prefix')
                          yield* Transaction.appendEntry(tx, root, {
                            kind: 'tool.result',
                            model: yield* Schema.encodeEffect(Run.Messages)([prefixMessage]),
                            data: yield* Schema.encodeEffect(Schema.toCodecJson(Schema.Json))({
                              committed: true,
                            }),
                          })
                        let checkpoint: Run.RunState
                        if (stage === 'queued')
                          checkpoint = {
                            _tag: 'queued',
                            phase: 'queued',
                            submissionId: submission.id,
                            draft: { type: 'input', content: 'old' },
                          }
                        else if (stage === 'request')
                          checkpoint = {
                            _tag: 'request',
                            phase: 'request',
                            submissionId: submission.id,
                            agent: {},
                            prompt,
                          }
                        else
                          checkpoint = {
                            _tag: 'tools',
                            phase: 'tools',
                            submissionId: submission.id,
                            agent: {},
                            prompt: savedPrompt,
                            calls: [
                              ...(stage === 'prefix'
                                ? [{ call: first, replay: 'unsafe' as const, started: true }]
                                : []),
                              {
                                call,
                                replay: stage === 'unsafe' ? 'unsafe' : 'safe',
                                started: stage === 'safe' || stage === 'unsafe',
                              },
                            ],
                            index: stage === 'prefix' ? 1 : 0,
                            results: stage === 'prefix' ? [result] : [],
                          }
                        const record = yield* Transaction.createTask(tx, {
                          kind: 'harness.run',
                          version: 1,
                          conversationId: root,
                          input: submission.id,
                          background: false,
                          abortRequested: false,
                          state: {
                            status: stage === 'queued' ? 'pending' : 'running',
                            checkpoint: yield* Run.encode(checkpoint),
                          },
                        })
                        yield* Transaction.ensureDocument(tx, doc, {
                          scope: { _tag: 'task', taskId: record.id },
                        })
                        const customRecord = yield* Transaction.createTask(tx, {
                          ...(yield* Task.initialize(custom, 23)),
                          kind: custom.kind,
                          version: 1,
                          conversationId: root,
                          background: false,
                          abortRequested: false,
                          state: { status: 'pending', checkpoint: { phase: 'run' } },
                        })
                        return { record, submission, customRecord }
                      }),
                    )
                  }),
                ),
              )
              const calls: string[] = []
              const toolkit = Toolkit.make(tool(stage === 'unsafe' ? 'unsafe' : 'safe'))
              yield* use(
                Effect.scoped(
                  Effect.gen(function* () {
                    const runtime = yield* HarnessRuntime.make({ tools: toolkit })
                    assert.strictEqual(
                      (yield* runtime.scheduler.inspect(saved.record.id)).resolution._tag,
                      'compatible',
                    )
                    yield* runtime.registry.install(customRegistration)
                    yield* runtime.scheduler.resume
                    const customResult = yield* runtime.scheduler.wait({
                      id: saved.customRecord.id,
                      definition: custom,
                    })
                    assert.deepStrictEqual(customResult, { status: 'completed', result: 23 })
                    const completed = yield* runtime.scheduler.waitForTask(saved.record.id)
                    assert.strictEqual(completed.kind, 'harness.run')
                    assert.strictEqual(completed.version, 1)
                    assert.strictEqual(completed.id, saved.record.id)
                    assert.strictEqual(completed.input, saved.submission.id)
                    assert.strictEqual(
                      (yield* Schema.decodeUnknownEffect(Record.TaskOutcome)(
                        completed.state.outcome,
                      )).status,
                      'completed',
                    )
                    const submission = Option.getOrThrow(
                      yield* Session.submission(runtime.session, saved.submission.id),
                    )
                    assert.strictEqual(submission.status, 'done')
                    const entries = yield* Session.scanEntries(runtime.session, {
                      conversationId: root,
                    }).pipe(Stream.runCollect)
                    const results = entries.filter((entry) => entry.kind === 'tool.result')
                    if (stage === 'prefix') assert.strictEqual(results.length, 2)
                    if (stage === 'unsafe')
                      assert.include(
                        JSON.stringify(results[0]?.data),
                        'external outcome is unknown',
                      )
                    assert.deepStrictEqual(
                      calls,
                      stage === 'queued' || stage === 'request' || stage === 'unsafe'
                        ? []
                        : ['remaining'],
                    )
                    const document = yield* Session.snapshot(runtime.session, doc, {
                      scope: { _tag: 'task', taskId: saved.record.id },
                    })
                    assert.isTrue(Option.isNone(document))
                    assert.strictEqual(
                      (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)).length,
                      2,
                    )
                  }),
                ).pipe(
                  Effect.provide([
                    plainModel,
                    toolkit.toLayer({
                      work: ({ text }) =>
                        Effect.sync(() => {
                          calls.push(text)
                          return text
                        }),
                    }),
                  ]),
                ),
              )
              yield* use(
                Effect.scoped(
                  Effect.gen(function* () {
                    const runtime = yield* HarnessRuntime.make()
                    yield* runtime.scheduler.resume
                    assert.strictEqual(
                      (yield* runtime.scheduler.waitForTask(saved.record.id)).state.status,
                      'terminal',
                    )
                    assert.strictEqual(
                      (yield* runtime.scheduler.waitForTask(saved.customRecord.id)).state.status,
                      'terminal',
                    )
                    assert.strictEqual(
                      (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)).length,
                      2,
                    )
                  }),
                ).pipe(Effect.provide(plainModel)),
              )
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )

  it.effect(
    'missing custom definition is blocked and installing it wakes the real runtime without resume',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* HarnessRuntime.make()
          yield* runtime.backend.root
          const record = yield* Session.commit(runtime.session, (tx) =>
            Transaction.createTask(tx, {
              kind: custom.kind,
              version: 1,
              conversationId: root,
              input: 7,
              background: false,
              abortRequested: false,
              state: { status: 'pending', checkpoint: { phase: 'run' } },
            }),
          )
          yield* drain
          const inspection = yield* runtime.scheduler.inspect(record.id)
          assert.strictEqual(inspection.scheduling, 'blocked')
          assert.strictEqual(inspection.resolution._tag, 'blocked')
          yield* runtime.registry.install(customRegistration)
          assert.deepStrictEqual((yield* runtime.scheduler.waitForTask(record.id)).state.outcome, {
            status: 'completed',
            result: 7,
          })
          const duplicate = yield* TaskScheduler.make(runtime.session).pipe(
            Effect.provideService(Registry.Registry, runtime.registry),
            Effect.flip,
          )
          assert.strictEqual(duplicate._tag, 'TaskError')
        }),
      ).pipe(Effect.provide([Storage.layerMemory, plainModel])),
  )

  it.effect('rejects inconsistent legacy prefixes without orphaning or invoking them', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make()
        yield* runtime.backend.root
        const submitted = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'pause' },
        })
        yield* runtime.backend.wait(submitted.id)
        const record = yield* Session.commit(runtime.session, (tx) =>
          Transaction.createTask(tx, {
            kind: 'harness.run',
            version: 1,
            input: submitted.id,
            conversationId: root,
            background: false,
            abortRequested: false,
            state: {
              status: 'pending',
              checkpoint: {
                _tag: 'tools',
                phase: 'tools',
                submissionId: submitted.id,
                agent: {},
                prompt: { content: [] },
                calls: [],
                index: 1,
                results: [],
              },
            },
          }),
        )
        yield* drain
        const inspection = yield* runtime.scheduler.inspect(record.id)
        assert.strictEqual(inspection.scheduling, 'blocked')
        assert.strictEqual(inspection.record.state.status, 'pending')
        assert.strictEqual(inspection.resolution._tag, 'blocked')
      }),
    ).pipe(Effect.provide([Storage.layerMemory, plainModel])),
  )
  it.effect(
    'actual interrupted tool and custom phase reopen together; retained callbacks revoke and task docs retire',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const memory = yield* Layer.build(Storage.layerMemory)
          const entered = yield* Deferred.make<void>()
          const customEntered = yield* Deferred.make<void>()
          let release = false
          let toolCalls = 0
          let customCalls = 0
          let retained: typeof ToolExecution.Service | undefined
          const toolkit = Toolkit.make(tool('safe'))
          const handlers = toolkit.toLayer({
            work: () =>
              Effect.gen(function* () {
                toolCalls++
                const access = yield* ToolExecution
                retained = access
                if (!release) {
                  yield* access.commit((tx) =>
                    Transaction.ensureDocument(tx, doc, {
                      scope: { _tag: 'task', taskId: access.taskId },
                    }).pipe(Effect.asVoid),
                  )
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }
                return 'done'
              }),
          })
          const resumedCustom = Task.make({
            ...customContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: ({ input }) =>
                Effect.gen(function* () {
                  customCalls++
                  if (!release) {
                    yield* Deferred.succeed(customEntered, undefined)
                    return yield* Effect.never
                  }
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    access.progress(tx, customContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: input },
                    }),
                  )
                }),
            },
          })
          const registration = Registry.withTask(
            Registry.toolkit({ id: 'custom', version: 1 }, Toolkit.make()),
            resumedCustom,
          )
          const model = Layer.effect(
            LanguageModel.LanguageModel,
            ScriptedStream.make({
              generateText: ({ prompt }) =>
                Effect.succeed(
                  prompt.content.some((message) => message.role === 'tool')
                    ? [{ type: 'text', text: 'complete' }, finish('stop')]
                    : [
                        {
                          type: 'tool-call',
                          id: 'native-live',
                          name: 'work',
                          params: { text: 'run' },
                          providerExecuted: false,
                        },
                        finish('tool-calls'),
                      ],
                ),
            }),
          )
          const firstScope = yield* Scope.make()
          const first = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideContext(memory),
            Effect.provide([model, handlers]),
            Scope.provide(firstScope),
          )
          yield* first.backend.root
          yield* first.registry.install(registration)
          const ref = yield* first.scheduler.start(resumedCustom, 9, { conversationId: root })
          const submission = yield* first.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'run' },
          })
          yield* Deferred.await(entered)
          yield* Deferred.await(customEntered)
          const before = yield* Session.scanTasks(first.session).pipe(Stream.runCollect)
          const run = before.find((record) => record.kind === 'harness.run')!
          assert.strictEqual((yield* Run.decode(run.state.checkpoint)).phase, 'toolRound')
          yield* Scope.close(firstScope, Exit.void)
          assert.strictEqual(
            (yield* Context.get(memory, Storage.Storage).task(run.id)).pipe(Option.getOrThrow).state
              .status,
            'waiting',
          )
          assert.strictEqual(
            (yield* retained!.output('late').pipe(Effect.flip))._tag,
            'ExecutionError',
          )
          assert.strictEqual((yield* first.scheduler.resume.pipe(Effect.flip))._tag, 'TaskError')
          release = true
          yield* Effect.scoped(
            Effect.gen(function* () {
              const next = yield* HarnessRuntime.make({ tools: toolkit })
              yield* next.registry.install(registration)
              yield* next.scheduler.resume
              assert.deepStrictEqual(yield* next.scheduler.wait(ref), {
                status: 'completed',
                result: 9,
              })
              assert.strictEqual((yield* next.backend.wait(submission.id)).status, 'done')
              assert.strictEqual((yield* next.scheduler.waitForTask(run.id)).id, run.id)
              assert.isTrue(
                Option.isNone(
                  yield* Session.snapshot(next.session, doc, {
                    scope: { _tag: 'task', taskId: run.id },
                  }),
                ),
              )
              assert.strictEqual(toolCalls, 2)
              assert.strictEqual(customCalls, 2)
              const tasks = yield* Session.scanTasks(next.session).pipe(Stream.runCollect)
              assert.strictEqual(tasks.length, 3)
              const child = tasks.find((task) => task.kind === 'harness.tool')!
              assert.strictEqual(child.owner, run.id)
              assert.strictEqual(
                (yield* Task.decode(Round.InputSchema, child.input)).call.id,
                'native-live',
              )
            }),
          ).pipe(Effect.provideContext(memory), Effect.provide([model, handlers]))
        }),
      ),
  )

  it.effect(
    'independently bound toolkit contexts execute together without merging and leases survive reload',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          class Owner extends Context.Service<Owner, string>()('adoption/Owner') {}
          const first = Toolkit.make(
            Tool.make('first', {
              parameters: Schema.Struct({}),
              success: Schema.String,
              replay: 'safe',
              executionMode: 'parallel',
            }).addDependency(Owner),
          )
          const second = Toolkit.make(
            Tool.make('second', {
              parameters: Schema.Struct({}),
              success: Schema.String,
              replay: 'safe',
              executionMode: 'parallel',
            }).addDependency(Owner),
          )
          const entered = yield* Deferred.make<void>()
          const secondEntered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const seen: string[] = []
          let released = 0
          const own = (name: string) =>
            Layer.effect(
              Owner,
              Effect.acquireRelease(Effect.succeed(name), () =>
                Effect.sync(() => {
                  released++
                }),
              ),
            )
          const firstLayer = first
            .toLayer({
              first: () =>
                Effect.gen(function* () {
                  seen.push(yield* Owner)
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  return 'first'
                }),
            })
            .pipe(Layer.provideMerge(own('A')))
          const secondLayer = second
            .toLayer({
              second: () =>
                Effect.gen(function* () {
                  const owner = yield* Owner
                  seen.push(owner)
                  yield* Deferred.succeed(secondEntered, undefined)
                  yield* Deferred.await(release)
                  return owner
                }),
            })
            .pipe(Layer.provideMerge(own('B')))
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.provide(Registry.toolkit({ id: 'first', version: 1 }, first), firstLayer),
          )
          yield* registry.install(
            Registry.provide(Registry.toolkit({ id: 'second', version: 1 }, second), secondLayer),
          )
          const runtime = yield* HarnessRuntime.make({ registry })
          assert.strictEqual(runtime.registry, registry)
          yield* runtime.backend.root
          const submission = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'both' },
          })
          yield* Deferred.await(entered)
          yield* Deferred.await(secondEntered)
          yield* registry.uninstall('second')
          assert.strictEqual(released, 0)
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
          yield* drain
          assert.deepStrictEqual(seen, ['A', 'B'])
          assert.strictEqual(released, 1)
        }),
      ).pipe(
        Effect.provide([
          Storage.layerMemory,
          Layer.effect(
            LanguageModel.LanguageModel,
            ScriptedStream.make({
              generateText: ({ prompt }) =>
                Effect.succeed(
                  prompt.content.some((message) => message.role === 'tool')
                    ? [{ type: 'text', text: 'answer' }, finish('stop')]
                    : [
                        {
                          type: 'tool-call',
                          id: 'A',
                          name: 'first',
                          params: {},
                          providerExecuted: false,
                        },
                        {
                          type: 'tool-call',
                          id: 'B',
                          name: 'second',
                          params: {},
                          providerExecuted: false,
                        },
                        finish('tool-calls'),
                      ],
                ),
            }),
          ),
        ]),
      ),
  )

  it.effect(
    'section and hook access revoke after callbacks while the native request remains active',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const retained: Array<HookExecution['Service']> = []
          const capture = HookExecution.pipe(
            Effect.tap((value) => Effect.sync(() => retained.push(value))),
            Effect.asVoid,
          )
          const runtime = yield* HarnessRuntime.make({
            hooks: [Hook.make({ event: 'beforeRequest', execute: () => capture })],
            extensions: [
              Extension.make({
                name: 'guidance',
                sections: [
                  PromptSection.make({
                    key: 'guidance',
                    render: () => capture.pipe(Effect.as('guide')),
                  }),
                ],
              }),
            ],
          }).pipe(
            Effect.provide(
              Layer.effect(
                LanguageModel.LanguageModel,
                ScriptedStream.make({
                  generateText: () =>
                    Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.as([{ type: 'text', text: 'answer' }, finish('stop')]),
                    ),
                }),
              ),
            ),
          )
          yield* runtime.backend.root
          const submission = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'capture callbacks' },
          })
          yield* Deferred.await(entered)
          assert.strictEqual(retained.length, 2)
          for (const callback of retained) {
            const snapshotError = yield* callback
              .snapshot(doc, {
                scope: { _tag: 'task', taskId: Record.TaskId.make(1) },
              })
              .pipe(Effect.flip)
            assert.deepInclude(snapshotError, { _tag: 'ExecutionError', reason: 'revoked' })
            const commitError = yield* callback
              .commit((tx) => Transaction.appendEntry(tx, root, { kind: 'late.callback' }))
              .pipe(Effect.flip)
            assert.deepInclude(commitError, { _tag: 'ExecutionError', reason: 'revoked' })
          }
          assert.strictEqual(
            (yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
              Stream.runCollect,
            )).filter((entry) => entry.kind === 'late.callback').length,
            0,
          )
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'builtin hooks dispatch real phase/failure/abort/settled events; reserved and counterfeit registrations are denied',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* HarnessRuntime.make()
          yield* runtime.backend.root
          const definition = {
            kind: 'harness.run',
            version: 1,
            input: Record.SubmissionId,
            checkpoint: Run.RunStateSchema,
            success: Record.EntryId,
            failure: Schema.Json,
          }
          assert.strictEqual(
            (yield* runtime.registry.snapshot).tasks.find(
              (item) => item.declaration.kind === 'harness.run',
            )!.declaration.checkpoint,
            definition.checkpoint,
          )
          const stages: string[] = []
          for (const stage of ['beforePhase', 'afterPhase', 'failure', 'abort', 'settled'] as const)
            yield* runtime.registry.install(
              Registry.taskHook(
                { id: `hook-${stage}`, version: 1 },
                TaskHook.make(definition, {
                  phase: 'queued',
                  stage,
                  execute: () =>
                    Effect.sync(() => {
                      stages.push(stage)
                    }),
                }),
              ),
            )
          const good = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'good' },
          })
          assert.strictEqual((yield* runtime.backend.wait(good.id)).status, 'done')
          yield* drain
          assert.deepStrictEqual(stages, ['beforePhase', 'afterPhase', 'settled'])
          const failed = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'fail' },
          })
          assert.strictEqual((yield* runtime.backend.wait(failed.id)).status, 'unanswered')
          yield* drain
          assert.deepStrictEqual(stages.slice(-3), ['beforePhase', 'failure', 'settled'])
          const publicReserved = Registry.withTask(
            Registry.toolkit({ id: 'counterfeit', version: 1 }, Toolkit.make()),
            Task.make({
              ...customContract,
              kind: 'harness.run',
              initial: () => ({ phase: 'run' as const }),
              phases: { run: () => Effect.void },
            }),
          )
          assert.strictEqual(
            (yield* runtime.registry.install(publicReserved).pipe(Effect.flip))._tag,
            'RegistryError',
          )
          assert.strictEqual(
            (yield* runtime.registry
              .install(Registry.toolkit({ id: 'native-runtime-run', version: 1 }, Toolkit.make()))
              .pipe(Effect.flip))._tag,
            'RegistryError',
          )
          // Cloning the public registration metadata/contents cannot authenticate a private native registration.
          const fake: Registry.Registration = {
            definition: { id: 'fake-native', version: 1 },
            acquire: publicReserved.acquire,
          }
          assert.strictEqual(
            (yield* runtime.registry.install(fake).pipe(Effect.flip))._tag,
            'RegistryError',
          )
          const record = yield* Session.commit(runtime.session, (tx) =>
            Effect.gen(function* () {
              const submission = yield* Transaction.createSubmission(tx, {
                _tag: 'InputQueued',
                type: 'input',
                status: 'queued',
                conversationId: root,
              })
              return yield* Transaction.createTask(tx, {
                kind: 'harness.run',
                version: 1,
                conversationId: root,
                input: submission.id,
                background: false,
                abortRequested: true,
                state: {
                  status: 'pending',
                  checkpoint: yield* Run.encode({
                    _tag: 'queued',
                    phase: 'queued',
                    submissionId: submission.id,
                    draft: { type: 'input', content: 'abort' },
                  }),
                },
              })
            }),
          )
          assert.strictEqual(
            (yield* Schema.decodeUnknownEffect(Record.TaskOutcome)(
              (yield* runtime.scheduler.waitForTask(record.id)).state.outcome,
            )).status,
            'aborted',
          )
          yield* drain
          assert.deepStrictEqual(stages.slice(-3), ['abort', 'failure', 'settled'])
        }),
      ).pipe(
        Effect.provide([
          Storage.layerMemory,
          Layer.effect(
            LanguageModel.LanguageModel,
            ScriptedStream.make({
              generateText: ({ prompt }) =>
                Effect.succeed(
                  prompt.content.some(
                    (message) =>
                      message.role === 'user' &&
                      message.content.some((part) => part.type === 'text' && part.text === 'fail'),
                  )
                    ? []
                    : [{ type: 'text', text: 'answer' }, finish('stop')],
                ),
            }),
          ),
        ]),
      ),
  )
})

it.effect(
  'creation notification fails after commit once while ordinary diagnostics remain nonfatal',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        class CreationValue extends Context.Service<CreationValue, string>()(
          'test/creation/value',
        ) {}
        const scope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
        let notifications = 0
        let diagnostics = 0
        let released = 0
        const created = Hook.provide(
          Hook.make({
            event: 'conversationCreated',
            execute: () =>
              Effect.gen(function* () {
                assert.strictEqual(yield* CreationValue, 'captured')
                notifications++
                return yield* Effect.fail('creation notification failed')
              }),
          }),
          Layer.effect(
            CreationValue,
            Effect.acquireRelease(Effect.succeed('captured'), () =>
              Effect.sync(() => {
                released++
              }),
            ),
          ),
        )
        const diagnostic = Hook.make({
          event: 'afterResponse',
          execute: () =>
            Effect.sync(() => {
              diagnostics++
            }).pipe(Effect.andThen(Effect.fail('ordinary response diagnostic'))),
        })
        const runtime = yield* HarnessRuntime.make({ hooks: [created, diagnostic] }).pipe(
          Effect.provideService(Scope.Scope, scope),
        )
        const failure = yield* runtime.backend.root.pipe(Effect.flip)
        assert.strictEqual(failure.operation, 'hook.execute')
        assert.isTrue(Option.isSome(yield* Session.conversation(runtime.session, root)))
        assert.strictEqual(yield* runtime.backend.root, root)
        assert.strictEqual(notifications, 1)
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'after committed creation' },
        })
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        assert.strictEqual(diagnostics, 1)
        assert.strictEqual(released, 0)
        yield* Scope.close(scope, Exit.void)
        assert.strictEqual(released, 1)
      }),
    ).pipe(Effect.provide([Storage.layerMemory, plainModel])),
)

for (const reused of [false, true]) {
  for (const boundary of ['read', 'handler', 'transaction', 'combined', 'interrupted'] as const) {
    it.effect(
      `caught output rejection cannot authorize ${boundary} failure with ${reused ? 'reused' : 'distinct'} error`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const real = yield* Storage.Storage
            const format = yield* StorageFormat
            const rejected = new StorageError({
              reason: 'io',
              operation: 'probe.output',
              message: 'proven rejected output',
            })
            const later = reused
              ? rejected
              : new StorageError({
                  reason: 'io',
                  operation: 'probe.read',
                  message: 'unrelated operation',
                })
            let nextRead = false
            let rejectedCount = 0
            let readCount = 0
            let settlementCount = 0
            let progressId: Record.DocumentId | undefined
            const storage = Storage.Storage.of({
              ...real,
              task: (...args) =>
                Effect.suspend(() => {
                  if (nextRead) {
                    nextRead = false
                    readCount++
                    return Effect.fail(later)
                  }
                  return real.task(...args)
                }),
              commit: (input) =>
                Effect.suspend(() => {
                  const writes = Array.from(input)
                  for (const write of writes) {
                    if (
                      write._tag === 'document.create' &&
                      write.record.kind === ToolProgress.document.definition.kind
                    )
                      progressId = write.record.id
                  }
                  if (
                    writes.some(
                      (write) => write._tag === 'document.change' && write.id === progressId,
                    )
                  ) {
                    rejectedCount++
                    return Effect.fail(rejected)
                  }
                  if (
                    writes.some(
                      (write) => write._tag === 'submission' && write.value.status === 'unanswered',
                    )
                  )
                    settlementCount++
                  return real.commit(writes)
                }),
            })
            const toolkit = Toolkit.make(
              Tool.make('probe', { success: Schema.String, replay: 'safe' }),
            )
            const handlers = toolkit.toLayer({
              probe: () =>
                Effect.gen(function* () {
                  const access = yield* ToolExecution
                  yield* access.output('reject me')
                  const output = yield* access
                    .details({ awaiting: 'durable output' })
                    .pipe(Effect.result)
                  assert.strictEqual(output._tag, 'Failure')
                  if (output._tag === 'Failure') assert.strictEqual(output.failure, rejected)
                  if (boundary === 'read') {
                    nextRead = true
                    return yield* storage.task(access.taskId).pipe(Effect.as('unreachable'))
                  }
                  if (boundary === 'handler') return yield* later
                  if (boundary === 'transaction')
                    return yield* access.commit(() => Effect.fail(later))
                  if (boundary === 'combined')
                    return yield* Effect.failCause(
                      Cause.combine(Cause.fail(later), Cause.die('nested failure')),
                    )
                  if (boundary === 'interrupted')
                    return yield* Effect.failCause(
                      Cause.combine(Cause.fail(later), Cause.interrupt()),
                    )
                  return 'caught'
                }),
            })
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.succeed([
                  {
                    type: 'tool-call',
                    id: 'one',
                    name: 'probe',
                    params: {},
                    providerExecuted: false,
                  },
                  finish('tool-calls'),
                ]),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideService(Storage.Storage, storage),
              Effect.provideService(StorageFormat, { ...format, storage }),
              Effect.provideService(LanguageModel.LanguageModel, native),
              Effect.provide(handlers),
            )
            yield* runtime.backend.root
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'probe' },
            })
            const result = yield* runtime.backend.wait(job.id).pipe(Effect.result)
            assert.strictEqual(result._tag, 'Failure')
            if (result._tag === 'Failure') {
              assert.strictEqual(result.failure.operation, 'runtime.scheduler')
              assert.include(result.failure.message, later.message)
              if (boundary === 'combined') assert.include(result.failure.message, 'nested failure')
            }
            assert.strictEqual(rejectedCount, 1)
            assert.strictEqual(readCount, boundary === 'read' ? 1 : 0)
            assert.strictEqual(settlementCount, 0)
            assert.strictEqual(Option.getOrThrow(yield* real.submission(job.id)).status, 'placed')
            const tasks = yield* Stream.runCollect(real.scanTasks({ kind: 'harness.run' }))
            assert.strictEqual(tasks.length, 1)
            assert.strictEqual(tasks[0]?.state.status, 'waiting')
            const state = yield* Run.decode(tasks[0]!.state.checkpoint)
            assert.strictEqual(state._tag, 'toolRound')
            if (state._tag !== 'toolRound') return
            const children = yield* Stream.runCollect(real.scanTasks({ kind: 'harness.tool' }))
            assert.strictEqual(children.length, 1)
            assert.strictEqual(children[0]?.owner, tasks[0]?.id)
            const childState = yield* Task.decode(
              Round.CheckpointSchema,
              children[0]!.state.checkpoint,
            )
            assert.strictEqual(childState.started, true)
            assert.strictEqual(childState.attempt, 0)
            const round = yield* Session.commit(runtime.session, (tx) =>
              Round.read(tx, root, state.roundKey),
            )
            assert.strictEqual(round.slots[0]?.taskId, children[0]?.id)
            assert.isUndefined(round.slots[0]?.result)
            assert.isUndefined(round.slots[0]?.receipt)
            const entries = yield* Stream.runCollect(real.scanEntries({ conversationId: root }))
            assert.strictEqual(entries.filter((entry) => entry.kind === 'tool.output').length, 0)
            assert.strictEqual(entries.filter((entry) => entry.kind === 'tool.result').length, 0)
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )
  }
}
