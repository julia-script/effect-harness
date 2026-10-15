import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Cause from 'effect/Cause'
import { ModelError, ModelUnsupportedError } from 'effect-harness/ModelError'
import * as Clock from 'effect/Clock'
import * as Duration from 'effect/Duration'
import * as Fiber from 'effect/Fiber'
import * as TestClock from 'effect/testing/TestClock'
import { StorageError } from 'effect-harness/StorageError'
import * as AiError from 'effect/ai/AiError'
import * as Extension from 'effect-harness/Extension'
import * as Hook from 'effect-harness/Hook'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as AnthropicLanguageModel from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
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
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import * as CompactionTask from 'effect-harness/CompactionTask'
import * as CompactionStatus from 'effect-harness/CompactionStatus'
import { CompactionRequest } from 'effect-harness/CompactionRequest'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const root = Record.ROOT_CONVERSATION_ID
const answer = (text: string): Response.PartEncoded[] => [
  { type: 'text', text },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 12, uncached: 12, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 3, text: 3, reasoning: undefined },
    },
    response: undefined,
  },
]
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sql = Storage.layerJournal().pipe(
    Layer.provide(SqlEventJournal.layer()),
    Layer.provide(SqliteClient.layer({ filename: `${directory}/compaction.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/compaction.jsonl` })
  return {
    directory,
    open: (backend: 'memory' | 'sqlite' | 'jsonl') =>
      backend === 'memory'
        ? Effect.succeed(memory)
        : Layer.build(Layer.fresh(backend === 'sqlite' ? sql : jsonl)),
  }
})
const seed = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      for (const text of ['old '.repeat(40), 'recent '.repeat(12)])
        yield* Transaction.appendEntry(tx, root, {
          kind: 'input',
          model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))([
            Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
          ]),
        })
    }),
  )
const entries = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root, order: 'ascending' }).pipe(
    Stream.runCollect,
  )
const policy = () =>
  Settings.make({
    compaction: { enabled: false, reserveTokens: 20, keepRecentTokens: 10, backgroundTokens: 0 },
  })
const descriptor = (
  native: Model.Any['languageModel'],
  capability?: Model.CompactionCapability,
  contextWindow?: number,
) =>
  Model.make({
    definition: {
      ref: { provider: 'fixture', modelId: 'summary' },
      maxOutputTokens: 9,
      ...(contextWindow === undefined ? {} : { contextWindow }),
      capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
    },
    languageModel: native,
    options: Schema.Struct({}),
    configure: () => Effect.succeed(Context.empty()),
    ...(capability === undefined ? {} : { compaction: capability }),
  })

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} durable compaction`, () => {
    it.live(
      'manual task pins policy, queues a fenced summary, records usage and clears separate status',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            let calls = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  calls++
                  const config = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
                  assert.strictEqual(config.maxOutputTokens, 9)
                  assert.strictEqual(config.cacheRetention, 'none')
                  assert.strictEqual(config.deferred, false)
                  const prepared = Option.getOrThrow(
                    yield* Effect.serviceOption(Prepared.PreparedRequest),
                  )
                  assert.deepEqual(prepared.current.tools, [])
                  return answer('Remember the old conversation.')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const handle = yield* runtime.backend.compact!(root)
            const result = yield* runtime.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
              Effect.catch((error) =>
                Effect.gen(function* () {
                  const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
                  return yield* Effect.die(new Error(JSON.stringify({ error, tasks })))
                }),
              ),
            )
            assert.strictEqual(result.status, 'completed', JSON.stringify(result))
            if (result.status !== 'completed' || result.result._tag !== 'Queued')
              return assert.fail('Expected queued compaction result')
            const placement = yield* runtime.backend.wait(result.result.submissionId)
            assert.strictEqual(placement.status, 'done')
            const history = yield* entries(runtime)
            assert.strictEqual(history.filter((item) => item.kind === 'compaction').length, 1)
            const receipts = yield* Session.commit(runtime.session, (tx) =>
              Usage.receipts(tx, root),
            )
            assert.strictEqual(
              receipts.filter(
                (item) =>
                  item.identity._tag === 'requestAttempt' && item.identity.purpose === 'compaction',
              ).length,
              1,
            )
            const status = yield* Session.snapshot(
              runtime.session,
              CompactionStatus.document,
              CompactionStatus.target(root),
            )
            assert.deepEqual(Option.getOrThrow(status).value.items, [])
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)))
                .minimumReaderVersion,
              12,
            )
            assert.strictEqual(calls, 1)
            assert.deepEqual(yield* runtime.backend.waitCompaction!(handle), result)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'cold recovery restores pinned summary configuration without configure replay or committed-summary replay',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const entered = yield* Deferred.make<void>()
            const settings = yield* policy()
            let calls = 0
            let configurations = 0
            class Config extends Context.Service<Config, { readonly value: string }>()(
              'test/compaction/pinned-config',
            ) {}
            const codec = yield* Model.makeConfiguration({
              id: 'fixture/config',
              version: 1,
              schema: Schema.Struct({ value: Schema.String }),
              tags: [Config],
              capture: (context) =>
                Effect.succeed(Option.getOrThrow(Context.getOption(context, Config))),
              restore: (value) => Effect.succeed(Context.make(Config, value)),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configuration: codec,
              configure: () =>
                Effect.sync(() => {
                  configurations++
                  return Context.make(Config, { value: 'pinned' })
                }),
            })
            const identities: string[] = []
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  calls++
                  assert.strictEqual(
                    Option.getOrThrow(yield* Effect.serviceOption(Config)).value,
                    'pinned',
                  )
                  const request = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
                  identities.push(request.requestId)
                  if (calls === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }
                  assert.strictEqual(request.attempt, 1)
                  assert.strictEqual(request.maxOutputTokens, 9)
                  return answer('Saved summary')
                }),
            })
            const open = Effect.gen(function* () {
              const storage = yield* store.open(backend)
              const runtime = yield* HarnessRuntime.make({
                models: [descriptor(native, capability)],
              }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(Settings.Settings, settings),
              )
              yield* runtime.backend.root
              return runtime
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* seed(first)
            const handle = yield* first.backend.compact!(root)
            yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
            const saved = yield* first.backend.compactionTask!(handle)
            assert.strictEqual(
              (yield* Schema.decodeUnknownEffect(
                Schema.toCodecJson(CompactionTask.CheckpointSchema),
              )(saved.state.checkpoint)).phase,
              'summarize',
            )
            yield* settings.update({ compaction: { reserveTokens: 50, keepRecentTokens: 50 } })
            yield* Scope.close(firstScope, Exit.void)
            const secondScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
            const second = yield* open.pipe(Effect.provideService(Scope.Scope, secondScope))
            const outcome = yield* second.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
            )
            if (outcome.status !== 'completed' || outcome.result._tag !== 'Queued')
              return assert.fail(JSON.stringify(outcome))
            assert.strictEqual(
              (yield* second.backend.wait(outcome.result.submissionId)).status,
              'done',
            )
            assert.strictEqual(configurations, 1)
            assert.deepEqual(identities, [identities[0], identities[0]])
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            const summary = receipts.filter(
              (item) =>
                item.identity._tag === 'requestAttempt' && item.identity.purpose === 'compaction',
            )
            assert.strictEqual(summary.length, 2)
            assert.strictEqual(summary[0]?.outcome._tag, 'interrupted')
            assert.strictEqual(summary[1]?.outcome._tag, 'completed')
            yield* Scope.close(secondScope, Exit.void)
            const third = yield* open
            assert.deepEqual(yield* third.backend.waitCompaction!(handle), outcome)
            assert.strictEqual(calls, 2)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'blocking threshold creates an owned child then reprepares generation once from the placed summary',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* Settings.make({
              compaction: {
                enabled: true,
                reserveTokens: 20,
                keepRecentTokens: 10,
                backgroundTokens: 0,
              },
            })
            let summaries = 0
            let generations = 0
            const native = yield* ScriptedStream.make({
              generateText: (request) =>
                Effect.gen(function* () {
                  if (Option.isSome(yield* Effect.serviceOption(CompactionRequest))) {
                    summaries++
                    return answer('Old work finished')
                  }
                  generations++
                  assert.isTrue(JSON.stringify(request.prompt).includes('<summary>'))
                  return answer('Generation after compaction')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability, 80)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            const result = yield* runtime.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
            assert.strictEqual(result.status, 'done', JSON.stringify(result))
            assert.strictEqual(summaries, 1)
            assert.strictEqual(generations, 1)
            const tasks = yield* Session.scanTasks(runtime.session, {
              kind: CompactionTask.kind,
            }).pipe(Stream.runCollect)
            assert.strictEqual(tasks.length, 1)
            assert.isDefined(tasks[0]?.owner)
            const history = yield* entries(runtime)
            assert.strictEqual(history.filter((item) => item.kind === 'compaction').length, 1)
            const status = yield* Session.snapshot(
              runtime.session,
              CompactionStatus.document,
              CompactionStatus.target(root),
            )
            assert.deepEqual(Option.getOrThrow(status).value.items, [])
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    for (const mode of ['success', 'secondOverflow', 'ordinaryInvalid'] as const)
      it.live(`structured overflow ${mode} has one finite compaction allowance`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* Settings.make({
              compaction: {
                enabled: true,
                reserveTokens: 20,
                keepRecentTokens: 10,
                backgroundTokens: 0,
              },
              retry: { maxRetries: 5 },
            })
            let summaries = 0
            let generations = 0
            const overflow = AiError.make({
              module: 'Fixture',
              method: 'generate',
              reason: new AiError.InvalidRequestError({
                metadata: { fixture: { code: 'context_length' } },
                description: 'Structured fixture overflow',
              }),
            })
            const ordinary = AiError.make({
              module: 'Fixture',
              method: 'generate',
              reason: new AiError.InvalidRequestError({
                parameter: 'temperature',
                description: 'Invalid temperature',
              }),
            })
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  if (Option.isSome(yield* Effect.serviceOption(CompactionRequest))) {
                    summaries++
                    return answer('Summary for overflow')
                  }
                  generations++
                  if (mode === 'ordinaryInvalid') return yield* ordinary
                  if (generations === 1 || mode === 'secondOverflow') return yield* overflow
                  return answer('Fits after compaction')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
              isOverflow: (error) =>
                error.reason._tag === 'InvalidRequestError' &&
                Schema.is(Schema.Struct({ code: Schema.Literal('context_length') }))(
                  error.reason.metadata.fixture,
                ),
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            const outcome = yield* runtime.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
            assert.strictEqual(
              outcome.status,
              mode === 'success' ? 'done' : 'unanswered',
              JSON.stringify(outcome),
            )
            assert.strictEqual(summaries, mode === 'ordinaryInvalid' ? 0 : 1)
            assert.strictEqual(generations, mode === 'ordinaryInvalid' ? 1 : 2)
            const tasks = yield* Session.scanTasks(runtime.session, {
              kind: CompactionTask.kind,
            }).pipe(Stream.runCollect)
            assert.strictEqual(tasks.length, mode === 'ordinaryInvalid' ? 0 : 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    for (const change of ['suffix', 'selectedEdit', 'retainedEdit', 'sameHead'] as const)
      it.live(`queued summary placement revalidates ${change} inside the native boundary`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  if (Option.isSome(yield* Effect.serviceOption(CompactionRequest)))
                    return answer('Queued summary')
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  return answer('Foreground completed')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
            const handle = yield* runtime.backend.compact!(root)
            const result = yield* runtime.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
            )
            if (result.status !== 'completed' || result.result._tag !== 'Queued')
              return assert.fail(JSON.stringify(result))
            const task = yield* runtime.backend.compactionTask!(handle)
            const history = yield* entries(runtime)
            const selected = history.find((entry) => entry.kind === 'input')!
            const retained = history.filter((entry) => entry.kind === 'input').at(-2)!
            yield* Session.commit(runtime.session, (tx) =>
              Effect.gen(function* () {
                if (change === 'suffix')
                  yield* Transaction.appendEntry(tx, root, {
                    kind: 'custom',
                    model: [{ role: 'user', content: 'safe new suffix' }],
                  })
                else if (change === 'sameHead')
                  yield* Transaction.appendEntry(tx, root, {
                    kind: 'context-boundary',
                    head: retained.id,
                  })
                else
                  yield* Transaction.appendEntry(tx, root, {
                    kind: 'edit',
                    edits: [
                      {
                        _tag: 'replace',
                        target: change === 'selectedEdit' ? selected.id : retained.id,
                        messages: [{ role: 'user', content: 'changed after summary admission' }],
                      },
                    ],
                  })
              }),
            )
            assert.strictEqual(task.state.status, 'terminal')
            yield* Deferred.succeed(release, undefined)
            yield* runtime.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
            const placed = yield* runtime.backend
              .wait(result.result.submissionId)
              .pipe(Effect.timeout('5 seconds'))
            assert.strictEqual(
              placed.status,
              change === 'suffix' ? 'done' : 'unanswered',
              JSON.stringify(placed),
            )
            assert.strictEqual(
              (yield* entries(runtime)).filter((entry) => entry.kind === 'compaction').length,
              change === 'suffix' ? 1 : 0,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    for (const decision of ['decline', 'empty', 'supplied'] as const)
      it.live(`beforeCompact ${decision} preserves validation and barrier semantics`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            let calls = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  calls++
                  return answer('unexpected')
                }),
            })
            const extension = Extension.make({
              name: 'compaction-hook',
              hooks: [
                Hook.make({
                  event: 'beforeCompact',
                  execute: () =>
                    Effect.succeed(
                      decision === 'decline'
                        ? { _tag: 'decline' as const }
                        : {
                            _tag: 'summary' as const,
                            summary: decision === 'empty' ? '   ' : 'Hook supplied summary',
                          },
                    ),
                }),
              ],
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native)],
              extensions: [extension],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const before = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            const handle = yield* runtime.backend.compact!(root)
            const result = yield* runtime.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
            )
            assert.strictEqual(
              result.status,
              decision === 'empty' ? 'failed' : 'completed',
              JSON.stringify(result),
            )
            if (decision === 'supplied') {
              if (result.status !== 'completed' || result.result._tag !== 'Queued')
                return assert.fail(JSON.stringify(result))
              yield* runtime.backend.wait(result.result.submissionId)
              assert.strictEqual(
                (yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)))
                  .minimumReaderVersion,
                12,
              )
            } else
              assert.deepEqual(
                yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)),
                before,
              )
            assert.strictEqual(calls, 0)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    for (const fault of ['rejected', 'acknowledgementLost'] as const)
      it.live(`summary atomic publication ${fault} preserves accounting and cold authority`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const owner = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
            const storage = yield* store
              .open(backend)
              .pipe(Effect.provideService(Scope.Scope, owner))
            const real = Context.get(storage, Storage.Storage)
            const format = Context.get(storage, StorageFormat.StorageFormat)
            const settings = yield* policy()
            let failures = 0
            let calls = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  calls++
                  return answer('Paid summary')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
            })
            const models = [descriptor(native, capability)]
            const wrapped = Storage.Storage.of({
              ...real,
              commit: (input) =>
                Effect.suspend(() => {
                  const writes = Array.from(input)
                  const publishing = writes.some(
                    (write) =>
                      write._tag === 'task' &&
                      write.value.kind === CompactionTask.kind &&
                      (write.value.state.status === 'completing' ||
                        write.value.state.status === 'terminal') &&
                      Schema.is(Schema.Struct({ status: Schema.Literal('completed') }))(
                        write.value.state.outcome,
                      ),
                  )
                  if (publishing && failures === 0) {
                    failures++
                    const failure = new StorageError({
                      reason: fault === 'rejected' ? 'io' : 'uncertain',
                      operation: 'fixture.compaction-publication',
                      message: fault,
                    })
                    return fault === 'rejected'
                      ? Effect.fail(failure)
                      : real.commit(writes).pipe(Effect.andThen(Effect.fail(failure)))
                  }
                  return real.commit(writes)
                }),
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* HarnessRuntime.make({ models }).pipe(
              Effect.provideContext(
                storage.pipe(
                  Context.add(Storage.Storage, wrapped),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
                ),
              ),
              Effect.provideService(Settings.Settings, settings),
              Effect.provideService(Scope.Scope, firstScope),
            )
            yield* first.backend.root
            yield* seed(first)
            const handle = yield* first.backend.compact!(root)
            const firstOutcome = yield* Effect.exit(
              first.backend.waitCompaction!(handle).pipe(Effect.timeout('5 seconds')),
            )
            if (fault === 'rejected') {
              assert.isTrue(Exit.isSuccess(firstOutcome))
              if (Exit.isSuccess(firstOutcome))
                assert.strictEqual(firstOutcome.value.status, 'failed')
              const receipts = yield* Session.commit(first.session, (tx) =>
                Usage.receipts(tx, root),
              )
              assert.strictEqual(receipts.length, 1, JSON.stringify({ calls, receipts }))
              assert.strictEqual(receipts[0]?.usage?._tag, 'model')
              assert.strictEqual(receipts[0]?.outcome._tag, 'failed')
              assert.strictEqual(
                (yield* entries(first)).filter((item) => item.kind === 'compaction').length,
                0,
              )
            }
            yield* Scope.close(firstScope, Exit.void)
            const clean = yield* store.open(backend)
            const second = yield* HarnessRuntime.make({ models }).pipe(
              Effect.provideContext(clean),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* second.backend.root
            const secondOutcome = yield* second.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
            )
            assert.strictEqual(
              secondOutcome.status,
              fault === 'rejected' ? 'failed' : 'completed',
              JSON.stringify(secondOutcome),
            )
            if (secondOutcome.status === 'completed' && secondOutcome.result._tag === 'Queued')
              assert.strictEqual(
                (yield* second.backend.wait(secondOutcome.result.submissionId)).status,
                'done',
              )
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            assert.strictEqual(receipts.length, 1, JSON.stringify({ calls, receipts }))
            assert.strictEqual(receipts[0]?.usage?._tag, 'model')
            assert.strictEqual(
              (yield* entries(second)).filter((item) => item.kind === 'compaction').length,
              fault === 'rejected' ? 0 : 1,
            )
            assert.strictEqual(failures, 1)
            assert.strictEqual(calls, 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    it.live(
      'abort interrupts the real summary call and cold reopen cannot append a late summary',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            const entered = yield* Deferred.make<void>()
            let calls = 0
            let finalized = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      calls++
                    }),
                  ),
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Effect.sync(() => {
                      finalized++
                    }),
                  ),
                ),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
            })
            const models = [descriptor(native, capability)]
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* HarnessRuntime.make({ models }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
              Effect.provideService(Scope.Scope, firstScope),
            )
            yield* first.backend.root
            yield* seed(first)
            const handle = yield* first.backend.compact!(root)
            yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
            yield* first.backend.abortCompaction!(handle).pipe(Effect.timeout('5 seconds'))
            assert.strictEqual((yield* first.backend.waitCompaction!(handle)).status, 'aborted')
            assert.strictEqual(finalized, 1)
            yield* Scope.close(firstScope, Exit.void)
            const clean = yield* store.open(backend)
            const second = yield* HarnessRuntime.make({ models }).pipe(
              Effect.provideContext(clean),
              Effect.provideService(Settings.Settings, settings),
            )
            assert.strictEqual((yield* second.backend.waitCompaction!(handle)).status, 'aborted')
            assert.strictEqual(
              (yield* entries(second)).filter((item) => item.kind === 'compaction').length,
              0,
            )
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            assert.strictEqual(receipts.length, 1, JSON.stringify({ calls, receipts }))
            assert.strictEqual(receipts[0]?.outcome._tag, 'interrupted')
            assert.strictEqual(calls, 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live('a callback returning after durable abort cannot publish a late summary', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          const storage = yield* store.open(backend)
          const settings = yield* policy()
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let returned = 0
          const native = yield* ScriptedStream.make({
            generateText: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
                returned++
                return answer('Late response must never become a summary')
              }).pipe(Effect.uninterruptible),
          })
          const capability = yield* Model.makeCompaction({
            binding: { id: 'fixture/late', version: 1 },
            configure: (context) => Effect.succeed(context),
          })
          const runtime = yield* HarnessRuntime.make({
            models: [descriptor(native, capability)],
          }).pipe(
            Effect.provideContext(storage),
            Effect.provideService(Settings.Settings, settings),
          )
          yield* runtime.backend.root
          yield* seed(runtime)
          const handle = yield* runtime.backend.compact!(root)
          yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
          const abort = yield* runtime.backend.abortCompaction!(handle).pipe(Effect.forkChild)
          yield* runtime.backend.compactionTask!(handle).pipe(
            Effect.tap(() => Effect.yieldNow),
            Effect.repeat({ until: (task) => task.abortRequested }),
            Effect.timeout('5 seconds'),
          )
          assert.strictEqual(returned, 0)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(abort).pipe(Effect.timeout('5 seconds'))
          assert.strictEqual(returned, 1)
          assert.strictEqual((yield* runtime.backend.waitCompaction!(handle)).status, 'aborted')
          assert.strictEqual(
            (yield* entries(runtime)).filter((entry) => entry.kind === 'compaction').length,
            0,
          )
          assert.strictEqual(
            Option.getOrThrow(
              yield* Session.snapshot(
                runtime.session,
                CompactionStatus.document,
                CompactionStatus.target(root),
              ),
            ).value.items.length,
            0,
          )
          const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
          assert.strictEqual(receipts.length, 1)
          assert.strictEqual(receipts[0]?.outcome._tag, 'interrupted')
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'one background compaction survives concurrent generation decisions without clearing its status',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* Settings.make({
              compaction: {
                enabled: true,
                reserveTokens: 20,
                keepRecentTokens: 10,
                backgroundTokens: 60,
              },
            })
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            let summaries = 0
            let generations = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  if (Option.isSome(yield* Effect.serviceOption(CompactionRequest))) {
                    summaries++
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                    return answer('Background summary')
                  }
                  generations++
                  return answer('Foreground continues')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) => Effect.succeed(context),
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability, 140)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const first = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'first' },
            })
            yield* runtime.backend.wait(first.id).pipe(Effect.timeout('5 seconds'))
            yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
            const second = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'second' },
            })
            yield* runtime.backend.wait(second.id).pipe(Effect.timeout('5 seconds'))
            const tasks = yield* Session.scanTasks(runtime.session, {
              kind: CompactionTask.kind,
            }).pipe(Stream.runCollect)
            assert.strictEqual(tasks.length, 1)
            assert.strictEqual(tasks[0]?.background, true)
            assert.isUndefined(tasks[0]?.owner)
            const status = yield* Session.snapshot(
              runtime.session,
              CompactionStatus.document,
              CompactionStatus.target(root),
            )
            assert.strictEqual(Option.getOrThrow(status).value.items.length, 1)
            assert.strictEqual(generations, 2)
            assert.strictEqual(summaries, 1)
            yield* Deferred.succeed(release, undefined)
            const outcome = yield* runtime.backend.waitCompaction!({
              id: tasks[0]!.id,
              conversationId: root,
            }).pipe(Effect.timeout('5 seconds'))
            if (outcome.status !== 'completed' || outcome.result._tag !== 'Queued')
              return assert.fail(JSON.stringify(outcome))
            assert.strictEqual(
              (yield* runtime.backend.wait(outcome.result.submissionId)).status,
              'done',
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.effect(
      'saved finite summary retry deadline survives cold reopen and preserves its pinned configuration',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const settings = yield* Settings.make({
              compaction: { enabled: false, reserveTokens: 20, keepRecentTokens: 10 },
              retry: { maxRetries: 1, baseDelayMs: 100, maxAgentDelayMs: 250 },
            })
            const rate = AiError.make({
              module: 'fixture',
              method: 'summary',
              reason: new AiError.RateLimitError({
                retryAfter: Duration.millis(250),
                metadata: { usage: { promptTokens: 3, completionTokens: 1 } },
              }),
            })
            let calls = 0
            let configurations = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  calls++
                  if (calls === 1) return yield* rate
                  const request = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
                  assert.strictEqual(request.attempt, 1)
                  assert.strictEqual(request.maxOutputTokens, 9)
                  return answer('Retry summary')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context) =>
                Effect.sync(() => {
                  configurations++
                  return context
                }),
            })
            const open = Effect.gen(function* () {
              const storage = yield* store.open(backend)
              const runtime = yield* HarnessRuntime.make({
                models: [descriptor(native, capability)],
              }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(Settings.Settings, settings),
              )
              yield* runtime.backend.root
              return runtime
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* seed(first)
            const now = yield* Clock.currentTimeMillis
            const handle = yield* first.backend.compact!(root)
            yield* Session.watch(
              first.session,
              CompactionStatus.document,
              CompactionStatus.target(root),
            ).pipe(
              Stream.filter((snapshot) =>
                snapshot.value.items.some((item) => item.phase === 'retry'),
              ),
              Stream.take(1),
              Stream.runDrain,
            )
            const checkpoint = yield* Schema.decodeUnknownEffect(
              Schema.toCodecJson(CompactionTask.CheckpointSchema),
            )((yield* first.backend.compactionTask!(handle)).state.checkpoint)
            if (checkpoint.phase !== 'retry')
              return assert.fail('Expected durable retry checkpoint')
            assert.strictEqual(checkpoint.until, now + 250)
            yield* Scope.close(firstScope, Exit.void)
            yield* settings.update({
              retry: { enabled: false },
              compaction: { reserveTokens: 100 },
            })
            const second = yield* open
            const waiter = yield* second.backend.waitCompaction!(handle).pipe(Effect.forkScoped)
            yield* TestClock.adjust(249)
            assert.strictEqual(calls, 1)
            yield* TestClock.adjust(1)
            const outcome = yield* Fiber.join(waiter)
            assert.strictEqual(outcome.status, 'completed')
            assert.strictEqual(calls, 2)
            assert.strictEqual(configurations, 1)
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            assert.deepEqual(
              receipts.map((item) =>
                item.identity._tag === 'requestAttempt' ? item.identity.attempt : -1,
              ),
              [0, 1],
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'actual deferred create overflow compacts once using nondeferred summary execution',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* Settings.make({
              stream: { deferred: true },
              compaction: { enabled: true, reserveTokens: 20, keepRecentTokens: 10 },
            })
            let starts = 0
            let summaries = 0
            const overflow = AiError.make({
              module: 'fixture',
              method: 'deferredStart',
              reason: new AiError.InvalidRequestError({
                metadata: { fixture: { code: 'context_length' } },
              }),
            })
            const deferred = yield* Model.makeDeferred<Schema.String, never>({
              handle: Schema.String,
              start: () =>
                Effect.gen(function* () {
                  starts++
                  if (starts === 1) return yield* overflow
                  return {
                    _tag: 'Complete' as const,
                    response: new LanguageModel.GenerateTextResponse([
                      Response.makePart('text', { text: 'Deferred result' }),
                    ]),
                  }
                }),
              poll: () => Effect.die('No deferred handle was admitted'),
              cancel: () => Effect.void,
            })
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  const policy = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
                  assert.strictEqual(policy.deferred, false)
                  summaries++
                  return answer('Deferred overflow summary')
                }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/summary', version: 1 },
              configure: (context, policy) => {
                assert.strictEqual(policy.settings.stream.deferred, false)
                return Effect.succeed(context)
              },
              isOverflow: (error) =>
                error.reason._tag === 'InvalidRequestError' &&
                Schema.is(Schema.Struct({ code: Schema.Literal('context_length') }))(
                  error.reason.metadata.fixture,
                ),
            })
            const runtime = yield* HarnessRuntime.make({
              models: [{ ...descriptor(native, capability), deferred }],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'go' },
            })
            const outcome = yield* runtime.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
            assert.strictEqual(outcome.status, 'done', JSON.stringify(outcome))
            assert.strictEqual(starts, 2)
            assert.strictEqual(summaries, 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'captured summary and codec services remain honest while new resources use the invocation Scope',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            class Owner extends Context.Service<Owner, { readonly value: string }>()(
              'test/compaction/resource-owner',
            ) {}
            class Config extends Context.Service<Config, { readonly value: string }>()(
              'test/compaction/resource-config',
            ) {}
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            const events: string[] = []
            const factoryScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(factoryScope, Exit.void))
            const codec = yield* Model.makeConfiguration({
              id: 'fixture/resource-config',
              version: 1,
              schema: Schema.Struct({ value: Schema.String }),
              tags: [Config],
              capture: (context) =>
                Effect.succeed(Option.getOrThrow(Context.getOption(context, Config))),
              restore: (value) =>
                Effect.gen(function* () {
                  assert.strictEqual((yield* Owner).value, 'captured')
                  yield* Effect.acquireRelease(
                    Effect.sync(() => {
                      events.push('restore.acquire')
                      return value
                    }),
                    () =>
                      Effect.sync(() => {
                        events.push('restore.release')
                      }),
                  )
                  return Context.make(Config, value)
                }),
            }).pipe(
              Effect.provideService(Owner, { value: 'captured' }),
              Effect.provideService(Scope.Scope, factoryScope),
            )
            const capability = yield* Model.makeCompaction({
              binding: { id: 'fixture/resources', version: 1 },
              configuration: codec,
              configure: () =>
                Effect.gen(function* () {
                  assert.strictEqual((yield* Owner).value, 'captured')
                  yield* Effect.acquireRelease(
                    Effect.sync(() => {
                      events.push('configure.acquire')
                    }),
                    () =>
                      Effect.sync(() => {
                        events.push('configure.release')
                      }),
                  )
                  return Context.make(Config, { value: 'saved' })
                }),
            }).pipe(
              Effect.provideService(Owner, { value: 'captured' }),
              Effect.provideService(Scope.Scope, factoryScope),
            )
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.gen(function* () {
                  assert.strictEqual(
                    Option.getOrThrow(yield* Effect.serviceOption(Config)).value,
                    'saved',
                  )
                  return answer('Scoped summary')
                }),
            })
            const runtimeScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void))
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
              Effect.provideService(Owner, { value: 'ambient' }),
              Effect.provideService(Scope.Scope, runtimeScope),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const handle = yield* runtime.backend.compact!(root)
            const outcome = yield* runtime.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
            )
            assert.strictEqual(outcome.status, 'completed', JSON.stringify(outcome))
            yield* Scope.close(runtimeScope, Exit.void)
            assert.deepEqual(events, [
              'configure.acquire',
              'configure.release',
              'restore.acquire',
              'restore.release',
            ])
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    for (const mode of ['invalidSummary', 'bindingChanged', 'staleCold'] as const)
      it.live(
        `${mode} preserves observed accounting and prevents invalid placement or restore effects`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* fixture
              const settings = yield* policy()
              const entered = yield* Deferred.make<void>()
              let calls = 0
              let configurations = 0
              const native = yield* ScriptedStream.make({
                generateText: () =>
                  Effect.gen(function* () {
                    calls++
                    yield* Deferred.succeed(entered, undefined)
                    if (mode !== 'invalidSummary') return yield* Effect.never
                    return answer('   ')
                  }),
              })
              const capability = yield* Model.makeCompaction({
                binding: { id: 'fixture/fence', version: 1 },
                configure: (context) =>
                  Effect.sync(() => {
                    configurations++
                    return context
                  }),
              })
              const firstScope = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
              const firstStorage = yield* store
                .open(backend)
                .pipe(Effect.provideService(Scope.Scope, firstScope))
              const first = yield* HarnessRuntime.make({
                models: [descriptor(native, capability)],
              }).pipe(
                Effect.provideContext(firstStorage),
                Effect.provideService(Settings.Settings, settings),
                Effect.provideService(Scope.Scope, firstScope),
              )
              yield* first.backend.root
              yield* seed(first)
              const selected = (yield* entries(first))[0]!
              const handle = yield* first.backend.compact!(root)
              yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
              if (mode === 'invalidSummary') {
                const outcome = yield* first.backend.waitCompaction!(handle)
                assert.strictEqual(outcome.status, 'failed')
                const receipts = yield* Session.commit(first.session, (tx) =>
                  Usage.receipts(tx, root),
                )
                assert.strictEqual(receipts.length, 1)
                assert.strictEqual(receipts[0]?.usage?.tokens?.output, 3)
              } else {
                yield* Scope.close(firstScope, Exit.void)
                const storage = yield* store.open(backend)
                if (mode === 'staleCold') {
                  const editScope = yield* Scope.make()
                  const session = yield* Session.make().pipe(
                    Effect.provideContext(storage),
                    Effect.provideService(Scope.Scope, editScope),
                  )
                  yield* Session.commit(session, (tx) =>
                    Transaction.appendEntry(tx, root, {
                      kind: 'edit',
                      edits: [
                        {
                          _tag: 'replace',
                          target: selected.id,
                          messages: [{ role: 'user', content: 'Changed while closed' }],
                        },
                      ],
                    }),
                  )
                  yield* Scope.close(editScope, Exit.void)
                }
                const second = yield* HarnessRuntime.make({
                  models: [
                    descriptor(
                      native,
                      mode === 'bindingChanged'
                        ? { ...capability, binding: { ...capability.binding, version: 2 } }
                        : capability,
                    ),
                  ],
                }).pipe(
                  Effect.provideContext(storage),
                  Effect.provideService(Settings.Settings, settings),
                )
                const outcome = yield* second.backend.waitCompaction!(handle).pipe(
                  Effect.timeout('5 seconds'),
                )
                assert.strictEqual(outcome.status, 'failed', JSON.stringify(outcome))
                assert.strictEqual(
                  (yield* entries(second)).filter((item) => item.kind === 'compaction').length,
                  0,
                )
                const receipts = yield* Session.commit(second.session, (tx) =>
                  Usage.receipts(tx, root),
                )
                assert.strictEqual(receipts.length, 1)
                assert.isUndefined(receipts[0]?.usage)
                assert.strictEqual(
                  Option.getOrThrow(
                    yield* Session.snapshot(
                      second.session,
                      CompactionStatus.document,
                      CompactionStatus.target(root),
                    ),
                  ).value.items.length,
                  0,
                )
              }
              assert.strictEqual(calls, 1)
              assert.strictEqual(configurations, 1)
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )

    for (const mode of ['undeclaredConfig', 'invalidBinding'] as const)
      it.live(`${mode} fails preflight without an incompatible format or provider call`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            class Config extends Context.Service<Config, { readonly value: string }>()(
              'test/compaction/undeclared',
            ) {}
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            let calls = 0
            let configurations = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  calls++
                  return answer('Unexpected')
                }),
            })
            const valid = yield* Model.makeCompaction({
              binding: { id: 'fixture/preflight', version: 1 },
              configure: () =>
                Effect.sync(() => {
                  configurations++
                  return Context.make(Config, { value: 'undeclared' })
                }),
            })
            const capability =
              mode === 'invalidBinding'
                ? { ...valid, binding: { ...valid.binding, version: 0 } }
                : valid
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability)],
            }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const before = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            const handle = yield* runtime.backend.compact!(root)
            assert.strictEqual((yield* runtime.backend.waitCompaction!(handle)).status, 'failed')
            assert.deepEqual(
              yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)),
              before,
            )
            assert.strictEqual(calls, 0)
            assert.strictEqual(configurations, mode === 'invalidBinding' ? 0 : 1)
            assert.strictEqual(
              Option.getOrThrow(
                yield* Session.snapshot(
                  runtime.session,
                  CompactionStatus.document,
                  CompactionStatus.target(root),
                ),
              ).value.items.length,
              0,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    it.live(
      'a real cut without explicit capability fails instead of becoming a successful noop',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* policy()
            let calls = 0
            const native = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  calls++
                  return answer('unexpected')
                }),
            })
            const runtime = yield* HarnessRuntime.make({ models: [descriptor(native)] }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const before = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            const handle = yield* runtime.backend.compact!(root)
            const outcome = yield* runtime.backend.waitCompaction!(handle)
            assert.strictEqual(outcome.status, 'failed')
            assert.strictEqual(calls, 0)
            assert.deepEqual(
              yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)),
              before,
            )
            assert.strictEqual(
              (yield* entries(runtime)).filter((item) => item.kind === 'compaction').length,
              0,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  })
}

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  it.live(
    `${backend}: raw native structured overflow preserves metadata without durable credentials and recovers only once after cold reopen`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          const entered = yield* Deferred.make<void>()
          const settings = yield* Settings.make({
            compaction: { enabled: true, reserveTokens: 20, keepRecentTokens: 10 },
            retry: { maxRetries: 5, baseDelayMs: 1 },
          })
          let generations = 0
          let summaries = 0
          const transport = HttpClient.make((request) =>
            Effect.gen(function* () {
              assert.strictEqual(request.headers.authorization, 'Bearer checkpoint-dummy-key')
              if (request.body._tag !== 'Uint8Array')
                return assert.fail('Expected bound native HTTP body')
              const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.JsonObject))(
                new TextDecoder().decode(request.body.body),
              ).pipe(Effect.orDie)
              if (payload.stream === true) {
                generations++
                return HttpClientResponse.fromWeb(
                  request,
                  new globalThis.Response(
                    JSON.stringify({
                      error: {
                        message: 'Structured context overflow',
                        code: 'context_length_exceeded',
                        type: 'invalid_request_error',
                      },
                    }),
                    { status: 400, headers: { 'content-type': 'application/json' } },
                  ),
                )
              }
              summaries++
              assert.strictEqual(payload.max_output_tokens, 16)
              assert.isUndefined(payload.prompt_cache_key)
              assert.isUndefined(payload.tools)
              assert.strictEqual(payload.background, false)
              if (summaries === 1) {
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never
              }
              return HttpClientResponse.fromWeb(
                request,
                new globalThis.Response(
                  JSON.stringify({
                    id: 'raw-response',
                    model: 'fixture',
                    created_at: 0,
                    output: [
                      {
                        type: 'message',
                        id: 'raw-message',
                        role: 'assistant',
                        status: 'completed',
                        content: [
                          {
                            type: 'output_text',
                            text: 'Raw summary',
                            annotations: [],
                            logprobs: [],
                          },
                        ],
                      },
                    ],
                    usage: {
                      input_tokens: 12,
                      output_tokens: 3,
                      total_tokens: 15,
                      input_tokens_details: { cached_tokens: 0 },
                      output_tokens_details: { reasoning_tokens: 0 },
                    },
                  }),
                  { status: 200, headers: { 'content-type': 'application/json' } },
                ),
              )
            }),
          )
          const client = yield* OpenAiClient.make({
            apiKey: Redacted.make('checkpoint-dummy-key'),
            apiUrl: 'https://fixture.invalid',
          }).pipe(Effect.provideService(HttpClient.HttpClient, transport))
          const native = yield* OpenAiLanguageModel.make({ model: 'fixture' }).pipe(
            Effect.provideService(OpenAiClient.OpenAiClient, client),
          )
          const capability = yield* Model.makeCompaction({
            binding: { id: 'fixture/raw-summary', version: 1 },
            configure: (context) => Effect.succeed(context),
            isOverflow: (error) =>
              error.reason._tag === 'InvalidRequestError' &&
              Schema.is(Schema.Struct({ errorCode: Schema.Literal('context_length_exceeded') }))(
                error.reason.metadata.openai,
              ),
          })
          const open = Effect.gen(function* () {
            const storage = yield* store.open(backend)
            const runtime = yield* HarnessRuntime.make({ nativeCompaction: capability }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(LanguageModel.LanguageModel, native),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            return { runtime, storage }
          })
          const firstScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
          yield* seed(first.runtime)
          const job = yield* first.runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
          const check = (storage: Context.Context<Storage.Storage>) =>
            Effect.gen(function* () {
              const tasks = yield* Context.get(storage, Storage.Storage)
                .scanTasks()
                .pipe(Stream.runCollect)
              const json = JSON.stringify(tasks)
              assert.isFalse(json.includes('checkpoint-dummy-key'))
              assert.isTrue(json.includes('context_length_exceeded'))
              const parent = tasks.find((task) => task.kind === 'harness.run')
              if (parent?.state.checkpoint !== undefined) {
                const checkpoint = yield* Schema.decodeUnknownEffect(
                  Schema.Struct({ overflow: Schema.toCodecJson(AiError.AiError) }),
                )(parent.state.checkpoint)
                assert.strictEqual(checkpoint.overflow.reason._tag, 'InvalidRequestError')
                assert.isTrue('http' in checkpoint.overflow.reason)
                if ('http' in checkpoint.overflow.reason)
                  assert.strictEqual(
                    checkpoint.overflow.reason.http?.request.headers.authorization,
                    '<redacted>',
                  )
              }
              const fs = yield* FileSystem.FileSystem
              for (const name of yield* fs.readDirectory(store.directory, { recursive: true })) {
                if ((yield* fs.stat(`${store.directory}/${name}`)).type !== 'File') continue
                const bytes = yield* fs.readFile(`${store.directory}/${name}`)
                assert.isFalse(new TextDecoder().decode(bytes).includes('checkpoint-dummy-key'))
              }
            })
          yield* check(first.storage)
          yield* Scope.close(firstScope, Exit.void)
          const secondScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
          const second = yield* open.pipe(Effect.provideService(Scope.Scope, secondScope))
          const outcome = yield* second.runtime.backend
            .wait(job.id)
            .pipe(Effect.timeout('5 seconds'))
          assert.strictEqual(outcome.status, 'unanswered')
          assert.strictEqual(generations, 2)
          assert.strictEqual(summaries, 2)
          yield* check(second.storage)
          const tasks = yield* Session.scanTasks(second.runtime.session, {
            kind: CompactionTask.kind,
          }).pipe(Stream.runCollect)
          assert.strictEqual(tasks.length, 1)
          yield* Scope.close(secondScope, Exit.void)
          const third = yield* open
          assert.strictEqual((yield* third.runtime.backend.wait(job.id)).status, 'unanswered')
          assert.strictEqual(generations, 2)
          assert.strictEqual(summaries, 2)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

for (const provider of ['openai', 'anthropic'] as const)
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
    it.live(
      `${provider}/${backend}: native final wire enforces pinned summary controls and cold restore preserves client auth and transforms`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const settings = yield* policy()
            const entered = yield* Deferred.make<void>()
            const payloads: Schema.JsonObject[] = []
            const transport = HttpClient.make((request) =>
              Effect.gen(function* () {
                assert.strictEqual(request.headers['x-fixture-transform'], 'captured')
                assert.strictEqual(
                  provider === 'openai'
                    ? request.headers.authorization
                    : request.headers['x-api-key'],
                  provider === 'openai' ? 'Bearer fixture-key' : 'fixture-key',
                )
                assert.isTrue(request.url.startsWith('https://fixture.invalid/'))
                if (request.body._tag !== 'Uint8Array')
                  return assert.fail('Expected encoded native JSON payload')
                const payload = yield* Schema.decodeEffect(
                  Schema.fromJsonString(Schema.JsonObject),
                )(new TextDecoder().decode(request.body.body)).pipe(Effect.orDie)
                payloads.push(payload)
                if (payloads.length === 1) {
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }
                const body =
                  provider === 'openai'
                    ? {
                        id: 'response-fixture',
                        model: 'fixture',
                        created_at: 0,
                        output: [
                          {
                            type: 'message',
                            id: 'message-fixture',
                            role: 'assistant',
                            status: 'completed',
                            content: [
                              {
                                type: 'output_text',
                                text: 'Native summary',
                                annotations: [],
                                logprobs: [],
                              },
                            ],
                          },
                        ],
                        usage: {
                          input_tokens: 12,
                          output_tokens: 3,
                          total_tokens: 15,
                          input_tokens_details: { cached_tokens: 0 },
                          output_tokens_details: { reasoning_tokens: 0 },
                        },
                      }
                    : {
                        id: 'message-fixture',
                        type: 'message',
                        role: 'assistant',
                        model: 'claude-sonnet-4-20250514',
                        content: [{ type: 'text', text: 'Native summary', citations: [] }],
                        stop_reason: 'end_turn',
                        stop_sequence: null,
                        usage: {
                          cache_creation: null,
                          cache_creation_input_tokens: null,
                          cache_read_input_tokens: null,
                          inference_geo: null,
                          input_tokens: 12,
                          output_tokens: 3,
                          service_tier: null,
                        },
                      }
                return HttpClientResponse.fromWeb(
                  request,
                  new globalThis.Response(JSON.stringify(body), {
                    status: 200,
                    headers: { 'content-type': 'application/json' },
                  }),
                )
              }),
            )
            const options = {
              apiKey: Redacted.make('fixture-key'),
              apiUrl: 'https://fixture.invalid',
              transformClient: (client: HttpClient.HttpClient) =>
                client.pipe(
                  HttpClient.mapRequest(
                    HttpClientRequest.setHeader('x-fixture-transform', 'captured'),
                  ),
                ),
            }
            let native: Model.Any['languageModel']
            let configuration: Model.Configuration
            let configured: Context.Context<never>
            if (provider === 'openai') {
              const client = yield* OpenAiClient.make(options).pipe(
                Effect.provideService(HttpClient.HttpClient, transport),
              )
              native = yield* OpenAiLanguageModel.make({
                model: 'fixture',
                config: {
                  prompt_cache_key: 'construction-cache',
                  max_output_tokens: 999,
                  background: true,
                  store: true,
                },
              }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, client))
              const schema = Schema.Struct({
                temperature: Schema.Finite,
                prompt_cache_key: Schema.String,
                max_output_tokens: Schema.Int,
                background: Schema.Boolean,
                store: Schema.Boolean,
              })
              configuration = yield* Model.makeConfiguration({
                id: 'fixture/openai-config',
                version: 1,
                schema,
                tags: [OpenAiLanguageModel.Config],
                capture: (context) =>
                  Schema.decodeUnknownEffect(schema)(
                    Option.getOrThrow(Context.getOption(context, OpenAiLanguageModel.Config)),
                  ),
                restore: (value) => Effect.succeed(Context.make(OpenAiLanguageModel.Config, value)),
              })
              configured = Context.make(OpenAiLanguageModel.Config, {
                temperature: 0.3,
                prompt_cache_key: 'request-cache',
                max_output_tokens: 999,
                background: true,
                store: true,
              })
            } else {
              const client = yield* AnthropicClient.make(options).pipe(
                Effect.provideService(HttpClient.HttpClient, transport),
              )
              native = yield* AnthropicLanguageModel.make({
                model: 'claude-sonnet-4-20250514',
                config: { max_tokens: 999 },
              }).pipe(Effect.provideService(AnthropicClient.AnthropicClient, client))
              const schema = Schema.Struct({
                temperature: Schema.Finite,
                max_tokens: Schema.Int,
                system: Schema.Array(
                  Schema.Struct({
                    type: Schema.Literal('text'),
                    text: Schema.String,
                    cache_control: Schema.Struct({ type: Schema.Literal('ephemeral') }),
                  }),
                ),
              })
              configuration = yield* Model.makeConfiguration({
                id: 'fixture/anthropic-config',
                version: 1,
                schema,
                tags: [AnthropicLanguageModel.Config],
                capture: (context) =>
                  Schema.decodeUnknownEffect(schema)(
                    Option.getOrThrow(Context.getOption(context, AnthropicLanguageModel.Config)),
                  ),
                restore: (value) =>
                  Effect.succeed(Context.make(AnthropicLanguageModel.Config, value)),
              })
              configured = Context.make(AnthropicLanguageModel.Config, {
                temperature: 0.3,
                max_tokens: 999,
                system: [
                  {
                    type: 'text',
                    text: 'Summarize the supplied conversation',
                    cache_control: { type: 'ephemeral' },
                  },
                ],
              })
            }
            let configurations = 0
            const capability = yield* Model.makeCompaction({
              binding: { id: `fixture/${provider}-summary`, version: 1 },
              configuration,
              configure: () =>
                Effect.sync(() => {
                  configurations++
                  return configured
                }),
            })
            const open = Effect.gen(function* () {
              const storage = yield* store.open(backend)
              const runtime = yield* HarnessRuntime.make({
                models: [descriptor(native, capability)],
              }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(Settings.Settings, settings),
              )
              yield* runtime.backend.root
              return runtime
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* seed(first)
            const handle = yield* first.backend.compact!(root)
            yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
            yield* settings.update({ compaction: { reserveTokens: 100 } })
            yield* Scope.close(firstScope, Exit.void)
            const second = yield* open
            const outcome = yield* second.backend.waitCompaction!(handle).pipe(
              Effect.timeout('5 seconds'),
            )
            if (outcome.status !== 'completed' || outcome.result._tag !== 'Queued')
              return assert.fail(JSON.stringify(outcome))
            assert.strictEqual(
              (yield* second.backend.wait(outcome.result.submissionId)).status,
              'done',
            )
            assert.strictEqual(configurations, 1)
            assert.strictEqual(payloads.length, 2)
            assert.deepEqual(payloads[1], payloads[0])
            for (const payload of payloads) {
              assert.strictEqual(
                payload[provider === 'openai' ? 'max_output_tokens' : 'max_tokens'],
                9,
              )
              assert.strictEqual(payload.temperature, 0.3)
              assert.isUndefined(payload.tools)
              assert.isUndefined(payload.tool_choice)
              if (provider === 'openai') {
                assert.isUndefined(payload.prompt_cache_key)
                assert.isUndefined(payload.prompt_cache_retention)
                assert.strictEqual(payload.background, false)
                assert.strictEqual(payload.store, false)
              } else assert.isFalse(JSON.stringify(payload).includes('cache_control'))
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }

// The charge belongs to the failed generation, before summary preflight starts.
for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} overflow preflight accounting`, () => {
    for (const mode of ['empty', 'codec', 'typed', 'mixed', 'unknown', 'decline'] as const)
      it.live(`${mode} retains original attempt and pricing once across cold reopen`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* Settings.make({
              compaction: {
                enabled: true,
                reserveTokens: 20,
                keepRecentTokens: 10,
                backgroundTokens: 0,
              },
              retry: { maxRetries: 5, baseDelayMs: 0 },
            })
            let generations = 0
            let summaries = 0
            const overflow = AiError.make({
              module: 'ReceiptRegression',
              method: 'streamText',
              reason: new AiError.InvalidRequestError({
                description: 'Original observed generation failure',
                metadata: {
                  fixture: { code: 'overflow' },
                  ...(mode === 'unknown'
                    ? {}
                    : { usage: { promptTokens: 31, completionTokens: 7, totalTokens: 38 } }),
                },
              }),
            })
            class Undeclared extends Context.Service<Undeclared, { readonly value: true }>()(
              'receipt/undeclared',
            ) {}
            const failure = new ModelError({
              reason: new ModelUnsupportedError({ message: 'typed-preflight-sentinel' }),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'receipt/summary', version: 1 },
              isOverflow: (e) =>
                e.reason._tag === 'InvalidRequestError' &&
                Schema.is(Schema.Struct({ code: Schema.Literal('overflow') }))(
                  e.reason.metadata.fixture,
                ),
              configure: (context) => {
                if (mode === 'codec')
                  return Effect.succeed(Context.make(Undeclared, { value: true }))
                if (mode === 'typed') return Effect.fail(failure)
                if (mode === 'mixed')
                  return Effect.failCause(
                    Cause.combine(Cause.fail(failure), Cause.die('mixed-preflight-sentinel')),
                  )
                return Effect.succeed(context)
              },
            })
            const native = yield* LanguageModel.make({
              generateText: () =>
                Effect.sync(() => {
                  summaries++
                  return answer('unexpected summary')
                }),
              streamText: () =>
                Stream.unwrap(
                  Effect.sync(() => {
                    generations++
                    return Stream.fail(overflow)
                  }),
                ),
            })
            const base = descriptor(native, capability)
            const pricing = {
              inputToken: 2,
              outputToken: 3,
              currency: 'fixture',
              source: 'receipt-price-v1',
            }
            const models = [Model.make({ ...base, definition: { ...base.definition, pricing } })]
            const extensions =
              mode === 'empty' || mode === 'unknown' || mode === 'decline'
                ? [
                    Extension.make({
                      name: 'receipt-hook',
                      hooks: [
                        Hook.make({
                          event: 'beforeCompact',
                          execute: () =>
                            Effect.succeed(
                              mode === 'decline'
                                ? { _tag: 'decline' }
                                : { _tag: 'summary', summary: '   ' },
                            ),
                        }),
                      ],
                    }),
                  ]
                : []
            const owner = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
            const first = yield* HarnessRuntime.make({ models, extensions }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
              Effect.provideService(Scope.Scope, owner),
            )
            yield* first.backend.root
            yield* seed(first)
            const format = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'continue' },
            })
            const outcome = yield* first.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
            assert.strictEqual(outcome.status, 'unanswered')
            const receipts = yield* Session.commit(first.session, (tx) => Usage.receipts(tx, root))
            assert.strictEqual(receipts.length, 1)
            const receipt = receipts[0]!
            assert.strictEqual(receipt.identity._tag, 'requestAttempt')
            if (receipt.identity._tag === 'requestAttempt') {
              assert.strictEqual(receipt.identity.purpose, 'generation')
              assert.strictEqual(receipt.identity.attempt, 0)
            }
            assert.strictEqual(receipt.outcome._tag, 'failed')
            if (mode === 'unknown') assert.isUndefined(receipt.usage)
            else {
              assert.strictEqual(receipt.usage?._tag, 'model')
              if (receipt.usage?._tag === 'model') {
                assert.strictEqual(receipt.usage.tokens.input, 31)
                assert.strictEqual(receipt.usage.tokens.output, 7)
                assert.deepEqual(receipt.usage.pricing, pricing)
                assert.strictEqual(receipt.usage.cost?.amount, 83)
              }
            }
            const tasks = yield* Session.scanTasks(first.session, { kind: 'harness.run' }).pipe(
              Stream.runCollect,
            )
            if (mode === 'typed' || mode === 'mixed') {
              const text = JSON.stringify(tasks)
              assert.include(text, 'typed-preflight-sentinel')
              if (mode === 'mixed') assert.include(text, 'mixed-preflight-sentinel')
            }
            // Generation activates its existing reader, never the compaction barrier.
            const after = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            assert.strictEqual(
              after.minimumReaderVersion,
              Math.max(format.minimumReaderVersion, 12),
            )
            assert.strictEqual(
              (yield* entries(first)).filter(
                (e) => e.kind.startsWith('assistant') || e.kind === 'compaction',
              ).length,
              0,
            )
            yield* Scope.close(owner, Exit.void)
            const clean = yield* store.open(backend)
            const second = yield* HarnessRuntime.make({ models, extensions }).pipe(
              Effect.provideContext(clean),
              Effect.provideService(Settings.Settings, settings),
            )
            assert.deepEqual(yield* second.backend.wait(job.id), outcome)
            assert.deepEqual(
              yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root)),
              receipts,
            )
            assert.strictEqual(generations, 1)
            assert.strictEqual(summaries, 0)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )

    for (const mode of ['close', 'rejected', 'uncertain'] as const)
      it.live(
        `${mode} at original receipt or preflight preserves causal accounting and cold no-replay`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* fixture
              const storageOwner = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(storageOwner, Exit.void))
              const storage = yield* store
                .open(backend)
                .pipe(Effect.provideService(Scope.Scope, storageOwner))
              const real = Context.get(storage, Storage.Storage)
              const format = Context.get(storage, StorageFormat.StorageFormat)
              let faults = 0
              const entered = yield* Deferred.make<void>()
              let generations = 0
              let summaries = 0
              const overflow = AiError.make({
                module: 'ReceiptRegression',
                method: 'streamText',
                reason: new AiError.InvalidRequestError({
                  description: 'Original observed generation failure',
                  metadata: {
                    fixture: { code: 'overflow' },
                    usage: { promptTokens: 31, completionTokens: 7, totalTokens: 38 },
                  },
                }),
              })
              const native = yield* LanguageModel.make({
                generateText: () =>
                  Effect.sync(() => {
                    summaries++
                    return answer('unexpected summary')
                  }),
                streamText: () =>
                  Stream.unwrap(
                    Effect.sync(() => {
                      generations++
                      return Stream.fail(overflow)
                    }),
                  ),
              })
              const capability = yield* Model.makeCompaction({
                binding: { id: 'receipt/summary', version: 1 },
                isOverflow: (e) =>
                  e.reason._tag === 'InvalidRequestError' &&
                  Schema.is(Schema.Struct({ code: Schema.Literal('overflow') }))(
                    e.reason.metadata.fixture,
                  ),
                configure: (context) =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(mode === 'close' ? Effect.never : Effect.succeed(context)),
                  ),
              })
              const models = [descriptor(native, capability)]
              const settings = yield* Settings.make({
                compaction: {
                  enabled: true,
                  reserveTokens: 20,
                  keepRecentTokens: 10,
                  backgroundTokens: 0,
                },
                retry: { maxRetries: 5 },
              })
              const wrapped = Storage.Storage.of({
                ...real,
                commit: (input) =>
                  Effect.suspend(() => {
                    const writes = Array.from(input)
                    const receipt = writes.some(
                      (w) =>
                        (w._tag === 'document.create' || w._tag === 'document.change') &&
                        JSON.stringify(w).includes('Original observed generation failure') &&
                        JSON.stringify(w).includes('requestAttempt'),
                    )
                    if (mode !== 'close' && receipt && faults === 0) {
                      faults++
                      const error = new StorageError({
                        reason: mode === 'rejected' ? 'conflict' : 'uncertain',
                        operation: 'receipt-regression',
                        message: mode,
                      })
                      return mode === 'rejected'
                        ? Effect.fail(error)
                        : real.commit(writes).pipe(Effect.andThen(Effect.fail(error)))
                    }
                    return real.commit(writes)
                  }),
              })
              const owner = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
              const first = yield* HarnessRuntime.make({ models }).pipe(
                Effect.provideContext(
                  storage.pipe(
                    Context.add(Storage.Storage, wrapped),
                    Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
                  ),
                ),
                Effect.provideService(Settings.Settings, settings),
                Effect.provideService(Scope.Scope, owner),
              )
              yield* first.backend.root
              yield* seed(first)
              const job = yield* first.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'continue' },
              })
              if (mode === 'close') {
                yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
                const receipts = yield* Session.commit(first.session, (tx) =>
                  Usage.receipts(tx, root),
                )
                assert.strictEqual(receipts.length, 1)
                assert.strictEqual(receipts[0]?.usage?._tag, 'model')
              } else {
                const outcome = yield* Effect.exit(
                  first.backend.wait(job.id).pipe(Effect.timeout('5 seconds')),
                )
                assert.strictEqual(faults, 1)
                assert.strictEqual(Exit.isSuccess(outcome), mode === 'rejected')
                if (Exit.isFailure(outcome))
                  assert.include(Cause.pretty(outcome.cause), 'uncertain')
                assert.isFalse(yield* Deferred.isDone(entered))
              }
              yield* Scope.close(owner, Exit.void)
              const clean = yield* store.open(backend)
              const second = yield* HarnessRuntime.make({ models }).pipe(
                Effect.provideContext(clean),
                Effect.provideService(Settings.Settings, settings),
              )
              const outcome = yield* second.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
              assert.strictEqual(outcome.status, 'unanswered')
              const receipts = yield* Session.commit(second.session, (tx) =>
                Usage.receipts(tx, root),
              )
              assert.strictEqual(receipts.length, 1)
              assert.strictEqual(receipts[0]?.identity._tag, 'requestAttempt')
              assert.strictEqual(receipts[0]?.outcome._tag, 'failed')
              if (receipts[0]?.usage?._tag !== 'model') return assert.fail(JSON.stringify(receipts))
              assert.strictEqual(receipts[0].usage.tokens.input, 31)
              assert.strictEqual(receipts[0].usage.tokens.output, 7)
              assert.strictEqual(generations, 1)
              assert.strictEqual(summaries, 0)
              assert.strictEqual(
                (yield* entries(second)).filter(
                  (e) => e.kind.startsWith('assistant') || e.kind === 'compaction',
                ).length,
                0,
              )
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )
  })
}

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  for (const evidence of [
    'absent',
    'other-request',
    'other-attempt',
    'summary',
    'completed',
  ] as const)
    it.live(`${backend} overflow receipt recovery ignores ${evidence} evidence`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          const storage = yield* store.open(backend)
          const entered = yield* Deferred.make<void>()
          let calls = 0
          const native = yield* ScriptedStream.make({
            generateText: () =>
              Effect.gen(function* () {
                calls++
                if (calls === 1) {
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }
                return answer('Ordinary interrupted recovery')
              }),
          })
          const models = [descriptor(native)]
          const owner = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
          const first = yield* HarnessRuntime.make({ models }).pipe(
            Effect.provideContext(storage),
            Effect.provideService(Scope.Scope, owner),
          )
          yield* first.backend.root
          const job = yield* first.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'ordinary' },
          })
          yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
          const records = yield* Session.scanTasks(first.session, { kind: 'harness.run' }).pipe(
            Stream.runCollect,
          )
          const checkpoint = yield* Schema.decodeUnknownEffect(
            Schema.TaggedStruct('streaming', {
              preparation: Schema.Struct({
                requestId: Usage.ReceiptIdentitySchema.members[0].fields.requestId,
              }),
              attempt: Usage.AttemptId,
            }),
          )(records[0]?.state.checkpoint)
          if (evidence !== 'absent')
            yield* Session.commit(first.session, (tx) =>
              Usage.record(tx, {
                conversationId: root,
                identity: {
                  _tag: 'requestAttempt',
                  requestId:
                    evidence === 'other-request'
                      ? Usage.ReceiptIdentitySchema.members[0].fields.requestId.make(
                          'other-request',
                        )
                      : checkpoint.preparation.requestId,
                  attempt:
                    evidence === 'other-attempt'
                      ? Usage.AttemptId.make(checkpoint.attempt + 9)
                      : checkpoint.attempt,
                  purpose: evidence === 'summary' ? 'compaction' : 'generation',
                },
                outcome:
                  evidence === 'completed'
                    ? { _tag: 'completed', value: 'other-outcome' }
                    : { _tag: 'failed', error: { message: 'unrelated-receipt-sentinel' } },
              }),
            )
          yield* Scope.close(owner, Exit.void)
          const clean = yield* store.open(backend)
          const second = yield* HarnessRuntime.make({ models }).pipe(Effect.provideContext(clean))
          const outcome = yield* Effect.exit(
            second.backend.wait(job.id).pipe(Effect.timeout('5 seconds')),
          )
          if (evidence === 'completed') {
            // Immutable conflicting observations still fail; a completed receipt is not
            // reclassified as a failed generation or allowed to waive the old barrier.
            assert.isTrue(Exit.isFailure(outcome))
            if (Exit.isFailure(outcome))
              assert.include(
                Cause.pretty(outcome.cause),
                'Accounting identity already has a different payload',
              )
            assert.strictEqual(calls, 1)
          } else {
            assert.isTrue(Exit.isSuccess(outcome))
            if (Exit.isSuccess(outcome)) assert.strictEqual(outcome.value.status, 'done')
            assert.strictEqual(calls, 2)
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            const original = receipts.find(
              (r) =>
                r.identity._tag === 'requestAttempt' &&
                r.identity.requestId === checkpoint.preparation.requestId &&
                r.identity.attempt === checkpoint.attempt &&
                r.identity.purpose === 'generation',
            )
            assert.strictEqual(original?.outcome._tag, 'interrupted')
            assert.isUndefined(original?.usage)
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  for (const fault of ['corrupt', 'closed', 'mixed', 'settlement'] as const)
    it.live(
      `${backend} overflow receipt ${fault} retains native health and the accepted checkpoint`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const real = Context.get(storage, Storage.Storage)
            const format = Context.get(storage, StorageFormat.StorageFormat)
            let failures = 0
            let preflights = 0
            let calls = 0
            const overflow = AiError.make({
              module: 'ReceiptRegression',
              method: 'streamText',
              reason: new AiError.InvalidRequestError({
                description: 'Original observed generation failure',
                metadata: {
                  fixture: { code: 'overflow' },
                  usage: { promptTokens: 31, completionTokens: 7, totalTokens: 38 },
                },
              }),
            })
            const native = yield* LanguageModel.make({
              generateText: () => Effect.succeed(answer('unexpected')),
              streamText: () =>
                Stream.unwrap(
                  Effect.sync(() => {
                    calls++
                    return Stream.fail(overflow)
                  }),
                ),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'receipt/summary', version: 1 },
              isOverflow: (e) =>
                e.reason._tag === 'InvalidRequestError' &&
                Schema.is(Schema.Struct({ code: Schema.Literal('overflow') }))(
                  e.reason.metadata.fixture,
                ),
              configure: (context) =>
                Effect.sync(() => {
                  preflights++
                  return context
                }),
            })
            const wrapped = Storage.Storage.of({
              ...real,
              commit: (input) =>
                Effect.suspend(() => {
                  const writes = Array.from(input)
                  const receipt = writes.some(
                    (w) =>
                      (w._tag === 'document.create' || w._tag === 'document.change') &&
                      JSON.stringify(w).includes('requestAttempt') &&
                      JSON.stringify(w).includes('Original observed generation failure'),
                  )
                  const settlement =
                    failures === 1 &&
                    fault === 'settlement' &&
                    writes.some((w) => w._tag === 'submission')
                  if (receipt || settlement) {
                    failures++
                    let reason: StorageError['reason'] = 'io'
                    if (fault === 'closed') reason = 'closed'
                    if (fault === 'corrupt') reason = 'corrupt'
                    const error = new StorageError({
                      reason,
                      operation: 'receipt-regression',
                      message: `receipt-${fault}-sentinel`,
                    })
                    return fault === 'mixed'
                      ? Effect.failCause(
                          Cause.combine(Cause.fail(error), Cause.die('receipt-mixed-defect')),
                        )
                      : Effect.fail(error)
                  }
                  return real.commit(writes)
                }),
            })
            const settings = yield* Settings.make({
              compaction: {
                enabled: true,
                reserveTokens: 20,
                keepRecentTokens: 10,
                backgroundTokens: 0,
              },
            })
            const runtime = yield* HarnessRuntime.make({
              models: [descriptor(native, capability)],
            }).pipe(
              Effect.provideContext(
                storage.pipe(
                  Context.add(Storage.Storage, wrapped),
                  Context.add(StorageFormat.StorageFormat, { ...format, storage: wrapped }),
                ),
              ),
              Effect.provideService(Settings.Settings, settings),
            )
            yield* runtime.backend.root
            yield* seed(runtime)
            const mixedProof = yield* Deferred.make<string>()
            if (fault === 'mixed') {
              const pull = yield* Stream.toPull(runtime.scheduler.changes)
              yield* Effect.gen(function* () {
                while (true) {
                  yield* pull
                  const health = yield* runtime.scheduler.failure
                  if (Option.isSome(health)) {
                    const text = Cause.pretty(Cause.fail(health.value))
                    if (text.includes('receipt-mixed-defect')) {
                      yield* Deferred.succeed(mixedProof, text)
                      return
                    }
                  }
                }
              }).pipe(Effect.forkChild)
            }
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'continue' },
            })
            const outcome = yield* Effect.exit(
              runtime.backend.wait(job.id).pipe(Effect.timeout('5 seconds')),
            )
            assert.isTrue(Exit.isFailure(outcome))
            if (Exit.isFailure(outcome)) {
              const cause = Cause.pretty(outcome.cause)
              assert.include(cause, `receipt-${fault}-sentinel`)
              if (fault === 'mixed') {
                // The public observer wakes on Session's first poison error. The
                // invocation remains a mixed failure, never a deterministic receipt
                // rejection eligible for settlement through a still-open Session.
                const poisoned = yield* Effect.exit(
                  Session.commit(runtime.session, () => Effect.void),
                )
                assert.isTrue(Exit.isFailure(poisoned))
                const health = yield* Deferred.await(mixedProof).pipe(Effect.timeout('5 seconds'))
                assert.include(health, 'receipt-mixed-sentinel')
                assert.include(health, 'receipt-mixed-defect')
              }
            }
            const records = yield* real.scanTasks({ kind: 'harness.run' }).pipe(Stream.runCollect)
            const checkpoint = yield* Schema.decodeUnknownEffect(
              Schema.TaggedStruct('streaming', { attempt: Usage.AttemptId }),
            )(records[0]?.state.checkpoint)
            assert.strictEqual(checkpoint.attempt, 0)
            assert.strictEqual(preflights, 0)
            assert.strictEqual(calls, 1)
            const documents = yield* real
              .scanDocuments({
                scope: { _tag: 'conversation', conversationId: root },
                kind: 'harness.usage.receipt',
              })
              .pipe(Stream.runCollect)
            assert.strictEqual(documents.length, 0)
            assert.strictEqual(failures, fault === 'settlement' ? 2 : 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} critical hook causes`, () => {
    for (const mode of [
      'defect',
      'multiple',
      'ordinary-first',
      'ordinary-last',
      'interrupt',
      'single',
      'ordinary-only',
      'defect-only',
    ] as const)
      it.live(`${mode} preserves native failure evidence and original accounting`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const settings = yield* Settings.make({
              compaction: {
                enabled: true,
                reserveTokens: 20,
                keepRecentTokens: 10,
                backgroundTokens: 0,
              },
            })
            let generations = 0
            let summaries = 0
            let hooks = 0
            let laterHooks = 0
            let prepared: Prepared.Snapshot | undefined
            const overflow = AiError.make({
              module: 'CriticalHook',
              method: 'streamText',
              reason: new AiError.InvalidRequestError({
                description: 'critical-hook-overflow',
                metadata: {
                  fixture: { code: 'overflow' },
                  usage: { promptTokens: 31, completionTokens: 7, totalTokens: 38 },
                },
              }),
            })
            const native = yield* LanguageModel.make({
              generateText: () =>
                Effect.sync(() => {
                  summaries++
                  return answer('unexpected')
                }),
              streamText: () =>
                Stream.unwrap(
                  Effect.gen(function* () {
                    generations++
                    prepared = Option.getOrThrow(
                      yield* Effect.serviceOption(Prepared.PreparedRequest),
                    ).current
                    return Stream.fail(overflow)
                  }),
                ),
            })
            const capability = yield* Model.makeCompaction({
              binding: { id: 'critical/summary', version: 1 },
              configure: (c) => Effect.succeed(c),
              isOverflow: (e) =>
                e.reason._tag === 'InvalidRequestError' &&
                Schema.is(Schema.Struct({ code: Schema.Literal('overflow') }))(
                  e.reason.metadata.fixture,
                ),
            })
            const base = descriptor(native, capability)
            const pricing = {
              inputToken: 2,
              outputToken: 3,
              currency: 'fixture',
              source: 'critical-hook-price',
            }
            const model = Model.make({ ...base, definition: { ...base.definition, pricing } })
            const typed = Cause.fail(
              new StorageError({
                reason: 'invalid',
                operation: 'critical-hook',
                message: 'critical-hook-typed',
              }),
            )
            const sibling = Cause.fail(
              new StorageError({
                reason: 'io',
                operation: 'critical-hook-sibling',
                message: 'critical-hook-second',
              }),
            )
            const ordinary = Cause.fail('critical-hook-ordinary')
            const causes = {
              defect: Cause.combine(typed, Cause.die('critical-hook-defect')),
              multiple: Cause.combine(typed, sibling),
              'ordinary-first': Cause.combine(ordinary, typed),
              'ordinary-last': Cause.combine(typed, ordinary),
              interrupt: Cause.combine(typed, Cause.interrupt()),
              single: typed,
              'ordinary-only': ordinary,
              'defect-only': Cause.die('critical-hook-defect'),
            }
            let first: HarnessRuntime.HarnessRuntimeService
            const extensions = [
              Extension.make({
                name: 'critical-hook',
                hooks: [
                  Hook.make({
                    event: 'beforeCompact',
                    execute: () =>
                      Effect.gen(function* () {
                        hooks++
                        const receipts = yield* Session.commit(first.session, (tx) =>
                          Usage.receipts(tx, root),
                        )
                        assert.strictEqual(receipts.length, 1)
                        assert.deepEqual(receipts[0]!.identity, {
                          _tag: 'requestAttempt',
                          requestId: prepared!.requestId,
                          attempt: prepared!.attempt,
                          purpose: 'generation',
                        })
                        assert.strictEqual(receipts[0]!.usage?._tag, 'model')
                        if (receipts[0]!.usage?._tag === 'model')
                          assert.strictEqual(receipts[0]!.usage.cost?.amount, 83)
                        return yield* Effect.failCause(causes[mode])
                      }),
                  }),
                  Hook.make({
                    event: 'beforeCompact',
                    execute: () =>
                      Effect.sync(() => {
                        laterHooks++
                        return { _tag: 'decline' as const }
                      }),
                  }),
                ],
              }),
            ]
            const owner = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
            const open = (
              context: Context.Context<Storage.Storage | StorageFormat.StorageFormat>,
            ) =>
              HarnessRuntime.make({ models: [model], extensions }).pipe(
                Effect.provideContext(context),
                Effect.provideService(Settings.Settings, settings),
              )
            first = yield* open(storage).pipe(Effect.provideService(Scope.Scope, owner))
            yield* first.backend.root
            yield* seed(first)
            const mixed = yield* Deferred.make<string>()
            if (mode === 'interrupt') {
              const pull = yield* Stream.toPull(first.scheduler.changes)
              yield* Effect.gen(function* () {
                while (true) {
                  yield* pull
                  const failure = yield* first.scheduler.failure
                  if (Option.isSome(failure)) {
                    yield* Deferred.succeed(mixed, Cause.pretty(Cause.fail(failure.value)))
                    return
                  }
                }
              }).pipe(Effect.forkChild)
            }
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'continue' },
            })
            if (mode === 'interrupt') {
              const full = yield* Deferred.await(mixed).pipe(Effect.timeout('5 seconds'))
              assert.include(full, 'critical-hook-typed')
              assert.include(full, 'Interrupt')
            } else {
              const outcome = yield* first.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))
              assert.strictEqual(outcome.status, 'unanswered')
              const tasks = yield* Session.scanTasks(first.session, { kind: 'harness.run' }).pipe(
                Stream.runCollect,
              )
              const text = JSON.stringify(tasks)
              if (mode !== 'ordinary-only' && mode !== 'defect-only') {
                assert.include(text, 'critical-hook-typed')
                assert.strictEqual(laterHooks, 0)
                if (mode === 'defect') assert.include(text, 'critical-hook-defect')
                if (mode === 'multiple') assert.include(text, 'critical-hook-second')
                if (mode === 'ordinary-first' || mode === 'ordinary-last')
                  assert.include(text, 'critical-hook-ordinary')
              } else assert.strictEqual(laterHooks, 1)
            }
            const receipts = yield* Session.commit(first.session, (tx) => Usage.receipts(tx, root))
            assert.strictEqual(receipts.length, 1)
            assert.strictEqual(receipts[0]!.outcome._tag, 'failed')
            assert.strictEqual(generations, 1)
            assert.strictEqual(summaries, 0)
            assert.strictEqual(hooks, 1)
            assert.strictEqual(
              (yield* entries(first)).filter(
                (e) => e.kind.startsWith('assistant') || e.kind === 'compaction',
              ).length,
              0,
            )
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)))
                .minimumReaderVersion,
              12,
            )
            yield* Scope.close(owner, Exit.void)
            if (mode !== 'interrupt') {
              const second = yield* open(yield* store.open(backend))
              assert.strictEqual((yield* second.backend.wait(job.id)).status, 'unanswered')
              const coldTasks = yield* Session.scanTasks(second.session, {
                kind: 'harness.run',
              }).pipe(Stream.runCollect)
              if (mode === 'defect')
                assert.include(JSON.stringify(coldTasks), 'critical-hook-defect')
              if (mode === 'multiple')
                assert.include(JSON.stringify(coldTasks), 'critical-hook-second')
              assert.deepEqual(
                yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root)),
                receipts,
              )
              assert.strictEqual(generations, 1)
              assert.strictEqual(hooks, 1)
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
  })
}
