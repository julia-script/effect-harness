import * as ScriptedStream from './fixtures/ScriptedStream.js'
import { assert, describe, it } from '@effect/vitest'
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
import type * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Record from 'effect-harness/Record'
import * as Identity from 'effect-harness/Identity'
import * as RunControl from 'effect-harness/RunControl'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as Format from 'effect-harness/StorageFormat'
import * as Submission from 'effect-harness/Submission'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Registry from 'effect-harness/Registry'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as Kernel from '../src/internal/Kernel.js'
import * as Memory from '../src/internal/Memory.js'
import * as Run from '../src/internal/RunState.js'

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
const users = (prompt: Prompt.Prompt) =>
  prompt.content
    .filter((m) => m.role === 'user')
    .flatMap((m) => m.content.filter((p) => p.type === 'text').map((p) => p.text))
const plain = Layer.effect(
  LanguageModel.LanguageModel,
  ScriptedStream.make({
    generateText: () => Effect.succeed(answer),
  }),
)
const client = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
const entries = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root, order: 'ascending' }).pipe(
    Stream.runCollect,
  )
const taskRecords = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanTasks(runtime.session, { conversationId: root, kind: 'harness.run' }).pipe(
    Stream.runCollect,
  )
const tick = Effect.forEach(Array.from({ length: 20 }), () => Effect.yieldNow, { discard: true })

/** Real scoped native backends; every disk reopen obtains a new backend and full paired Context. */
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  const access = yield* Memory.makeAccess
  const memory = Layer.effectContext(
    Kernel.make(access).pipe(
      Effect.map((storage) =>
        Context.make(Storage.Storage, storage).pipe(
          Context.add(Format.StorageFormat, storage.storageFormat),
        ),
      ),
    ),
  )
  const sqlite = Storage.layerSql.pipe(
    Layer.provide(SqliteClient.layer({ filename: `${directory}/submissions.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/submissions.jsonl` })
  return { fs, directory, memory, sqlite, jsonl }
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} durable submission boundaries`, () => {
    it.live(
      'final all-mode settles the old run before one successor; saved handles resume without opening',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const disk = yield* fixture
            const settings = yield* Settings.make()
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const prompts: string[][] = []
            const model = yield* ScriptedStream.make({
              generateText: ({ prompt }) => {
                prompts.push(users(prompt))
                return prompts.length === 1
                  ? Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.as(answer),
                    )
                  : Effect.succeed(answer)
              },
            })
            const open = Effect.gen(function* () {
              const native = yield* Layer.build(Layer.fresh(disk[backend]))
              const runtime = yield* HarnessRuntime.make().pipe(
                Effect.provideContext(native),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provideService(Settings.Settings, settings),
              )
              return { runtime, harness: yield* client(runtime) }
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
            const conversation = yield* first.harness.root
            const original = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'original',
              requestId: 'original',
            })
            yield* Deferred.await(entered)
            const a = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'A',
              requestId: 'A',
            })
            const b = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'B',
              requestId: 'B',
            })
            const c = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'C',
              requestId: 'C',
            })
            yield* settings.update({ followUpMode: 'all' })
            const duplicate = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'changed',
              requestId: 'original',
              whenBusy: 'reject',
            })
            assert.strictEqual(duplicate.id, original.id)
            const before = {
              tasks: (yield* taskRecords(first.runtime)).length,
              entries: (yield* entries(first.runtime)).length,
            }
            const rejected = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'denied',
              whenBusy: 'reject',
            }).pipe(Effect.flip)
            assert.strictEqual(rejected.operation, 'conversation.busy')
            assert.deepStrictEqual(
              {
                tasks: (yield* taskRecords(first.runtime)).length,
                entries: (yield* entries(first.runtime)).length,
              },
              before,
            )
            yield* Scope.close(firstScope, Exit.void)
            yield* Deferred.succeed(release, undefined)
            const second = yield* open
            const count = (yield* entries(second.runtime)).length
            const handles = yield* Effect.forEach([original.id, a.id, b.id, c.id], (id) =>
              second.harness.submission(id),
            )
            assert.strictEqual((yield* entries(second.runtime)).length, count)
            assert.strictEqual(
              (yield* Format.inspect.pipe(
                Effect.provideContext(yield* Layer.build(Layer.fresh(Storage.layerMemory))),
              )).formatVersion,
              1,
            )
            const outcomes = yield* Effect.forEach(handles, Submission.wait, {
              concurrency: 'unbounded',
            })
            assert.isTrue(outcomes.every((receipt) => receipt._tag === 'InputDone'))
            assert.deepStrictEqual(prompts.at(-1), ['original', 'A', 'B', 'C'])
            assert.strictEqual((yield* taskRecords(second.runtime)).length, 2)
            const done = outcomes.filter((receipt) => receipt._tag === 'InputDone')
            assert.strictEqual(new Set(done.slice(1).map((receipt) => receipt.answer)).size, 1)
            assert.notStrictEqual(done[0]!.answer, done[1]!.answer)
            assert.strictEqual(
              (yield* entries(second.runtime)).filter((entry) => entry.kind === 'input').length,
              4,
            )
            assert.strictEqual(
              (yield* Submission.read(yield* second.harness.submission(b.id))).status,
              'done',
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'postTools waits for all results, joins steers to current run and preserves follow-ups',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const disk = yield* fixture
            const policy = yield* Settings.make({ steeringMode: 'all' })
            const firstEntered = yield* Deferred.make<void>()
            const secondEntered = yield* Deferred.make<void>()
            const firstRelease = yield* Deferred.make<void>()
            const secondRelease = yield* Deferred.make<void>()
            const prompts: Prompt.Prompt[] = []
            let executed = 0
            const tools = Toolkit.make(
              Tool.make('work', {
                parameters: Schema.Struct({ n: Schema.Finite }),
                success: Schema.String,
                replay: 'safe',
              }),
            )
            const handlers = tools.toLayer({
              work: ({ n }) =>
                Effect.gen(function* () {
                  executed++
                  yield* Deferred.succeed(n === 1 ? firstEntered : secondEntered, undefined)
                  yield* Deferred.await(n === 1 ? firstRelease : secondRelease)
                  return `result${n}`
                }),
            })
            const model = yield* ScriptedStream.make({
              generateText: ({ prompt }) => {
                prompts.push(prompt)
                return Effect.succeed(
                  prompts.length === 1
                    ? [
                        {
                          type: 'tool-call',
                          id: 'first',
                          name: 'work',
                          params: { n: 1 },
                          providerExecuted: false,
                        },
                        {
                          type: 'tool-call',
                          id: 'second',
                          name: 'work',
                          params: { n: 2 },
                          providerExecuted: false,
                        },
                        finish('tool-calls'),
                      ]
                    : answer,
                )
              },
            })
            const storageContext = yield* Layer.build(Layer.fresh(disk[backend]))
            const runtime = yield* HarnessRuntime.make({ tools }).pipe(
              Effect.provideContext(storageContext),
              Effect.provide(handlers),
              Effect.provideService(Settings.Settings, policy),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const harness = yield* client(runtime)
            const conversation = yield* harness.root
            const original = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'original',
            })
            yield* Deferred.await(firstEntered)
            const a = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'steerA',
              whenBusy: 'steer',
            })
            const b = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'steerB',
              whenBusy: 'steer',
            })
            const follow = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'follow',
            })
            const write = yield* Conversation.submit(conversation, {
              type: 'write',
              entry: {
                kind: 'passive',
                model: [{ role: 'user', content: [{ type: 'text', text: 'write' }] }],
              },
            })
            assert.strictEqual((yield* Submission.read(a)).status, 'queued')
            yield* Deferred.succeed(firstRelease, undefined)
            yield* Deferred.await(secondEntered)
            assert.strictEqual(prompts.length, 1)
            assert.strictEqual((yield* Submission.read(a)).status, 'queued')
            yield* Deferred.succeed(secondRelease, undefined)
            const outcomes = yield* Effect.forEach([original, a, b, follow], Submission.wait, {
              concurrency: 'unbounded',
            })
            assert.strictEqual(executed, 2)
            assert.isTrue(outcomes.every((receipt) => receipt._tag === 'InputDone'))
            const done = outcomes.filter((receipt) => receipt._tag === 'InputDone')
            assert.strictEqual(done[0]!.answer, done[1]!.answer)
            assert.strictEqual(done[0]!.answer, done[2]!.answer)
            assert.notStrictEqual(done[0]!.answer, done[3]!.answer)
            assert.deepStrictEqual(users(prompts[1]!), ['original', 'write', 'steerA', 'steerB'])
            assert.strictEqual(
              prompts[1]!.content
                .filter((message) => message.role === 'tool')
                .flatMap((message) => message.content).length,
              2,
            )
            assert.strictEqual((yield* Submission.wait(write))._tag, 'WriteDone')
            assert.strictEqual((yield* taskRecords(runtime)).length, 2)
            const history = yield* entries(runtime)
            const lastResult = history.findLastIndex((entry) => entry.kind === 'tool.result')
            const placedWrite = history.findIndex((entry) => entry.kind === 'passive')
            assert.isTrue(lastResult < placedWrite)
            assert.isTrue(
              placedWrite <
                history.findIndex(
                  (entry) =>
                    entry.kind === 'input' && JSON.stringify(entry.model).includes('steerA'),
                ),
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'idle write/reset are passive, stale cuts never append, fork/reopen preserve reset context',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const disk = yield* fixture
            const prompts: string[][] = []
            const model = yield* ScriptedStream.make({
              generateText: ({ prompt }) => {
                prompts.push(users(prompt))
                return Effect.succeed(answer)
              },
            })
            const open = Effect.gen(function* () {
              const storageContext = yield* Layer.build(Layer.fresh(disk[backend]))
              const runtime = yield* HarnessRuntime.make().pipe(
                Effect.provideContext(storageContext),
                Effect.provideService(LanguageModel.LanguageModel, model),
              )
              return { runtime, harness: yield* client(runtime) }
            })
            const scope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, scope))
            const conversation = yield* first.harness.root
            const input = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'old',
            })
            const old = yield* Submission.wait(input)
            assert.strictEqual(old._tag, 'InputDone')
            const history = yield* entries(first.runtime)
            const oldHead = history[0]!.id
            const write = yield* Conversation.submit(conversation, {
              type: 'write',
              entry: { kind: 'audit', data: { passive: true } },
              requestId: 'audit',
            })
            assert.strictEqual((yield* Submission.wait(write))._tag, 'WriteDone')
            const cut = yield* Conversation.reset(conversation, {
              handoff: 'handoff',
              requestId: 'reset',
            })
            const reset = yield* Submission.wait(cut)
            assert.strictEqual(reset._tag, 'WriteDone')
            if (reset._tag !== 'WriteDone') return
            assert.strictEqual(prompts.length, 1)
            assert.strictEqual((yield* taskRecords(first.runtime)).length, 1)
            const count = (yield* entries(first.runtime)).length
            const stale = yield* Conversation.submit(conversation, {
              type: 'write',
              entry: { kind: 'old.cut', head: oldHead },
            })
            assert.deepStrictEqual((yield* Submission.wait(stale)).status, 'unanswered')
            assert.strictEqual((yield* entries(first.runtime)).length, count)
            assert.strictEqual(
              (yield* Conversation.reset(conversation, { requestId: 'reset' })).id,
              cut.id,
            )
            const beforeReset = yield* Conversation.fork(conversation, { at: oldHead })
            yield* Submission.wait(
              yield* Conversation.submit(beforeReset, { type: 'input', content: 'before' }),
            )
            assert.deepStrictEqual(prompts.at(-1), ['old', 'before'])
            const fork = yield* Conversation.fork(conversation, { at: reset.entry })
            const continued = yield* Conversation.submit(fork, { type: 'input', content: 'fork' })
            yield* Submission.wait(continued)
            assert.deepStrictEqual(prompts.at(-1), ['handoff', 'fork'])
            yield* Scope.close(scope, Exit.void)
            const second = yield* open
            const reacquired = yield* second.harness.submission(write.id)
            assert.strictEqual((yield* Submission.wait(reacquired))._tag, 'WriteDone')
            const reopened = Option.getOrThrow(yield* second.harness.conversation(conversation.id))
            const next = yield* Conversation.submit(reopened, { type: 'input', content: 'new' })
            yield* Submission.wait(next)
            assert.deepStrictEqual(prompts.at(-1), ['handoff', 'new'])
            assert.strictEqual(
              (yield* entries(second.runtime)).filter((entry) => entry.kind === 'reset').length,
              1,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  })
}

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const settleMode of ['abort', 'failed'] as const) {
    it.live(
      `${backend}: grouped ${settleMode} settles every member once and survives cold reopen`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const disk = yield* fixture
            const toolEntered = yield* Deferred.make<void>()
            const toolRelease = yield* Deferred.make<void>()
            const modelEntered = yield* Deferred.make<void>()
            const modelRelease = yield* Deferred.make<void>()
            let requests = 0
            const tools = Toolkit.make(
              Tool.make('work', {
                parameters: Schema.Struct({}),
                success: Schema.String,
                replay: 'safe',
              }),
            )
            const handlers = tools.toLayer({
              work: () =>
                Deferred.succeed(toolEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(toolRelease)),
                  Effect.as('result'),
                ),
            })
            const model = yield* ScriptedStream.make({
              generateText: () => {
                requests++
                if (requests === 1)
                  return Effect.succeed([
                    {
                      type: 'tool-call',
                      id: 'call',
                      name: 'work',
                      params: {},
                      providerExecuted: false,
                    },
                    finish('tool-calls'),
                  ])
                if (requests > 2) return Effect.succeed(answer)
                return Deferred.succeed(modelEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(modelRelease)),
                  Effect.andThen(Effect.die('model failed')),
                )
              },
            })
            const scope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
            const open = Effect.gen(function* () {
              const storage = yield* Layer.build(Layer.fresh(disk[backend]))
              const runtime = yield* HarnessRuntime.make({ tools }).pipe(
                Effect.provideContext(storage),
                Effect.provide(handlers),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provideService(
                  Settings.Settings,
                  yield* Settings.make({ steeringMode: 'all' }),
                ),
              )
              return { runtime, harness: yield* client(runtime) }
            })
            const first = yield* open.pipe(Effect.provideService(Scope.Scope, scope))
            const conversation = yield* first.harness.root
            const original = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'original',
            })
            yield* Deferred.await(toolEntered)
            const a = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'steerA',
              whenBusy: 'steer',
            })
            const b = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'steerB',
              whenBusy: 'steer',
            })
            yield* Deferred.succeed(toolRelease, undefined)
            yield* Deferred.await(modelEntered)
            assert.isTrue(
              (yield* Effect.forEach([original, a, b], Submission.read)).every(
                (receipt) => receipt.status === 'placed',
              ),
            )
            const queued = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'queued',
            })
            const write = yield* Conversation.submit(conversation, {
              type: 'write',
              entry: { kind: 'surviving.write', data: { keep: true } },
            })
            if (settleMode === 'abort') yield* Conversation.abort(conversation)
            else yield* Deferred.succeed(modelRelease, undefined)
            const outcomes = yield* Effect.forEach([original, a, b], Submission.wait)
            assert.isTrue(
              outcomes.every(
                (receipt) =>
                  receipt._tag === 'InputUnanswered' &&
                  receipt.reason === (settleMode === 'abort' ? 'aborted' : 'failed'),
              ),
            )
            assert.strictEqual(
              (yield* Submission.wait(queued)).status,
              settleMode === 'abort' ? 'unanswered' : 'done',
            )
            assert.strictEqual((yield* Submission.wait(write))._tag, 'WriteDone')
            yield* Scope.close(scope, Exit.void)
            const second = yield* open
            for (const id of [original.id, a.id, b.id, queued.id, write.id]) {
              const saved = yield* second.harness.submission(id)
              assert.include(['done', 'unanswered'], (yield* Submission.wait(saved)).status)
            }
            assert.strictEqual(
              (yield* entries(second.runtime)).filter((entry) => entry.kind === 'surviving.write')
                .length,
              1,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

it.live(
  'publication settles saved write/input waiters directly; withdrawal removes queue atomically and wait interruption preserves work',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make()
        const harness = yield* client(runtime)
        yield* harness.root
        const ids = yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            const input = yield* Transaction.createSubmission(tx, {
              _tag: 'InputQueued',
              type: 'input',
              status: 'queued',
              conversationId: root,
            })
            const write = yield* Transaction.createSubmission(tx, {
              _tag: 'WriteQueued',
              type: 'write',
              status: 'queued',
              conversationId: root,
            })
            return { input, write }
          }),
        )
        const input = yield* harness.submission(ids.input.id)
        const write = yield* harness.submission(ids.write.id)
        const waitingInput = yield* Submission.wait(input).pipe(
          Effect.forkChild({ startImmediately: true }),
        )
        const cancelled = yield* Submission.wait(write).pipe(
          Effect.forkChild({ startImmediately: true }),
        )
        yield* tick
        yield* Fiber.interrupt(cancelled)
        assert.strictEqual((yield* Submission.read(write)).status, 'queued')
        const waitingWrite = yield* Submission.wait(write).pipe(
          Effect.forkChild({ startImmediately: true }),
        )
        yield* tick
        yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            if (ids.input._tag !== 'InputQueued' || ids.write._tag !== 'WriteQueued')
              return yield* Effect.die('invalid fixture receipts')
            yield* Transaction.putSubmission(tx, {
              ...ids.input,
              _tag: 'InputUnanswered',
              status: 'unanswered',
              reason: 'direct',
            })
            yield* Transaction.putSubmission(tx, {
              ...ids.write,
              _tag: 'WriteUnanswered',
              status: 'unanswered',
              reason: 'direct',
            })
          }),
        )
        assert.strictEqual((yield* Fiber.join(waitingInput))._tag, 'InputUnanswered')
        assert.strictEqual((yield* Fiber.join(waitingWrite))._tag, 'WriteUnanswered')
      }),
    ).pipe(Effect.provide([Storage.layerMemory, plain])),
)

it.live(
  'one-mode and reset during tools place every write before selected users without another old-context request',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const prompts: string[][] = []
        const tools = Toolkit.make(
          Tool.make('work', {
            parameters: Schema.Struct({}),
            success: Schema.String,
            replay: 'safe',
          }),
        )
        const handlers = tools.toLayer({
          work: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as('result'),
            ),
        })
        const model = yield* ScriptedStream.make({
          generateText: ({ prompt }) => {
            prompts.push(users(prompt))
            return Effect.succeed(
              prompts.length === 1
                ? [
                    {
                      type: 'tool-call',
                      id: 'call',
                      name: 'work',
                      params: {},
                      providerExecuted: false,
                    },
                    finish('tool-calls'),
                  ]
                : answer,
            )
          },
        })
        const runtime = yield* HarnessRuntime.make({ tools }).pipe(
          Effect.provide(handlers),
          Effect.provideService(LanguageModel.LanguageModel, model),
        )
        const harness = yield* client(runtime)
        const conversation = yield* harness.root
        const original = yield* Conversation.submit(conversation, { type: 'input', content: 'old' })
        yield* Deferred.await(entered)
        const head = (yield* entries(runtime))[0]!.id
        const a = yield* Conversation.submit(conversation, { type: 'input', content: 'A' })
        const b = yield* Conversation.submit(conversation, { type: 'input', content: 'B' })
        const steer = yield* Conversation.submit(conversation, {
          type: 'input',
          content: 'steer',
          whenBusy: 'steer',
        })
        const reset = yield* Conversation.reset(conversation, { handoff: 'handoff' })
        const stale = yield* Conversation.submit(conversation, {
          type: 'write',
          entry: { kind: 'stale', head },
        })
        const write = yield* Conversation.submit(conversation, {
          type: 'write',
          entry: { kind: 'audit' },
        })
        yield* Deferred.succeed(release, undefined)
        const old = yield* Submission.wait(original)
        assert.strictEqual(old._tag, 'InputUnanswered')
        if (old._tag === 'InputUnanswered') assert.strictEqual(old.reason, 'reset')
        const outcomes = yield* Effect.forEach([a, b, steer], Submission.wait)
        assert.isTrue(outcomes.every((receipt) => receipt._tag === 'InputDone'))
        assert.deepStrictEqual(prompts[1], ['handoff', 'A', 'steer'])
        assert.deepStrictEqual(prompts[2], ['handoff', 'A', 'steer', 'B'])
        const done = outcomes.filter((receipt) => receipt._tag === 'InputDone')
        assert.strictEqual(done[0]!.answer, done[2]!.answer)
        assert.notStrictEqual(done[0]!.answer, done[1]!.answer)
        assert.strictEqual((yield* Submission.wait(reset))._tag, 'WriteDone')
        assert.strictEqual((yield* Submission.wait(write))._tag, 'WriteDone')
        const staleReceipt = yield* Submission.wait(stale)
        assert.strictEqual(staleReceipt._tag, 'WriteUnanswered')
        if (staleReceipt._tag === 'WriteUnanswered')
          assert.strictEqual(staleReceipt.reason, 'stale')
        const history = yield* entries(runtime)
        assert.isFalse(history.some((entry) => entry.kind === 'stale'))
        const resultIndex = history.findIndex((entry) => entry.kind === 'tool.result')
        const resetIndex = history.findIndex((entry) => entry.kind === 'reset')
        const auditIndex = history.findIndex((entry) => entry.kind === 'audit')
        assert.isTrue(resultIndex < resetIndex && resetIndex < auditIndex)
        assert.isTrue(
          auditIndex <
            history.findIndex(
              (entry) => entry.kind === 'input' && JSON.stringify(entry.model).includes('A'),
            ),
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.live(
  'invalid, denied and cross-type drafts perform no new-format activation or durable writes',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make()
        yield* Session.commit(runtime.session, Transaction.ensureRoot)
        const before = yield* Format.inspect
        const count = (yield* Session.scanSubmissions(runtime.session).pipe(Stream.runCollect))
          .length
        for (const draft of [
          { type: 'write', entry: {} },
          { type: 'write', entry: { kind: 'invalid', data: { closure: () => 1 } } },
          { type: 'write', entry: { kind: 'invalid', head: -1 } },
          { type: 'input', content: 'invalid', whenBusy: 'invented' },
        ]) {
          const failed = yield* Schema.decodeUnknownEffect(Submission.DraftSchema)(draft).pipe(
            Effect.result,
          )
          assert.strictEqual(failed._tag, 'Failure')
        }
        const invisible = yield* runtime.backend
          .submit({
            conversationId: root,
            draft: { type: 'write', entry: { kind: 'invalid', head: Record.EntryId.make(99999) } },
          })
          .pipe(Effect.flip)
        assert.strictEqual(invisible.reason, 'invalid')
        assert.deepStrictEqual(yield* Format.inspect, before)
        assert.strictEqual(
          (yield* Session.scanSubmissions(runtime.session).pipe(Stream.runCollect)).length,
          count,
        )
        const requestId = Identity.RequestId
        // The real legacy task/data can exist before this reader enables the new queue format.
        const seeded = yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            const submission = yield* Transaction.createSubmission(tx, {
              _tag: 'InputQueued',
              type: 'input',
              status: 'queued',
              conversationId: root,
              requestId: yield* Schema.decodeEffect(requestId)('existing'),
            })
            yield* Transaction.createTask(tx, {
              conversationId: root,
              kind: 'harness.run',
              version: 1,
              input: submission.id,
              background: false,
              abortRequested: false,
              state: {
                status: 'pending',
                checkpoint: yield* Run.encode({
                  _tag: 'queued',
                  phase: 'queued',
                  submissionId: submission.id,
                  draft: { type: 'input', content: 'legacy' },
                }),
              },
            })
            return submission
          }),
        )
        const denied = yield* runtime.backend
          .submit({
            conversationId: root,
            draft: { type: 'input', content: 'denied', whenBusy: 'reject' },
          })
          .pipe(Effect.flip)
        assert.strictEqual(denied.operation, 'conversation.busy')
        const cross = yield* runtime.backend
          .submit({
            conversationId: root,
            draft: { type: 'write', requestId: 'existing', entry: { kind: 'different' } },
          })
          .pipe(Effect.flip)
        assert.strictEqual(cross.reason, 'conflict')
        assert.deepStrictEqual(yield* Format.inspect, before)
        assert.strictEqual((yield* runtime.backend.read(seeded.id)).status, 'queued')
      }),
    ).pipe(Effect.provide([Storage.layerMemory, plain])),
)

it.live('withdrawal/admission races remove each queued item and do not duplicate placements', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      let requests = 0
      const model = yield* ScriptedStream.make({
        generateText: () => {
          requests++
          return requests === 1
            ? Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(answer),
              )
            : Effect.succeed(answer)
        },
      })
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      const harness = yield* client(runtime)
      const conversation = yield* harness.root
      const original = yield* Conversation.submit(conversation, {
        type: 'input',
        content: 'original',
      })
      yield* Deferred.await(entered)
      const results = yield* Effect.forEach(
        Array.from({ length: 12 }, (_, n) => n),
        (n) =>
          Effect.gen(function* () {
            const draft: Submission.Draft =
              n % 2 === 0
                ? { type: 'input', content: `input${n}`, requestId: `r${n}` }
                : { type: 'write', entry: { kind: `write${n}` }, requestId: `r${n}` }
            const pair = yield* Effect.forEach(
              [0, 1],
              () => Conversation.submit(conversation, draft),
              { concurrency: 'unbounded' },
            )
            assert.strictEqual(pair[0]!.id, pair[1]!.id)
            const saved = yield* harness.submission(pair[0]!.id)
            assert.strictEqual(yield* Submission.withdraw(saved), 'aborted')
            assert.strictEqual(yield* Submission.withdraw(pair[1]!), 'settled')
            return saved
          }),
        { concurrency: 'unbounded' },
      )
      const queue = Option.getOrThrow(
        yield* Session.snapshot(runtime.session, RunControl.inbox, RunControl.target(root)),
      )
      assert.strictEqual(queue.value.items.length, 0)
      yield* Deferred.succeed(release, undefined)
      yield* Submission.wait(original)
      assert.isTrue(
        (yield* Effect.forEach(results, Submission.wait)).every(
          (receipt) => receipt.status === 'unanswered',
        ),
      )
      assert.strictEqual(
        (yield* entries(runtime)).filter((entry) => entry.kind === 'input').length,
        1,
      )
      assert.strictEqual((yield* taskRecords(runtime)).length, 1)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

it.live(
  'passive write rejection rolls back queue, mapping and entry; the same request can be retried once',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const real = yield* Storage.Storage
        const format = yield* Format.StorageFormat
        let reject = true
        const storage: Storage.Storage['Service'] = {
          ...real,
          commit: (writes) => {
            if (
              reject &&
              Array.from(writes).some(
                (write) => write._tag === 'entry' && write.value.kind === 'write.fail',
              )
            )
              return Effect.fail(
                new Storage.StorageError({
                  reason: 'io',
                  operation: 'test.write',
                  message: 'rejected before commit',
                }),
              )
            return real.commit(writes)
          },
        }
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(Storage.Storage, storage),
          Effect.provideService(Format.StorageFormat, { ...format, storage }),
        )
        const harness = yield* client(runtime)
        const conversation = yield* harness.root
        const draft: Submission.Draft = {
          type: 'write',
          entry: { kind: 'write.fail', data: { once: true } },
          requestId: 'write.fail',
        }
        const failure = yield* Conversation.submit(conversation, draft).pipe(Effect.flip)
        assert.include(failure.message, 'rejected before commit')
        assert.strictEqual((yield* entries(runtime)).length, 0)
        assert.strictEqual(
          (yield* Session.scanSubmissions(runtime.session).pipe(Stream.runCollect)).length,
          0,
        )
        reject = false
        const succeeded = yield* Conversation.submit(conversation, draft)
        const repeated = yield* Conversation.submit(conversation, draft)
        assert.strictEqual(succeeded.id, repeated.id)
        assert.strictEqual((yield* Submission.wait(succeeded))._tag, 'WriteDone')
        assert.strictEqual((yield* entries(runtime)).length, 1)
        assert.strictEqual((yield* taskRecords(runtime)).length, 0)
      }),
    ).pipe(Effect.provide([Storage.layerMemory, plain])),
)

/** Reader10 validation rules from b3e650f77 Metadata.ts, before domain rows. */
const priorReader10 = Effect.fnUntraced(function* (metadata: {
  format: number
  minimumReader?: number
}) {
  if (metadata.format > 10 || (metadata.minimumReader ?? 1) > 10)
    return yield* new Storage.StorageError({
      reason: 'unsupportedFormat',
      operation: 'metadata',
      message: 'Storage requires an unsupported reader',
    })
  if (metadata.format >= 2 && metadata.minimumReader !== metadata.format)
    return yield* new Storage.StorageError({
      reason: 'corrupt',
      operation: 'metadata',
      message: 'Container format requires its matching reader',
    })
  if (metadata.format === 1 && (metadata.minimumReader ?? 1) !== 1)
    return yield* new Storage.StorageError({
      reason: 'corrupt',
      operation: 'metadata',
      message: 'Legacy format requires minimum reader 1',
    })
  return metadata
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  it.live(
    `${backend}: format11 persists before grouped facts and unchanged reader10 refuses before replay`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const disk = yield* fixture
          const context = yield* Layer.build(Layer.fresh(disk[backend]))
          const runtime = yield* HarnessRuntime.make().pipe(Effect.provideContext(context))
          yield* Session.commit(runtime.session, Transaction.ensureRoot)
          assert.deepStrictEqual(yield* Format.inspect.pipe(Effect.provideContext(context)), {
            formatVersion: 1,
            minimumReaderVersion: 1,
          })
          const harness = yield* client(runtime)
          const draft = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'write', entry: { kind: 'passive' } },
          })
          const saved = yield* harness.submission(draft.id)
          assert.strictEqual((yield* Submission.wait(saved))._tag, 'WriteDone')
          const marker = yield* Format.inspect.pipe(Effect.provideContext(context))
          assert.deepStrictEqual(marker, { formatVersion: 11, minimumReaderVersion: 11 })
          assert.strictEqual(
            (yield* priorReader10({
              format: marker.formatVersion,
              minimumReader: marker.minimumReaderVersion,
            }).pipe(Effect.flip)).reason,
            'unsupportedFormat',
          )
          for (const version of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            assert.strictEqual(
              (yield* priorReader10({ format: version, minimumReader: version })).format,
              version,
            )
          if (backend === 'jsonl') {
            const frames = yield* Effect.forEach(
              (yield* disk.fs.readFileString(`${disk.directory}/submissions.jsonl`))
                .trim()
                .split('\n'),
              (line) =>
                Schema.decodeEffect(
                  Schema.fromJsonString(
                    Schema.Struct({
                      metadata: Schema.Struct({ format: Schema.Finite }),
                      rows: Schema.Array(Schema.Struct({ id: Schema.Finite })),
                    }),
                  ),
                )(line),
            )
            const upgrade = frames.findIndex((frame) => frame.metadata.format === 11)
            const documents = yield* Context.get(context, Storage.Storage)
              .scanDocuments({
                scope: { _tag: 'conversation', conversationId: root },
                kind: 'harness.contextProtocol',
              })
              .pipe(Stream.runCollect)
            assert.lengthOf(documents, 1)
            const protocol = frames.findIndex((frame) =>
              frame.rows.some((row) => row.id === documents[0]!.id),
            )
            assert.isTrue(upgrade >= 0 && upgrade < protocol)
          }
        }),
      ).pipe(Effect.provide([NodeServices.layer, plain])),
  )
}

for (const includeBackground of [false, true]) {
  it.live(`Conversation.abort supervises fresh cleanup with background=${includeBackground}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* HarnessRuntime.make()
        const harness = yield* client(runtime)
        const conversation = yield* harness.root
        const entered = yield* Deferred.make<void>()
        const backgroundEntered = yield* Deferred.make<void>()
        const cleaned: number[] = []
        const contract = {
          kind: 'submission.cleanup',
          version: 1,
          input: Schema.Natural,
          checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
          success: Schema.Void,
          failure: Schema.String,
        }
        const definition = Task.make({
          ...contract,
          initial: () => ({ phase: 'run' as const }),
          phases: {
            run: ({ input }) =>
              Deferred.succeed(input === 1 ? entered : backgroundEntered, undefined).pipe(
                Effect.andThen(Effect.never),
              ),
          },
          abort: ({
            input,
          }: {
            readonly input: number
            readonly checkpoint: { readonly phase: 'run' }
          }) =>
            Effect.gen(function* () {
              const access = yield* TaskExecution
              yield* access.commit((tx) =>
                access.progress(tx, contract, {
                  status: 'completing',
                  outcome: { status: 'aborted', reason: 'cleaned' },
                }),
              )
              cleaned.push(input)
            }),
        })
        yield* runtime.registry.install(
          Registry.withTask(
            Registry.toolkit({ id: 'submission.cleanup', version: 1 }, Toolkit.make()),
            definition,
          ),
        )
        const ids = {
          a: yield* runtime.scheduler.start(definition, 1, { conversationId: root }),
          b: yield* runtime.scheduler.start(definition, 2, {
            conversationId: root,
            background: true,
          }),
        }
        yield* runtime.scheduler.resume
        yield* Deferred.await(entered)
        yield* Deferred.await(backgroundEntered)
        yield* Conversation.abort(conversation, { includeBackground })
        assert.strictEqual((yield* runtime.scheduler.wait(ids.a)).status, 'aborted')
        const background = Option.getOrThrow(yield* Session.task(runtime.session, ids.b.id))
        assert.strictEqual(background.state.status === 'terminal', includeBackground)
        assert.deepStrictEqual(
          cleaned.sort((a, b) => a - b),
          includeBackground ? [1, 2] : [1],
        )
        if (!includeBackground) yield* runtime.scheduler.abort(ids.b.id)
        yield* Conversation.abort(conversation, { includeBackground: true })
        assert.deepStrictEqual(
          cleaned.sort((a, b) => a - b),
          [1, 2],
        )
      }),
    ).pipe(Effect.provide([Storage.layerMemory, plain])),
  )
}

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  it.live(
    `${backend}: cold reopen of placed grouped steering answers each ID once without replaying completed tools`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const disk = yield* fixture
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const requesting = yield* Deferred.make<void>()
          const never = yield* Deferred.make<void>()
          let calls = 0
          let requests = 0
          const tools = Toolkit.make(
            Tool.make('work', {
              parameters: Schema.Struct({}),
              success: Schema.String,
              replay: 'unsafe',
            }),
          )
          const handlers = tools.toLayer({
            work: () =>
              Effect.gen(function* () {
                calls++
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
                return 'once'
              }),
          })
          const model = yield* ScriptedStream.make({
            generateText: () => {
              requests++
              if (requests === 1)
                return Effect.succeed([
                  {
                    type: 'tool-call',
                    id: 'call',
                    name: 'work',
                    params: {},
                    providerExecuted: false,
                  },
                  finish('tool-calls'),
                ])
              return requests === 2
                ? Deferred.succeed(requesting, undefined).pipe(
                    Effect.andThen(Deferred.await(never)),
                    Effect.as(answer),
                  )
                : Effect.succeed(answer)
            },
          })
          const open = Effect.gen(function* () {
            const storage = yield* Layer.build(Layer.fresh(disk[backend]))
            const runtime = yield* HarnessRuntime.make({ tools }).pipe(
              Effect.provideContext(storage),
              Effect.provide(handlers),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provideService(
                Settings.Settings,
                yield* Settings.make({ steeringMode: 'all' }),
              ),
            )
            return { runtime, harness: yield* client(runtime) }
          })
          const scope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, scope))
          const conversation = yield* first.harness.root
          const original = yield* Conversation.submit(conversation, {
            type: 'input',
            content: 'old',
          })
          yield* Deferred.await(entered)
          const steers = yield* Effect.forEach(['A', 'B'], (content) =>
            Conversation.submit(conversation, { type: 'input', content, whenBusy: 'steer' }),
          )
          yield* Deferred.succeed(release, undefined)
          yield* Deferred.await(requesting)
          const pendingWrite = yield* Conversation.submit(conversation, {
            type: 'write',
            entry: { kind: 'reopen.write' },
          })
          const ids = [original.id, ...steers.map((handle) => handle.id)]
          assert.isTrue(
            (yield* Effect.forEach(ids, first.runtime.backend.read)).every(
              (receipt) => receipt.status === 'placed',
            ),
          )
          yield* Scope.close(scope, Exit.void)
          const second = yield* open
          const before = {
            entries: (yield* entries(second.runtime)).length,
            tasks: (yield* taskRecords(second.runtime)).length,
          }
          const saved = yield* Effect.forEach([...ids, pendingWrite.id], (id) =>
            second.harness.submission(id),
          )
          assert.deepStrictEqual(
            {
              entries: (yield* entries(second.runtime)).length,
              tasks: (yield* taskRecords(second.runtime)).length,
            },
            before,
          )
          const outcomes = yield* Effect.forEach(saved, Submission.wait, {
            concurrency: 'unbounded',
          })
          const inputs = outcomes.filter((receipt) => receipt._tag === 'InputDone')
          assert.strictEqual(inputs.length, 3)
          assert.strictEqual(new Set(inputs.map((receipt) => receipt.answer)).size, 1)
          assert.strictEqual(outcomes.at(-1)!._tag, 'WriteDone')
          assert.strictEqual(calls, 1)
          assert.strictEqual(
            (yield* entries(second.runtime)).filter((entry) => entry.kind === 'input').length,
            3,
          )
          assert.strictEqual(
            (yield* entries(second.runtime)).filter((entry) => entry.kind === 'tool.result').length,
            1,
          )
          assert.strictEqual((yield* taskRecords(second.runtime)).length, 1)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}

it.live('historical queued v1 tasks retain IDs and order ahead of new inbox batches', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const prompts: string[][] = []
      const model = yield* ScriptedStream.make({
        generateText: ({ prompt }) => {
          prompts.push(users(prompt))
          return prompts.length === 1
            ? Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(answer),
              )
            : Effect.succeed(answer)
        },
      })
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      const old = yield* Session.commit(runtime.session, (tx) =>
        Effect.gen(function* () {
          yield* Transaction.ensureRoot(tx)
          return yield* Effect.forEach(['oldA', 'oldB'], (content) =>
            Effect.gen(function* () {
              const receipt = yield* Transaction.createSubmission(tx, {
                _tag: 'InputQueued',
                type: 'input',
                status: 'queued',
                conversationId: root,
              })
              const task = yield* Transaction.createTask(tx, {
                conversationId: root,
                kind: 'harness.run',
                version: 1,
                input: receipt.id,
                background: false,
                abortRequested: false,
                state: {
                  status: 'pending',
                  checkpoint: yield* Run.encode({
                    _tag: 'queued',
                    phase: 'queued',
                    submissionId: receipt.id,
                    draft: { type: 'input', content },
                  }),
                },
              })
              return { receipt, task }
            }),
          )
        }),
      )
      const admitted = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'new' },
      })
      yield* Deferred.await(entered)
      assert.strictEqual((yield* taskRecords(runtime)).length, 2)
      yield* Deferred.succeed(release, undefined)
      const outcomes = yield* Effect.forEach(
        [...old.map((item) => item.receipt.id), admitted.id],
        runtime.backend.wait,
      )
      assert.isTrue(outcomes.every((receipt) => receipt._tag === 'InputDone'))
      assert.deepStrictEqual(prompts, [['oldA'], ['oldA', 'oldB'], ['oldA', 'oldB', 'new']])
      const tasks = yield* taskRecords(runtime)
      assert.strictEqual(tasks.length, 3)
      assert.deepStrictEqual(
        tasks
          .slice(0, 2)
          .map((task) => ({ id: task.id, input: task.input, version: task.version })),
        old.map((item) => ({ id: item.task.id, input: item.receipt.id, version: 1 })),
      )
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
