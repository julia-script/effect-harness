import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Cause from 'effect/Cause'
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
import * as Agent from '../src/Agent.js'
import * as Document from '../src/Document.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import * as Record from '../src/Record.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as StorageFormat from '../src/StorageFormat.js'
import { StorageError } from '../src/StorageError.js'
import * as Subagent from '../src/Subagent.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as ToolProgress from '../src/ToolProgress.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'
import * as Usage from '../src/Usage.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const nativeBun = 'Bun' in globalThis
const services = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : NodeServices.layer
const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 2, uncached: 2, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
// The persisted private wire contract is read through public document capabilities.
const admissions = Document.define({
  kind: 'harness.subagent.slots',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({
    slots: Schema.Record(
      Schema.String,
      Schema.Struct({
        request: Schema.toCodecJson(Subagent.RequestSchema),
        handle: Subagent.HandleSchema,
        inherited: Agent.StateSchema,
      }),
    ),
  }),
  initial: () => ({ slots: {} }),
})
type Backend = 'memory' | 'sqlite' | 'jsonl'
type Scenario =
  | 'parallel'
  | 'sequential'
  | 'observed'
  | 'foreign'
  | 'stale'
  | 'rejected'
  | 'uncertain'

const cold = (backend: Backend, scenario: Scenario) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const sqlite = nativeBun
        ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
        : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
      const layer = (() => {
        if (backend === 'memory') return Storage.layerMemory
        if (backend === 'jsonl') return Storage.layerJsonl({ filePath: `${directory}/cold.jsonl` })
        return Storage.layerSql.pipe(
          Layer.provide(sqlite({ filename: `${directory}/cold.sqlite` })),
        )
      })()
      const memory = backend === 'memory' ? yield* Layer.build(layer) : undefined
      const paidReady = yield* Deferred.make<Subagent.Receipt>()
      const observedReady = yield* Deferred.make<void>()
      const closing = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const siblingEntered = yield* Deferred.make<void>()
      let warm = true
      let entries = 0
      let handlers = 0
      let siblingHandlers = 0
      let afterTools = 0
      let saved: Subagent.Handle | undefined
      let late: ToolExecution['Service'] | undefined
      let injected = false
      let rawFailure: Cause.Cause<StorageError> | undefined
      const failure = Cause.combine(
        Cause.fail(
          new StorageError({ reason: 'invalid', operation: 'cold.settlement', message: scenario }),
        ),
        Cause.die('cold-settlement-defect'),
      )
      const toolkit = Toolkit.make(
        Tool.makeResult('delegate', {
          success: Schema.String,
          replay: 'unsafe',
          executionMode: scenario === 'sequential' ? 'sequential' : 'parallel',
        }),
        Tool.makeResult('sibling', {
          success: Schema.String,
          replay: 'safe',
          executionMode: scenario === 'sequential' ? 'sequential' : 'parallel',
        }),
      )
      const provider = yield* ScriptedStream.make({
        generateText: ({ prompt }) =>
          Effect.sync(() => {
            entries++ // Actual native callback entry, including a callback that throws at the guard.
            assert.isTrue(warm, 'Cold recovery must not enter the native provider')
            const child = prompt.content.some(
              (m) =>
                m.role === 'user' &&
                m.content.some((p) => p.type === 'text' && p.text === 'paid child'),
            )
            if (child) return [{ type: 'text' as const, text: 'paid' }, finish]
            return [
              {
                type: 'tool-call' as const,
                id: 'delegate-call',
                name: 'delegate',
                params: {},
                providerExecuted: false,
              },
              {
                type: 'tool-call' as const,
                id: 'sibling-call',
                name: 'sibling',
                params: {},
                providerExecuted: false,
              },
              { ...finish, reason: 'tool-calls' as const },
            ]
          }),
      })
      const open = (scope: Scope.Scope, fault = false) =>
        Effect.gen(function* () {
          let storage = memory ?? (yield* Layer.buildWithScope(layer, scope))
          if (fault) {
            const actual = Context.get(storage, Storage.Storage)
            const format = Context.get(storage, StorageFormat.StorageFormat)
            const captured: Storage.Storage['Service'] = {
              ...actual,
              commit: (input) => {
                const writes = [...input]
                const target =
                  !injected &&
                  writes.some(
                    (w) =>
                      w._tag === 'task' &&
                      w.value.kind === 'harness.tool' &&
                      Schema.is(
                        Schema.Struct({
                          status: Schema.tag('aborted'),
                          reason: Schema.Literal('unsafe-subagent-recovery'),
                        }),
                      )(w.value.state.outcome),
                  )
                if (!target) return actual.commit(writes)
                injected = true
                const reject = Effect.failCause(failure).pipe(
                  Effect.onExit((exit) =>
                    Effect.sync(() => {
                      if (Exit.isFailure(exit)) rawFailure = exit.cause
                    }),
                  ),
                )
                return scenario === 'uncertain'
                  ? actual.commit(writes).pipe(Effect.andThen(reject))
                  : reject
              },
            }
            storage = storage.pipe(
              Context.add(Storage.Storage, captured),
              Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
            )
          }
          return yield* HarnessRuntime.make({
            tools: toolkit,
            hooks: [
              Hook.make({
                event: 'afterTools',
                execute: () =>
                  Effect.sync(() => {
                    afterTools++
                  }),
              }),
              Hook.make({
                event: 'afterTool',
                execute: ({ call }) =>
                  Effect.gen(function* () {
                    if (scenario === 'observed' && call.name === 'delegate' && warm) {
                      yield* Deferred.succeed(observedReady, undefined)
                      return yield* Effect.never
                    }
                  }),
              }),
            ],
          }).pipe(
            Effect.provideContext(storage),
            Effect.provideService(Scope.Scope, scope),
            Effect.provideService(LanguageModel.LanguageModel, provider),
            Effect.provide(
              toolkit.toLayer({
                delegate: () =>
                  Effect.gen(function* () {
                    handlers++
                    late = yield* ToolExecution
                    yield* late.output('accepted output\n')
                    yield* late.details({ accepted: true })
                    saved = yield* Subagent.spawn(late, {
                      draft: { type: 'input', content: 'paid child' },
                      options: { key: 'saved' },
                    })
                    const paid = yield* Subagent.wait(late, saved)
                    if (paid.receipt === undefined)
                      return yield* Effect.die('Missing paid child receipt')
                    yield* Deferred.succeed(paidReady, paid.receipt)
                    if (scenario !== 'observed') return yield* Effect.never
                    return {
                      content: [Prompt.textPart({ text: 'observed' })],
                      structuredOutput: 'observed',
                      spend: { amount: 3, currency: 'USD', source: 'observed-tool' },
                      control: { terminate: true as const },
                    }
                  }).pipe(Effect.orDie),
                sibling: () =>
                  Effect.gen(function* () {
                    siblingHandlers++
                    yield* Deferred.succeed(siblingEntered, undefined)
                    if (scenario === 'parallel' && warm)
                      return yield* Effect.never.pipe(
                        Effect.ensuring(
                          Effect.uninterruptible(
                            Deferred.succeed(closing, undefined).pipe(
                              Effect.andThen(Deferred.await(release)),
                            ),
                          ),
                        ),
                      )
                    return {
                      content: [],
                      structuredOutput: 'sibling',
                      spend: { amount: 2, currency: 'USD', source: 'sibling' },
                      control: { terminate: true as const },
                    }
                  }),
              }),
            ),
          )
        })
      const firstScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
      yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
      const first = yield* open(firstScope)
      const root = yield* first.backend.root
      const job = yield* first.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'parent' },
      })
      const paid = yield* Deferred.await(paidReady)
      if (scenario === 'observed') yield* Deferred.await(observedReady)
      if (saved === undefined || late === undefined)
        return yield* Effect.die('Missing native handle')
      const originalHandle = saved
      const originalUsage = yield* Session.commit(first.session, (tx) =>
        Usage.receipts(tx, paid.conversationId),
      )
      const originalEntries = yield* Session.scanEntries(first.session, {
        conversationId: paid.conversationId,
      }).pipe(Stream.runCollect)
      assert.strictEqual(entries, 2)
      assert.strictEqual(handlers, 1)
      if (scenario === 'foreign' || scenario === 'stale')
        yield* Session.commit(first.session, (tx) =>
          Effect.gen(function* () {
            const target = { scope: { _tag: 'task' as const, taskId: late!.taskId } }
            const found = yield* Transaction.snapshot(tx, admissions, target)
            if (Option.isNone(found)) return yield* Effect.die('Missing actual admission index')
            const slot = found.value.value.slots.saved!
            const handle =
              scenario === 'foreign'
                ? { ...slot.handle, originTaskId: slot.handle.taskId }
                : { ...slot.handle, taskId: Record.TaskId.make(999999) }
            yield* Transaction.setDocument(tx, admissions, target, {
              slots: { saved: { ...slot, handle } },
            })
          }),
        )
      if (scenario === 'parallel') {
        yield* Deferred.await(siblingEntered)
        const sibling = (yield* Session.scanTasks(first.session).pipe(Stream.runCollect)).find(
          (t) => t.kind === 'harness.tool' && t.id !== late!.taskId,
        )!
        assert.notStrictEqual(sibling.state.status, 'terminal')
        const parent = yield* Session.commit(first.session, (tx) =>
          Transaction.submission(tx, job.id),
        )
        assert.isTrue(Option.isSome(parent))
        if (Option.isSome(parent)) assert.strictEqual(parent.value.status, 'placed')
        const closed = yield* Deferred.make<void>()
        const close = yield* Scope.close(firstScope, Exit.void).pipe(
          Effect.andThen(Deferred.succeed(closed, undefined)),
          Effect.forkChild,
        )
        yield* Deferred.await(closing)
        yield* Effect.yieldNow
        assert.isFalse(yield* Deferred.isDone(closed))
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(close)
      } else {
        yield* Scope.close(firstScope, Exit.void)
      }
      const revoked = yield* Effect.exit(Subagent.read(late, originalHandle))
      assert.isTrue(Exit.isFailure(revoked))
      warm = false
      const secondScope = yield* Scope.make()
      yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
      const second = yield* open(secondScope, scenario === 'rejected' || scenario === 'uncertain')
      const result = yield* Effect.exit(second.backend.wait(job.id))
      if (scenario === 'foreign' || scenario === 'stale') {
        assert.isTrue(Exit.isFailure(result))
        assert.strictEqual(entries, 2)
        assert.strictEqual(handlers, 1)
        return
      }
      if (scenario === 'rejected' || scenario === 'uncertain') {
        assert.isTrue(Exit.isFailure(result))
        assert.isTrue(injected)
        assert.deepStrictEqual(rawFailure?.reasons, failure.reasons)
        yield* Scope.close(secondScope, Exit.void)
      } else {
        assert.isTrue(Exit.isSuccess(result))
        if (Exit.isFailure(result)) return yield* Effect.failCause(result.cause)
        assert.strictEqual(result.value.status, scenario === 'observed' ? 'done' : 'unanswered')
        assert.isTrue(Option.isNone(yield* second.scheduler.failure))
        yield* Scope.close(secondScope, Exit.void)
      }
      const third = yield* open(yield* Effect.scope)
      const settled = yield* third.backend.wait(job.id)
      assert.strictEqual(settled.status, scenario === 'observed' ? 'done' : 'unanswered')
      assert.strictEqual(entries, 2, 'Warm provider2, cold provider0 including terminal reopen')
      assert.strictEqual(handlers, 1, 'Unsafe handler is not replayed')
      let expectedSiblings = 1
      if (scenario === 'sequential') expectedSiblings = 0
      else if (scenario === 'parallel') expectedSiblings = 2
      assert.strictEqual(siblingHandlers, expectedSiblings)
      assert.strictEqual(afterTools, scenario === 'observed' ? 1 : 0)
      assert.deepStrictEqual(yield* third.backend.wait(paid.submissionId), paid.submission)
      assert.deepStrictEqual(
        yield* Session.commit(third.session, (tx) => Usage.receipts(tx, paid.conversationId)),
        originalUsage,
      )
      assert.deepStrictEqual(
        yield* Session.scanEntries(third.session, { conversationId: paid.conversationId }).pipe(
          Stream.runCollect,
        ),
        originalEntries,
      )
      const parentReceipts = yield* Session.commit(third.session, (tx) => Usage.receipts(tx, root))
      assert.lengthOf(
        parentReceipts.filter((r) => r.identity._tag === 'requestAttempt'),
        1,
      )
      const receipt = parentReceipts.find(
        (r) => r.identity._tag === 'toolCall' && r.identity.taskId === late!.taskId,
      )!
      assert.strictEqual(receipt.identity._tag, 'toolCall')
      if (receipt.identity._tag === 'toolCall') assert.strictEqual(receipt.identity.attempt, 0)
      assert.strictEqual(
        receipt.outcome._tag,
        scenario === 'observed' ? 'completed' : 'interrupted',
      )
      if (scenario !== 'observed') {
        assert.isUndefined(receipt.usage)
        assert.isUndefined(receipt.linkedEntryId)
        const progress = yield* Session.snapshot(third.session, ToolProgress.document, {
          scope: { _tag: 'conversation', conversationId: root },
          key: `tool:${late.taskId}`,
        })
        assert.isTrue(Option.isSome(progress))
        if (Option.isSome(progress)) {
          assert.strictEqual(progress.value.value.status, 'interrupted')
          assert.strictEqual(progress.value.value.attempt, 0)
          assert.strictEqual(progress.value.value.output.text, 'accepted output\n')
          assert.deepStrictEqual(progress.value.value.details, { accepted: true })
        }
      } else
        assert.strictEqual(
          receipt.usage?._tag === 'tool' ? receipt.usage.cost?.amount : undefined,
          3,
        )
      const parentEntries = yield* Session.scanEntries(third.session, {
        conversationId: root,
      }).pipe(Stream.runCollect)
      assert.lengthOf(
        parentEntries.filter((e) => e.kind === 'tool.result' && e.byTaskId === late!.taskId),
        scenario === 'observed' ? 1 : 0,
      )
      if (scenario !== 'sequential')
        assert.strictEqual(
          parentReceipts.find((r) => r.usage?._tag === 'tool' && r.usage.name === 'sibling')?.usage
            ?._tag,
          'tool',
        )
      assert.isTrue(Option.isNone(yield* third.scheduler.failure))
    }),
  ).pipe(Effect.provide(services))

describe('native tool subagent cold disposition', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const scenario of [
      'parallel',
      'sequential',
      'observed',
      'foreign',
      'stale',
      'rejected',
      'uncertain',
    ] as const)
      it.live(
        `${backend}/${scenario}: preserve authority, accounting and native lifetime without model continuation`,
        () => cold(backend, scenario),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const domainFailure of [false, true])
      it.live(
        `${backend}/live: preserve ${domainFailure ? 'declared failure and continuation' : 'successful termination'} after paid subagent work`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const layer = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/live.jsonl` })
                return Storage.layerSql.pipe(
                  Layer.provide(sqlite({ filename: `${directory}/live.sqlite` })),
                )
              })()
              const storage = yield* Layer.build(layer)
              let requests = 0
              let parentRequests = 0
              let handlers = 0
              let paid: Subagent.Receipt | undefined
              const toolkit = Toolkit.make(
                Tool.makeResult('live', {
                  success: Schema.String,
                  failure: Schema.String,
                  replay: 'unsafe',
                }),
              )
              const provider = yield* ScriptedStream.make({
                generateText: ({ prompt }) =>
                  Effect.sync(() => {
                    requests++
                    if (
                      prompt.content.some(
                        (m) =>
                          m.role === 'user' &&
                          m.content.some((p) => p.type === 'text' && p.text === 'paid child'),
                      )
                    )
                      return [{ type: 'text' as const, text: 'paid' }, finish]
                    parentRequests++
                    if (parentRequests > 1)
                      return [{ type: 'text' as const, text: 'continued' }, finish]
                    return [
                      {
                        type: 'tool-call' as const,
                        id: 'live',
                        name: 'live',
                        params: {},
                        providerExecuted: false,
                      },
                      { ...finish, reason: 'tool-calls' as const },
                    ]
                  }),
              })
              const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
                Effect.provideContext(storage),
                Effect.provideService(LanguageModel.LanguageModel, provider),
                Effect.provide(
                  toolkit.toLayer({
                    live: () =>
                      Effect.gen(function* () {
                        handlers++
                        const access = yield* ToolExecution
                        const handle = yield* Subagent.spawn(access, {
                          draft: { type: 'input', content: 'paid child' },
                          options: { key: 'live' },
                        }).pipe(Effect.orDie)
                        const result = yield* Subagent.wait(access, handle).pipe(Effect.orDie)
                        paid = result.receipt
                        const output = {
                          content: [Prompt.textPart({ text: 'live result' })],
                          structuredOutput: 'declared',
                          spend: { amount: 4, currency: 'USD', source: 'live' },
                          control: { terminate: true as const },
                        }
                        return domainFailure ? yield* Effect.fail(output) : output
                      }),
                  }),
                ),
              )
              const root = yield* runtime.backend.root
              const job = yield* runtime.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'parent' },
              })
              const settled = yield* runtime.backend.wait(job.id)
              assert.strictEqual(settled.status, 'done')
              assert.strictEqual(requests, domainFailure ? 3 : 2)
              assert.strictEqual(parentRequests, domainFailure ? 2 : 1)
              assert.strictEqual(handlers, 1)
              assert.isDefined(paid)
              if (paid === undefined) return yield* Effect.die('Missing live paid receipt')
              assert.deepStrictEqual(
                yield* runtime.backend.wait(paid.submissionId),
                paid.submission,
              )
              const receipts = yield* Session.commit(runtime.session, (tx) =>
                Usage.receipts(tx, root),
              )
              const tool = receipts.find((r) => r.identity._tag === 'toolCall')!
              assert.strictEqual(tool.outcome._tag, domainFailure ? 'failed' : 'completed')
              assert.strictEqual(
                tool.usage?._tag === 'tool' ? tool.usage.cost?.amount : undefined,
                4,
              )
              assert.isDefined(tool.linkedEntryId)
              assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
            }),
          ).pipe(Effect.provide(services)),
      )
})
