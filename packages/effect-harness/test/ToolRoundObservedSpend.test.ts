import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
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
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import type * as Record from '../src/Record.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as StorageFormat from '../src/StorageFormat.js'
import { StorageError } from '../src/StorageError.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as ToolProgress from '../src/ToolProgress.js'
import * as Usage from '../src/Usage.js'
import * as Round from '../src/internal/ToolRound.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const cost = { amount: 17, currency: 'USD', source: 'observed-handler' }
const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const scenarios = [
  ...(['missing', 'incompatible', 'revoked', 'compatible'] as const).flatMap((binding) =>
    [true, false].map((priced) => ({ binding, priced, mode: 'ordinary' as const })),
  ),
  { binding: 'compatible', priced: true, mode: 'remove' },
  { binding: 'compatible', priced: true, mode: 'reprice' },
  { binding: 'missing', priced: true, mode: 'sibling' },
  { binding: 'missing', priced: true, mode: 'rollback' },
  { binding: 'incompatible', priced: true, mode: 'uncertain' },
  { binding: 'compatible', priced: true, mode: 'fatal' },
  { binding: 'compatible', priced: true, mode: 'cancel' },
] as const

describe('checkpoint observed spend during cold denial', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const { binding, priced, mode } of scenarios)
      it.live(`${backend}: ${binding}, ${priced ? 'observed' : 'unreported'} spend, ${mode}`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const filename = `${directory}/cold.${backend}`
            const memory = yield* Layer.build(Storage.layerMemory)
            const open = (scope: Scope.Scope) => {
              if (backend === 'memory') return Effect.succeed(memory)
              const layer =
                backend === 'sqlite'
                  ? Storage.layerJournal().pipe(
                      Layer.provide(SqlEventJournal.layer()),
                      Layer.provide(SqliteClient.layer({ filename })),
                    )
                  : Storage.layerJsonl({ filePath: filename })
              return Layer.buildWithScope(layer, scope)
            }
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const firstContext = yield* open(firstScope)
            const entered = yield* Deferred.make<void>()
            let handlers = 0,
              replacements = 0,
              requests = 0,
              before = 0,
              after = 0,
              finalizers = 0
            const charged = Tool.makeResult('charged', {
              success: Schema.String,
              replay: 'unsafe',
              executionMode: 'parallel',
            })
            const sibling = Tool.makeResult('sibling', {
              success: Schema.String,
              replay: 'unsafe',
              executionMode: 'parallel',
            })
            const toolkit = Toolkit.make(charged, sibling)
            const model = yield* ScriptedStream.make({
              generateText: () =>
                Effect.succeed(
                  ++requests === 1
                    ? [
                        {
                          type: 'tool-call' as const,
                          id: 'provider-charge',
                          name: 'charged',
                          params: {},
                          providerExecuted: false,
                        },
                        ...(mode === 'sibling'
                          ? [
                              {
                                type: 'tool-call' as const,
                                id: 'provider-sibling',
                                name: 'sibling',
                                params: {},
                                providerExecuted: false,
                              },
                            ]
                          : []),
                        { ...finish, reason: 'tool-calls' as const },
                      ]
                    : [{ type: 'text' as const, text: 'done' }, finish],
                ),
            })
            const first = yield* HarnessRuntime.make({
              tools: toolkit,
              hooks: [
                Hook.make({
                  event: 'beforeTool',
                  execute: () =>
                    Effect.sync(() => {
                      before++
                    }),
                }),
                Hook.make({
                  event: 'afterTool',
                  execute: ({ call, result }) =>
                    call.name === 'charged'
                      ? Effect.gen(function* () {
                          after++
                          yield* Effect.addFinalizer(() =>
                            Effect.sync(() => {
                              finalizers++
                            }),
                          )
                          yield* Deferred.succeed(entered, undefined)
                          return yield* Effect.never
                        })
                      : Effect.succeed(result),
                }),
              ],
            }).pipe(
              Effect.provideContext(firstContext),
              Effect.provideService(Scope.Scope, firstScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  charged: () =>
                    Effect.gen(function* () {
                      handlers++
                      yield* Effect.addFinalizer(() =>
                        Effect.sync(() => {
                          finalizers++
                        }),
                      )
                      return {
                        content: [],
                        structuredOutput: 'private-saved-result',
                        details: { private: 'secret' },
                        control: { terminate: true },
                        ...(priced ? { spend: cost } : {}),
                      }
                    }),
                  sibling: () =>
                    Effect.succeed({
                      content: [],
                      structuredOutput: 'sibling',
                      control: { terminate: true },
                      spend: { amount: 3, currency: 'USD', source: 'sibling' },
                    }),
                }),
              ),
            )
            const root = yield* first.backend.root
            const job = yield* first.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'charge' },
            })
            yield* Deferred.await(entered)
            const tasks = yield* Session.scanTasks(first.session).pipe(Stream.runCollect)
            const saved = tasks.find(
              (t) =>
                t.kind === 'harness.tool' &&
                Schema.is(
                  Schema.Struct({ call: Schema.Struct({ id: Schema.Literal('provider-charge') }) }),
                )(t.input),
            )!
            const checkpoint = yield* Schema.decodeUnknownEffect(Round.CheckpointSchema)(
              saved.state.checkpoint,
            )
            assert.deepStrictEqual(checkpoint.observedResult?.spend, priced ? cost : undefined)
            assert.isTrue(checkpoint.started)
            assert.strictEqual(checkpoint.attempt, 0)
            assert.strictEqual(checkpoint.acceptedCall?.id, 'provider-charge')
            const parent = tasks.find((t) => t.id === saved.owner)!
            const { roundKey } = yield* Schema.decodeUnknownEffect(
              Schema.Struct({ roundKey: Schema.String }),
            )(parent.state.checkpoint)
            if (mode === 'sibling') {
              const siblingTask = tasks.find((t) => t.kind === 'harness.tool' && t.id !== saved.id)!
              yield* first.scheduler.waitForTask(siblingTask.id)
            }
            if (mode === 'cancel') {
              yield* first.backend.abort(root)
              assert.strictEqual((yield* first.backend.wait(job.id)).status, 'unanswered')
            }
            yield* Scope.close(firstScope, Exit.void)
            assert.strictEqual(finalizers, 2)
            const missing = Toolkit.make(sibling)
            const next = Toolkit.make(
              Tool.makeResult('charged', {
                success: binding === 'incompatible' ? Schema.Finite : Schema.String,
                replay: 'unsafe',
                executionMode: 'parallel',
                ...(binding === 'revoked' ? { callers: ['tools'] as const } : {}),
              }),
              sibling,
            )
            const nextHandlers = Layer.mergeAll(
              next.toLayer({
                charged: () =>
                  Effect.gen(function* () {
                    replacements++
                    return yield* Effect.die('Replacement handler invoked')
                  }),
                sibling: () => Effect.die('Committed sibling replayed'),
              }),
              missing.toLayer({ sibling: () => Effect.die('Committed sibling replayed') }),
            )
            const batches: ReadonlyArray<Record.StorageWrite>[] = []
            let injected = false
            const failure = new StorageError({
              reason: mode === 'uncertain' ? 'uncertain' : 'io',
              operation: 'test.cold-final',
              message: 'cold denial settlement fault',
            })
            const secondScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
            const secondContext = yield* open(secondScope)
            const actual = Context.get(secondContext, Storage.Storage)
            const format = Context.get(secondContext, StorageFormat.StorageFormat)
            const captured: Storage.Storage['Service'] = {
              ...actual,
              commit: (writes) => {
                const batch = [...writes]
                const final = batch.some(
                  (w) => w._tag === 'entry' && w.value.kind === 'tool.result',
                )
                if ((mode === 'rollback' || mode === 'uncertain') && final && !injected) {
                  injected = true
                  if (mode === 'uncertain')
                    return actual.commit(batch).pipe(Effect.andThen(failure))
                  return Effect.fail(failure)
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
            const services = secondContext.pipe(
              Context.add(Storage.Storage, captured),
              Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
            )
            const hooks =
              mode === 'remove' || mode === 'reprice' || mode === 'fatal'
                ? [
                    Hook.make({
                      event: 'afterTool',
                      execute: ({ result }) => {
                        const { spend: _spend, ...withoutSpend } = result
                        if (mode === 'remove') return Effect.succeed(withoutSpend)
                        if (mode === 'reprice')
                          return Effect.succeed({
                            ...result,
                            spend: { amount: 5, currency: 'EUR', source: 'after-hook' },
                          })
                        return Effect.succeed({ ...result, structuredOutput: 42 })
                      },
                    }),
                  ]
                : []
            const second = yield* (
              binding === 'missing'
                ? HarnessRuntime.make({ tools: missing, hooks })
                : HarnessRuntime.make({ tools: next, hooks })
            ).pipe(
              Effect.provideContext(services),
              Effect.provideService(Scope.Scope, secondScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(nextHandlers),
            )
            yield* second.backend.root
            if (mode === 'rollback' || mode === 'uncertain') {
              assert.strictEqual(
                (yield* second.backend.wait(job.id).pipe(Effect.flip))._tag,
                'HarnessError',
              )
              assert.isTrue(injected)
              if (mode === 'uncertain')
                assert.strictEqual(
                  yield* Session.commit(second.session, () => Effect.void).pipe(Effect.flip),
                  failure,
                )
              const durable = Option.getOrThrow(yield* actual.task(saved.id))
              assert.strictEqual(durable.state.status, mode === 'rollback' ? 'running' : 'terminal')
              const entries = yield* actual
                .scanEntries({ conversationId: root })
                .pipe(Stream.runCollect)
              assert.strictEqual(
                entries.filter((e) => e.kind === 'tool.result').length,
                Number(mode === 'uncertain'),
              )
              if (mode === 'rollback')
                assert.deepStrictEqual(
                  (yield* Schema.decodeUnknownEffect(Round.CheckpointSchema)(
                    durable.state.checkpoint,
                  )).observedResult?.spend,
                  cost,
                )
            } else
              assert.strictEqual(
                (yield* second.backend.wait(job.id)).status,
                mode === 'cancel' || mode === 'fatal' ? 'unanswered' : 'done',
              )
            yield* Scope.close(secondScope, Exit.void)
            // Reopen independently again: deterministic rollback resumes the original checkpoint,
            // uncertain acknowledgement reconciles the committed child, and terminal receipts deduplicate.
            const third = yield* (
              binding === 'missing'
                ? HarnessRuntime.make({ tools: missing })
                : HarnessRuntime.make({ tools: next })
            ).pipe(
              Effect.provideContext(yield* open(yield* Effect.scope)),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(nextHandlers),
            )
            yield* third.backend.root
            assert.strictEqual(
              (yield* third.backend.wait(job.id)).status,
              mode === 'cancel' || mode === 'fatal' ? 'unanswered' : 'done',
            )
            const receipts = (yield* Session.commit(third.session, (tx) =>
              Usage.receipts(tx, root),
            )).filter((r) => r.identity._tag === 'toolCall')
            const receipt = receipts.find(
              (r) => r.identity._tag === 'toolCall' && r.identity.taskId === saved.id,
            )!
            assert.strictEqual(receipts.length, mode === 'sibling' ? 2 : 1)
            assert.deepStrictEqual(receipt.identity, {
              _tag: 'toolCall',
              taskId: saved.id,
              callId: 'provider-charge',
              attempt: Usage.AttemptId.make(0),
            })
            let expectedCost: Usage.Cost | undefined = priced ? cost : undefined
            if (mode === 'remove') expectedCost = undefined
            else if (mode === 'reprice')
              expectedCost = { amount: 5, currency: 'EUR', source: 'after-hook' }
            assert.deepStrictEqual(receipt.usage?.cost, expectedCost)
            const totals = yield* Session.usage(third.session, root)
            assert.strictEqual(
              totals.costs.find((c) => c.currency === (mode === 'reprice' ? 'EUR' : 'USD'))?.tool,
              expectedCost === undefined
                ? undefined
                : expectedCost.amount + (mode === 'sibling' ? 3 : 0),
            )
            const round = yield* Session.commit(third.session, (tx) =>
              Round.receipt(tx, root, roundKey),
            )
            const slot = round.slots[0]!
            const denied = binding !== 'compatible'
            let disposition = 'succeeded'
            if (denied) disposition = 'denied'
            else if (mode === 'cancel') disposition = 'cancelled'
            else if (mode === 'fatal') disposition = 'failed'
            assert.strictEqual(slot.receipt?.status, disposition)
            if (denied) {
              assert.strictEqual(receipt.outcome._tag, 'failed')
              assert.isTrue(slot.result?.isError)
              assert.isUndefined(slot.result?.structuredOutput)
              assert.isUndefined(slot.result?.details)
              assert.isUndefined(slot.result?.control)
              assert.notInclude(JSON.stringify(slot.result), 'private-saved-result')
              assert.notInclude(JSON.stringify(slot.result), 'secret')
            }
            const entries = yield* Session.scanEntries(third.session, {
              conversationId: root,
            }).pipe(Stream.runCollect)
            const entry = entries.find((e) => e.id === receipt.linkedEntryId)
            if (mode !== 'fatal' && mode !== 'cancel') {
              assert.isDefined(entry)
              assert.deepStrictEqual(entry?.usage, receipt.usage)
              if (denied) {
                assert.notInclude(JSON.stringify(entry), 'secret')
                assert.notInclude(JSON.stringify(entry), 'private-saved-result')
              }
            } else assert.isUndefined(entry)
            const finalBatch = batches.find((batch) =>
              batch.some(
                (w) =>
                  w._tag === 'task' &&
                  w.value.id === saved.id &&
                  w.value.state.status === 'terminal',
              ),
            )
            if (mode !== 'rollback' && mode !== 'uncertain' && mode !== 'cancel') {
              assert.isDefined(finalBatch)
              const values = finalBatch!.flatMap((write) =>
                (write._tag === 'document.change' || write._tag === 'document.create') &&
                write.content._tag === 'base'
                  ? [write.content.value]
                  : [],
              )
              const storedReceipt = values
                .flatMap((value) => {
                  const decoded = Schema.decodeUnknownOption(
                    Schema.Struct({ receipt: Usage.ReceiptSchema }),
                  )(value)
                  return Option.isSome(decoded) ? [decoded.value.receipt] : []
                })
                .find(
                  (value) =>
                    value.identity._tag === 'toolCall' && value.identity.taskId === saved.id,
                )
              assert.deepStrictEqual(storedReceipt, receipt)
              const storedRound = values
                .flatMap((value) => {
                  const decoded = Schema.decodeUnknownOption(
                    Schema.Struct({ round: Round.StoredSchema }),
                  )(value)
                  return Option.isSome(decoded) ? [decoded.value.round] : []
                })
                .find((value) => value.ownerTaskId === parent.id)!
              assert.deepStrictEqual(storedRound.slots[0]?.receipt, slot.receipt)
              assert.strictEqual(storedRound.slots[0]?.taskId, slot.taskId)
              assert.strictEqual(storedRound.slots[0]?.entryId, slot.entryId)
              if (entry !== undefined) assert.isUndefined(storedRound.slots[0]?.result)
              const progress = values
                .flatMap((value) => {
                  const decoded = Schema.decodeUnknownOption(ToolProgress.Snapshot)(value)
                  return Option.isSome(decoded) ? [decoded.value] : []
                })
                .find((value) => value.callId === Round.progressKey(saved.id))!
              assert.strictEqual(progress.attempt, 0)
              assert.strictEqual(progress.status, 'completed')
              if (entry !== undefined)
                assert.isTrue(
                  finalBatch!.some(
                    (w) =>
                      w._tag === 'entry' &&
                      w.value.id === entry.id &&
                      JSON.stringify(w.value.usage) === JSON.stringify(receipt.usage),
                  ),
                )
            }
            assert.strictEqual(handlers, 1)
            assert.strictEqual(replacements, 0)
            assert.strictEqual(before, mode === 'sibling' ? 2 : 1)
            assert.strictEqual(after, 1)
            assert.strictEqual(requests, binding === 'compatible' ? 1 : 2)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
})
