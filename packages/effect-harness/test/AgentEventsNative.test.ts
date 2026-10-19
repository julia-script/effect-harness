import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as NativeResponse from 'effect/ai/Response'
import * as Context from 'effect/Context'
import * as Clock from 'effect/Clock'
import * as Schema from 'effect/Schema'
import * as TestClock from 'effect/testing/TestClock'
import * as Model from 'effect-harness/Model'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Prompt from 'effect/ai/Prompt'
import type * as Record from 'effect-harness/Record'
import * as Hook from 'effect-harness/Hook'
import * as Extension from 'effect-harness/Extension'
import * as Session from 'effect-harness/Session'
import * as Transaction from 'effect-harness/Transaction'
import * as Conversation from 'effect-harness/Conversation'
import * as View from 'effect-harness/ConversationView'
import * as Events from 'effect-harness/AgentEvent'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Settings from 'effect-harness/Settings'

import * as Submission from 'effect-harness/Submission'

describe('native live event replication', () => {
  it.live('native generation cleanup preserves an independent held manual compaction', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const summarizing = yield* Deferred.make<void>()
        const summaryRelease = yield* Deferred.make<void>()
        const modelRelease = yield* Deferred.make<void>()
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(summaryRelease, undefined).pipe(
            Effect.andThen(Deferred.succeed(modelRelease, undefined)),
          ),
        )
        let streams = 0
        let summaries = 0
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('Summary supplied by actual hook'),
          streamText: () => {
            streams++
            return Stream.concat(
              Stream.fromIterable<Response.StreamPartEncoded>([
                { type: 'text-start', id: 'independent-live' },
                { type: 'text-delta', id: 'independent-live', delta: 'held live' },
              ]),
              Stream.unwrap(
                Deferred.await(modelRelease).pipe(
                  Effect.as(
                    Stream.fromIterable<Response.StreamPartEncoded>([
                      { type: 'text-end', id: 'independent-live' },
                      {
                        type: 'finish',
                        reason: 'stop',
                        usage: {
                          inputTokens: {
                            total: 3,
                            uncached: 3,
                            cacheRead: undefined,
                            cacheWrite: undefined,
                          },
                          outputTokens: { total: 1, text: 1, reasoning: undefined },
                        },
                        response: undefined,
                      },
                    ]),
                  ),
                ),
              ),
            )
          },
        })
        const settings = yield* Settings.make({
          progress: { partialIntervalMs: 0 },
          compaction: {
            enabled: false,
            reserveTokens: 20,
            keepRecentTokens: 10,
            backgroundTokens: 0,
          },
        })
        const runtime = yield* HarnessRuntime.make({
          extensions: [
            Extension.make({
              name: 'observed-compaction',
              hooks: [
                Hook.make({
                  event: 'beforeCompact',
                  execute: () =>
                    Effect.gen(function* () {
                      summaries++
                      yield* Deferred.succeed(summarizing, undefined)
                      yield* Deferred.await(summaryRelease)
                      return { _tag: 'summary' as const, summary: 'Actual supplied summary' }
                    }),
                }),
              ],
            }),
          ],
        }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provideService(Settings.Settings, settings),
        )
        const client = yield* Harness.make.pipe(
          Effect.provideService(HarnessBackend, runtime.backend),
        )
        const conversation = yield* client.root
        yield* Session.commit(runtime.session, (tx) =>
          Effect.forEach(
            ['old '.repeat(40), 'recent '.repeat(12)],
            (text) =>
              Transaction.appendEntry(tx, conversation.id, {
                kind: 'input',
                model: [{ role: 'user', content: text }],
              }),
            { discard: true },
          ),
        )
        const consumer = yield* Scope.make()
        const pull = yield* Stream.toPull(Events.watch(conversation, { capacity: 4096 })).pipe(
          Effect.provideService(Scope.Scope, consumer),
        )
        const initial = (yield* pull)[0]
        if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial required')
        const compaction = yield* Conversation.compact(conversation)
        yield* Deferred.await(summarizing)
        const submission = yield* Conversation.submit(conversation, {
          type: 'input',
          content: 'Generate while summary is held',
        })
        yield* View.watch(conversation).pipe(
          Stream.filter(
            (frame) =>
              (frame._tag === 'Delta' ? frame.ops[0].value : frame.value).live.generation
                ?.partial !== undefined,
          ),
          Stream.take(1),
          Stream.runDrain,
        )
        const both = yield* View.snapshot(conversation)
        assert.strictEqual(both.value.compactions.items[0]!.taskId, compaction.id)
        assert.isDefined(both.value.live.generation)
        yield* Deferred.succeed(modelRelease, undefined)
        yield* Submission.wait(submission)
        const generationEnded = yield* View.snapshot(conversation)
        assert.deepStrictEqual(generationEnded.value.live, {})
        assert.strictEqual(generationEnded.value.compactions.items[0]!.taskId, compaction.id)
        yield* Deferred.succeed(summaryRelease, undefined)
        assert.strictEqual((yield* runtime.backend.waitCompaction!(compaction)).status, 'completed')
        yield* client.waitForIdle
        const final = yield* View.snapshot(conversation)
        assert.deepStrictEqual(final.value.compactions.items, [])
        assert.isTrue(final.value.entries.some((entry) => entry.kind === 'compaction'))
        let replica = initial
        const events: Events.Event[] = []
        while (replica.revision.cursor < final.revision.cursor)
          for (const frame of yield* pull) {
            if (frame._tag === 'Batch') events.push(...frame.events)
            replica = yield* Events.reduce(replica, frame)
          }
        assert.deepStrictEqual(replica, final)
        assert.strictEqual(events.filter((event) => event._tag === 'CompactionStart').length, 1)
        assert.strictEqual(events.filter((event) => event._tag === 'CompactionEnd').length, 1)
        assert.strictEqual(streams, 1)
        assert.strictEqual(summaries, 1)
        assert.deepStrictEqual(final.value.usage, yield* Conversation.usage(conversation))
        yield* Scope.close(consumer, Exit.void)
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  for (const mode of ['parallel', 'sequential'] as const)
    it.live(
      `actual ${mode} tool slots publish end before linked messages with final progress`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>()
            const secondEntered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
            const toolkit = Toolkit.make(
              Tool.makeResult('observed-first', {
                success: Schema.String,
                replay: 'safe',
                executionMode: mode,
                outputRetention: { retain: 'tail', maxBytes: 64, maxLines: 2 },
              }),
              Tool.makeResult('observed-second', {
                success: Schema.String,
                replay: 'safe',
                executionMode: mode,
              }),
            )
            let requests = 0
            const ids: Record.TaskId[] = []
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('Use stream'),
              streamText: () =>
                Stream.unwrap(
                  Effect.sync(() => {
                    requests++
                    return Stream.fromIterable<Response.StreamPartEncoded>([
                      {
                        type: 'tool-call',
                        id: 'native-first',
                        name: 'observed-first',
                        params: {},
                        providerExecuted: false,
                      },
                      {
                        type: 'tool-call',
                        id: 'native-second',
                        name: 'observed-second',
                        params: {},
                        providerExecuted: false,
                      },
                      {
                        type: 'finish',
                        reason: 'tool-calls',
                        usage: {
                          inputTokens: {
                            total: 2,
                            uncached: 2,
                            cacheRead: undefined,
                            cacheWrite: undefined,
                          },
                          outputTokens: { total: 1, text: 1, reasoning: undefined },
                        },
                        response: undefined,
                      },
                    ])
                  }),
                ),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  'observed-first': () =>
                    Effect.gen(function* () {
                      const execution = yield* ToolExecution
                      ids[0] = execution.taskId
                      yield* execution.output('native retained start\n')
                      yield* execution.details({ actual: 'held first' })
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(release)
                      yield* execution.output('native retained final\n')
                      return {
                        content: [Prompt.textPart({ text: 'first model result' })],
                        structuredOutput: 'first typed result',
                        control: { terminate: true },
                      }
                    }),
                  'observed-second': () =>
                    Effect.gen(function* () {
                      ids[1] = (yield* ToolExecution).taskId
                      yield* Deferred.succeed(secondEntered, undefined)
                      return {
                        content: [Prompt.textPart({ text: 'second model result' })],
                        structuredOutput: 'second typed result',
                        control: { terminate: true },
                      }
                    }),
                }),
              ),
            )
            const client = yield* Harness.make.pipe(
              Effect.provideService(HarnessBackend, runtime.backend),
            )
            const conversation = yield* client.root
            const consumer = yield* Scope.make()
            const pull = yield* Stream.toPull(Events.watch(conversation, { capacity: 4096 })).pipe(
              Effect.provideService(Scope.Scope, consumer),
            )
            const initial = (yield* pull)[0]
            if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial required')
            const submission = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'Run actual tools',
            })
            yield* Deferred.await(entered)
            if (mode === 'parallel') {
              yield* Deferred.await(secondEntered)
              yield* runtime.scheduler.waitForTask(ids[1]!)
            }
            const middle = yield* View.snapshot(conversation)
            assert.strictEqual(middle.value.rounds[0]!.mode, mode)
            assert.strictEqual(middle.value.rounds[0]!.slots[0]!.taskId, ids[0])
            assert.isUndefined(middle.value.rounds[0]!.slots[0]!.result)
            if (mode === 'parallel') assert.isDefined(middle.value.rounds[0]!.slots[1]!.result)
            else assert.isUndefined(ids[1])
            yield* Deferred.succeed(release, undefined)
            yield* Submission.wait(submission)
            yield* client.waitForIdle
            const final = yield* View.snapshot(conversation)
            const events: Events.Event[] = []
            let replica = initial
            while (replica.revision.cursor < final.revision.cursor)
              for (const frame of yield* pull) {
                if (frame._tag === 'Batch') events.push(...frame.events)
                replica = yield* Events.reduce(replica, frame)
              }
            assert.deepStrictEqual(replica, final)
            assert.strictEqual(requests, 1)
            assert.strictEqual(events.filter((event) => event._tag === 'ToolStart').length, 2)
            assert.strictEqual(events.filter((event) => event._tag === 'ToolEnd').length, 2)
            const ends = events.filter((event) => event._tag === 'ToolEnd')
            assert.deepStrictEqual(
              ends.map((event) => event.slot.taskId),
              mode === 'parallel' ? [ids[1], ids[0]] : ids,
            )
            for (const end of ends) {
              assert.isDefined(end.slot.entryId)
              const linkedMessage = events.findIndex(
                (event) => event._tag === 'Message' && event.entry.id === end.slot.entryId,
              )
              assert.isAtLeast(linkedMessage, 0)
              assert.isBelow(events.indexOf(end), linkedMessage)
            }
            assert.isTrue(
              final.value.progress.some((item) =>
                JSON.stringify(item.value).includes('native retained final'),
              ),
            )
            assert.deepStrictEqual(final.value.usage, yield* Conversation.usage(conversation))
            assert.strictEqual(
              final.value.usage.tools.reduce((sum, tool) => sum + tool.results, 0),
              2,
            )
            assert.deepStrictEqual(final.value.graph.tasks, [])
            yield* Scope.close(consumer, Exit.void)
          }),
        ).pipe(Effect.provide(TemporaryStorage.layer)),
    )

  it.effect(
    'deferred create and poll expose the pinned actual phase and attempt without a stream',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let starts = 0
          let polls = 0
          let streams = 0
          const identities: Model.DeferredIdentity[] = []
          const adapter = yield* Model.makeDeferred({
            handle: Schema.String,
            start: (request) =>
              Effect.gen(function* () {
                starts++
                identities.push({ requestId: request.requestId, attempt: request.attempt })
                return {
                  _tag: 'Deferred' as const,
                  handle: 'native-observed-handle',
                  nextPollAt: (yield* Clock.currentTimeMillis) + 10,
                }
              }),
            poll: (handle, previous, request) =>
              Effect.sync(() => {
                polls++
                assert.strictEqual(handle, 'native-observed-handle')
                assert.strictEqual(previous, 10)
                identities.push({ requestId: request.requestId, attempt: request.attempt })
                return {
                  _tag: 'Complete' as const,
                  response: new LanguageModel.GenerateTextResponse([
                    NativeResponse.makePart('text', { text: 'actual deferred answer' }),
                    NativeResponse.makePart('finish', {
                      reason: 'stop',
                      usage: {
                        inputTokens: {
                          total: 13,
                          uncached: 13,
                          cacheRead: undefined,
                          cacheWrite: undefined,
                        },
                        outputTokens: { total: 7, text: 7, reasoning: undefined },
                      },
                    }),
                  ]),
                }
              }),
            cancel: () => Effect.void,
          })
          const languageModel = yield* LanguageModel.make({
            generateText: () => Effect.die('No ordinary generation'),
            streamText: () => {
              streams++
              return Stream.die('No native stream')
            },
          })
          const descriptor = Model.make({
            definition: {
              ref: { provider: 'custom', modelId: 'observed-deferred' },
              capabilities: {
                tools: true,
                reasoning: true,
                images: false,
                structuredOutput: false,
              },
            },
            options: Schema.JsonObject,
            configure: () => Effect.succeed(Context.empty()),
            languageModel,
            deferred: adapter,
          })
          const settings = yield* Settings.make({ stream: { deferred: true } })
          const runtime = yield* HarnessRuntime.make({ models: [descriptor] }).pipe(
            Effect.provideService(Settings.Settings, settings),
          )
          const client = yield* Harness.make.pipe(
            Effect.provideService(HarnessBackend, runtime.backend),
          )
          const conversation = yield* client.root
          const consumer = yield* Scope.make()
          const pull = yield* Stream.toPull(Events.watch(conversation, { capacity: 4096 })).pipe(
            Effect.provideService(Scope.Scope, consumer),
          )
          const initial = (yield* pull)[0]
          if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial required')
          const submission = yield* Conversation.submit(conversation, {
            type: 'input',
            content: 'Observe the deferred request',
          })
          yield* View.watch(conversation).pipe(
            Stream.filter((frame) =>
              (frame._tag === 'Delta' ? frame.ops[0].value : frame.value).generations.some(
                (generation) => generation.phase === 'deferred',
              ),
            ),
            Stream.take(1),
            Stream.runDrain,
          )
          const late = yield* View.snapshot(conversation)
          assert.strictEqual(late.value.generations[0]!.phase, 'deferred')
          assert.strictEqual(late.value.generations[0]!.pollAt, 10)
          assert.strictEqual(late.value.generations[0]!.attempt, identities[0]!.attempt)
          assert.strictEqual(starts, 1)
          assert.strictEqual(polls, 0)
          assert.strictEqual(streams, 0)
          yield* TestClock.adjust(10)
          assert.strictEqual((yield* Submission.wait(submission)).status, 'done')
          yield* client.waitForIdle
          const final = yield* View.snapshot(conversation)
          let replica = initial
          const phases: string[] = []
          while (replica.revision.cursor < final.revision.cursor)
            for (const frame of yield* pull) {
              if (frame._tag === 'Batch')
                for (const event of frame.events)
                  if (event._tag === 'Phase') phases.push(event.generation.phase)
              replica = yield* Events.reduce(replica, frame)
            }
          assert.deepStrictEqual(replica, final)
          assert.include(phases, 'deferred-start')
          assert.include(phases, 'deferred')
          assert.deepStrictEqual(identities[1], identities[0])
          assert.strictEqual(starts, 1)
          assert.strictEqual(polls, 1)
          assert.strictEqual(streams, 0)
          assert.strictEqual(final.value.usage.models[0]!.tokens.input, 13)
          yield* Scope.close(consumer, Exit.void)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  for (const parameters of [false, true])
    it.live(
      `late partial snapshot and ${parameters ? 'actual abort without tool intent' : 'ordered successful message/turn/run'}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const release = yield* Deferred.make<void>()
            let providers = 0
            const parts: Response.StreamPartEncoded[] = [
              { type: 'text-start', id: 'actual-text' },
              { type: 'text-delta', id: 'actual-text', delta: 'committed partial' },
              {
                type: 'tool-params-start',
                id: 'unstarted-tool',
                name: 'never-invoke',
                providerExecuted: false,
              },
              { type: 'tool-params-delta', id: 'unstarted-tool', delta: '{"unusable":' },
            ]
            if (!parameters) parts.splice(2)
            const finish: Response.StreamPartEncoded = {
              type: 'finish',
              reason: 'stop',
              usage: {
                inputTokens: { total: 5, uncached: 5, cacheRead: undefined, cacheWrite: undefined },
                outputTokens: { total: 3, text: 3, reasoning: undefined },
              },
              response: undefined,
            }
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('Use native streaming'),
              streamText: () =>
                Stream.unwrap(
                  Effect.sync(() => {
                    providers++
                    return Stream.concat(
                      Stream.fromIterable(parts),
                      Stream.unwrap(
                        Deferred.await(release).pipe(
                          Effect.as(
                            Stream.fromIterable<Response.StreamPartEncoded>([
                              { type: 'text-delta', id: 'actual-text', delta: ' final' },
                              { type: 'text-end', id: 'actual-text' },
                              finish,
                            ]),
                          ),
                        ),
                      ),
                    )
                  }),
                ),
            })
            const settings = yield* Settings.make({ progress: { partialIntervalMs: 0 } })
            const runtime = yield* HarnessRuntime.make().pipe(
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provideService(Settings.Settings, settings),
            )
            const client = yield* Harness.make.pipe(
              Effect.provideService(HarnessBackend, runtime.backend),
            )
            const conversation = yield* client.root
            const consumer = yield* Scope.make()
            const eventPull = yield* Stream.toPull(
              Events.watch(conversation, { capacity: 4096 }),
            ).pipe(Effect.provideService(Scope.Scope, consumer))
            const initial = (yield* eventPull)[0]
            if (initial._tag !== 'Snapshot') return yield* Effect.die('Initial snapshot required')
            const submission = yield* Conversation.submit(conversation, {
              type: 'input',
              content: 'Observe the committed stream',
            })
            yield* View.watch(conversation).pipe(
              Stream.filter((frame) => {
                const value = frame._tag === 'Delta' ? frame.ops[0].value : frame.value
                const partial = value.live.generation?.partial
                if (parameters) return partial?.parameters[0]?.text === '{"unusable":'
                return (
                  partial?.message.content.some(
                    (message) =>
                      message.role === 'assistant' &&
                      message.content.some(
                        (part) => part.type === 'text' && part.text === 'committed partial',
                      ),
                  ) === true
                )
              }),
              Stream.take(1),
              Stream.runDrain,
            )
            const late = yield* View.snapshot(conversation)
            assert.strictEqual(late.value.generations[0]!.phase, 'streaming')
            if (parameters)
              assert.strictEqual(
                late.value.live.generation!.partial!.parameters[0]!.text,
                '{"unusable":',
              )
            assert.strictEqual(late.value.rounds.length, 0)
            assert.strictEqual(late.value.nested.length, 0)
            assert.strictEqual(providers, 1)
            if (parameters) yield* Conversation.abort(conversation)
            yield* Deferred.succeed(release, undefined)
            yield* Submission.wait(submission)
            yield* client.waitForIdle
            const final = yield* View.snapshot(conversation)
            assert.deepStrictEqual(final.value.live, {})
            assert.deepStrictEqual(final.value.run, {})
            assert.deepStrictEqual(final.value.graph.tasks, [])
            assert.deepStrictEqual(final.value.usage, yield* Conversation.usage(conversation))
            if (parameters) {
              assert.strictEqual(final.value.terminal[0]!.status, 'aborted')
              assert.isAtLeast(final.value.usage.unknownAttempts ?? 0, 1)
            } else {
              assert.strictEqual(final.value.terminal[0]!.status, 'completed')
              assert.strictEqual(final.value.usage.models[0]!.tokens.input, 5)
            }
            const events: Events.Event[] = []
            let replica = initial
            while (replica.revision.cursor < final.revision.cursor)
              for (const frame of yield* eventPull) {
                if (frame._tag === 'Batch') events.push(...frame.events)
                replica = yield* Events.reduce(replica, frame)
              }
            assert.deepStrictEqual(replica, final)
            assert.strictEqual(events.filter((event) => event._tag === 'RunStart').length, 1)
            assert.strictEqual(events.filter((event) => event._tag === 'RunEnd').length, 1)
            assert.strictEqual(events.filter((event) => event._tag === 'GenerationStart').length, 1)
            assert.strictEqual(events.filter((event) => event._tag === 'GenerationEnd').length, 1)
            assert.strictEqual(events.filter((event) => event._tag === 'ToolStart').length, 0)
            assert.strictEqual(events.filter((event) => event._tag === 'TurnStart').length, 1)
            assert.strictEqual(events.filter((event) => event._tag === 'TurnEnd').length, 1)
            const responseAt = events.findIndex(
              (event) => event._tag === 'Message' && event.entry.kind === 'assistant',
            )
            const turnEndAt = events.findIndex((event) => event._tag === 'TurnEnd')
            if (parameters) {
              assert.strictEqual(responseAt, -1)
              assert.isAtLeast(turnEndAt, 0)
            } else {
              assert.isAtLeast(responseAt, 0)
              assert.isAbove(turnEndAt, responseAt)
            }
            assert.isAbove(
              events.findIndex((event) => event._tag === 'RunEnd'),
              turnEndAt,
            )
            yield* Scope.close(consumer, Exit.void)
          }),
        ).pipe(Effect.provide(TemporaryStorage.layer)),
    )
})
