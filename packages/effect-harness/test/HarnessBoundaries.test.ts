import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import type * as Agent from '../src/Agent.ts'
import * as Conversation from '../src/Conversation.ts'
import * as Executor from '../src/Executor.ts'
import * as Harness from '../src/Harness.ts'
import * as Invocation from '../src/Invocation.ts'
import { Persistence } from '../src/Persistence.ts'
import * as Submission from '../src/Submission.ts'
import * as ToolRegistration from '../src/ToolRegistration.ts'
import * as Memory from '../src/storage/Memory.ts'
import * as Native from './embedded/NativeFixture.ts'
import * as State from '../src/internal/ConversationState.ts'
import * as Transcript from '../src/Transcript.ts'
import * as PromptPreparation from '../src/PromptPreparation.ts'

const open = Effect.fn('test.boundaries.open')(function* (
  executor: Executor.Executor['Service'],
  agent: Agent.State = {},
) {
  const store = yield* Memory.make
  return yield* Harness.make({
    cwd: '/host/default',
    agent: { model: Native.ref, ...agent },
    settings: {
      retry: { enabled: false },
      compaction: { enabled: false },
      progress: { partialInterval: '0 millis', outputInterval: '0 millis' },
    },
  }).pipe(
    Effect.provideService(Persistence, store),
    Effect.provideService(Executor.Executor, executor),
  )
})

describe('Harness execution boundaries', () => {
  it.effect('saved invalid model output stays inspectable and is excluded from later context', () =>
    Effect.gen(function* () {
      const { executor } = yield* Native.makeExecutor({
        provider: {
          ...Native.provider,
          streamText: () =>
            Stream.fromIterable([
              { type: 'text-start', id: 'bad' },
              { type: 'text-delta', id: 'bad', delta: 'unvalidated draft' },
              { type: 'text-end', id: 'bad' },
              {
                type: 'tool-call',
                id: 'bad-call',
                name: 'missing',
                params: {},
                providerExecuted: false,
              },
              Native.finish('tool-calls'),
            ]),
        },
      })
      const harness = yield* open(executor)
      const conversation = yield* harness.root
      const settled = yield* Submission.wait(yield* Conversation.submit(conversation, 'hello'))
      assert.strictEqual(settled.status, 'unanswered')
      const entries = yield* Effect.forEach(
        (yield* Conversation.snapshot(conversation)).entries,
        State.projectEntry,
      )
      const failed = entries.filter((entry) => entry.status === 'error')
      assert.strictEqual(failed.length, 1)
      assert.strictEqual(failed[0]?.error?.reason._tag, 'InvalidOutputError')
      const view = Transcript.derive(entries)
      assert.isFalse(view.messages.some((message) => message.role === 'assistant'))
      assert.isTrue(
        view.messages.some(
          (message) =>
            message.role === 'user' &&
            message.content.some(
              (part) => part.type === 'text' && part.text.includes('could not be validated'),
            ),
        ),
      )
    }),
  )

  it.effect(
    'runs creation hooks once per new conversation and afterTools once per committed batch',
    () =>
      Effect.gen(function* () {
        const created = yield* Ref.make(0)
        const batches = yield* Ref.make(0)
        const { executor, registry } = yield* Native.makeExecutor()
        yield* registry.install([
          {
            name: 'lifecycle',
            hooks: [
              {
                operation: 'conversation',
                handlers: { conversationCreated: () => Ref.update(created, (count) => count + 1) },
              },
              {
                operation: 'tool',
                handlers: {
                  afterTools: (results) => {
                    assert.strictEqual(results.length, 1)
                    assert.strictEqual(results[0]?.name, 'uppercase')
                    assert.strictEqual(results[0]?.outcome, 'completed')
                    return Ref.update(batches, (count) => count + 1)
                  },
                },
              },
            ],
          },
        ])
        const store = yield* Memory.make
        const make = Harness.make({
          agent: { model: Native.ref },
          settings: { compaction: { enabled: false }, retry: { enabled: false } },
        }).pipe(
          Effect.provideService(Persistence, store),
          Effect.provideService(Executor.Executor, executor),
        )
        const first = yield* make
        const conversation = yield* first.root
        yield* Submission.wait(yield* Conversation.submit(conversation, 'hello'))
        yield* Conversation.awaitIdle(conversation)
        assert.strictEqual(yield* Ref.get(created), 1)
        assert.strictEqual(yield* Ref.get(batches), 1)
        const entry = (yield* Conversation.snapshot(conversation)).entries.at(-1)
        assert.isDefined(entry)
        if (entry !== undefined) {
          const fork = yield* Conversation.fork(conversation, entry.id)
          yield* Conversation.awaitIdle(fork)
          assert.strictEqual(yield* Ref.get(created), 2, 'fork creation hook')
        }
        const other = yield* first.create
        yield* Conversation.awaitIdle(other)
        assert.strictEqual(
          yield* Ref.get(created),
          3,
          JSON.stringify(
            (yield* Conversation.snapshot(other)).tasks.map((task) => ({
              kind: task.kind,
              state: task.state,
            })),
          ),
        )
        yield* first.close
        const second = yield* make
        yield* second.resume
        yield* second.awaitIdle(conversation.id)
        assert.strictEqual(yield* Ref.get(created), 3)
        assert.strictEqual(yield* Ref.get(batches), 1)
      }),
  )

  it.effect('commits managed instruction patches and applies updated instructions to context', () =>
    Effect.gen(function* () {
      const { executor } = yield* Native.makeExecutor()
      const harness = yield* open(executor, { instructions: 'original instructions' })
      const conversation = yield* harness.root
      yield* Submission.wait(yield* Conversation.submit(conversation, 'first'))
      yield* Conversation.configure(conversation, { instructions: 'updated instructions' })
      yield* Submission.wait(yield* Conversation.submit(conversation, 'second'))
      const entries = yield* Effect.forEach(
        (yield* Conversation.snapshot(conversation)).entries,
        State.projectEntry,
      )
      assert.isTrue(
        entries.some((entry) => entry.kind === 'harness.system' && entry.system !== undefined),
      )
      const view = Transcript.derive(entries)
      const sections = PromptPreparation.replaySections(Transcript.systemPatches(view))
      assert.strictEqual(
        sections.get('instructions'),
        '<instructions>\nupdated instructions\n</instructions>',
      )
    }),
  )

  for (const recover of [true, false]) {
    it.effect(
      `classified overflow compacts once and ${recover ? 'resumes' : 'settles repeated failure'}`,
      () =>
        Effect.gen(function* () {
          const requests = yield* Ref.make(0)
          const compactions = yield* Ref.make(0)
          const { executor } = yield* Native.makeExecutor({
            provider: {
              ...Native.provider,
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(requests, (count) => count + 1).pipe(
                    Effect.map((count) =>
                      recover && count > 0
                        ? Stream.fromIterable(Native.answer('Recovered'))
                        : Stream.fail(
                            new AiError.AiError({
                              module: 'LanguageModel',
                              method: 'streamText',
                              reason: new AiError.InvalidOutputError({
                                description: 'Classified overflow',
                              }),
                            }),
                          ),
                    ),
                  ),
                ),
            },
          })
          const overflowing: Executor.Executor['Service'] = {
            ...executor,
            compactionThreshold: () => Effect.succeedNone,
            classifyFailure: () => Effect.succeed({ overflow: true, retryable: false }),
            prepareCompaction: () =>
              Ref.update(compactions, (count) => count + 1).pipe(
                Effect.as(Executor.CompactionPreparation.none()),
              ),
          }
          const store = yield* Memory.make
          const harness = yield* Harness.make({
            agent: { model: Native.ref },
            settings: { retry: { enabled: false }, compaction: { enabled: true } },
          }).pipe(
            Effect.provideService(Persistence, store),
            Effect.provideService(Executor.Executor, overflowing),
          )
          const conversation = yield* harness.root
          const settled = yield* Submission.wait(yield* Conversation.submit(conversation, 'hello'))
          assert.strictEqual(
            settled.status,
            recover ? 'done' : 'unanswered',
            settled.status === 'unanswered' ? settled.reason : undefined,
          )
          assert.strictEqual(yield* Ref.get(requests), 2)
          assert.strictEqual(yield* Ref.get(compactions), 1)
          const entries = (yield* Conversation.snapshot(conversation)).entries
          const metadata = yield* Effect.forEach(entries, State.projectEntry)
          assert.strictEqual(
            metadata.filter((entry) => entry.status === 'error').length,
            recover ? 1 : 2,
          )
        }),
    )
  }

  for (const threshold of ['blocking', 'background'] as const) {
    it.effect(`${threshold} compaction has the intended model admission boundary`, () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const requests = yield* Ref.make(0)
        const thresholds = yield* Ref.make(0)
        const { executor } = yield* Native.makeExecutor({
          provider: {
            ...Native.provider,
            streamText: () =>
              Stream.unwrap(
                Ref.update(requests, (count) => count + 1).pipe(
                  Effect.as(Stream.fromIterable(Native.answer('Ready'))),
                ),
              ),
          },
        })
        const gated: Executor.Executor['Service'] = {
          ...executor,
          compactionThreshold: () =>
            Ref.getAndUpdate(thresholds, (count) => count + 1).pipe(
              Effect.map((count) => (count === 0 ? Option.some(threshold) : Option.none())),
            ),
          prepareCompaction: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(Executor.CompactionPreparation.none()),
            ),
        }
        const store = yield* Memory.make
        const harness = yield* Harness.make({
          agent: { model: Native.ref },
          settings: { retry: { enabled: false }, compaction: { enabled: true } },
        }).pipe(
          Effect.provideService(Persistence, store),
          Effect.provideService(Executor.Executor, gated),
        )
        const conversation = yield* harness.root
        const submission = yield* Conversation.submit(conversation, 'hello')
        yield* Deferred.await(entered)
        if (threshold === 'blocking') {
          assert.strictEqual(yield* Ref.get(requests), 0)
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* Submission.wait(submission)).status, 'done')
        } else {
          assert.strictEqual((yield* Submission.wait(submission)).status, 'done')
          const snapshot = yield* Conversation.snapshot(conversation)
          assert.isTrue(
            snapshot.tasks.some(
              (task) =>
                task.kind === 'harness.compaction' &&
                task.background &&
                task.state.status !== 'terminal',
            ),
          )
          yield* Deferred.succeed(release, undefined)
        }
        yield* Conversation.awaitIdle(conversation, true)
        assert.strictEqual(yield* Ref.get(requests), 1)
      }),
    )
  }

  it.effect('uses the configured conversation cwd for model and tool hooks', () =>
    Effect.gen(function* () {
      const observed = yield* Ref.make<ReadonlyArray<string>>([])
      const { executor, registry } = yield* Native.makeExecutor()
      const recordCwd = Effect.gen(function* () {
        const invocation = yield* Invocation.Invocation
        yield* Ref.update(observed, (values) => [...values, invocation.cwd])
      })
      yield* registry.install([
        {
          name: 'cwd-observer',
          hooks: [
            {
              operation: 'generation',
              handlers: { beforeRequest: () => recordCwd.pipe(Effect.as(undefined)) },
            },
            {
              operation: 'tool',
              handlers: { beforeTool: () => recordCwd.pipe(Effect.as(undefined)) },
            },
          ],
        },
      ])
      const harness = yield* open(executor)
      const conversation = yield* harness.root
      yield* Conversation.configure(conversation, { cwd: '/conversation/workspace' })
      yield* Submission.wait(yield* Conversation.submit(conversation, 'hello'))
      assert.deepEqual(yield* Ref.get(observed), [
        '/conversation/workspace',
        '/conversation/workspace',
        '/conversation/workspace',
      ])
    }),
  )

  it.effect('fires afterResponse once per successful native model response', () =>
    Effect.gen(function* () {
      const responses = yield* Ref.make(0)
      const { executor, registry } = yield* Native.makeExecutor()
      yield* registry.install([
        {
          name: 'response-observer',
          hooks: [
            {
              operation: 'generation',
              handlers: { afterResponse: () => Ref.update(responses, (count) => count + 1) },
            },
          ],
        },
      ])
      const harness = yield* open(executor)
      const conversation = yield* harness.root
      yield* Submission.wait(yield* Conversation.submit(conversation, 'hello'))
      assert.strictEqual(yield* Ref.get(responses), 2)
    }),
  )

  it.effect('a recovered tool with a missing registration settles unavailable without replay', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const entered = yield* Deferred.make<void>()
      const calls = yield* Ref.make(0)
      const firstExecutor = yield* Native.makeExecutor({
        handle: () =>
          Ref.update(calls, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(entered, undefined)),
            Effect.andThen(Effect.never),
          ),
      })
      const options: Harness.Options = {
        agent: { model: Native.ref },
        settings: { retry: { enabled: false }, compaction: { enabled: false } },
      }
      const first = yield* Harness.make(options).pipe(
        Effect.provideService(Persistence, store),
        Effect.provideService(Executor.Executor, firstExecutor.executor),
      )
      const conversation = yield* first.root
      const submission = yield* Conversation.submit(conversation, 'hello')
      yield* Deferred.await(entered)
      yield* first.close
      const secondExecutor = yield* Native.makeExecutor()
      yield* secondExecutor.registry.uninstall('test')
      const second = yield* Harness.make(options).pipe(
        Effect.provideService(Persistence, store),
        Effect.provideService(Executor.Executor, secondExecutor.executor),
      )
      yield* second.resume
      assert.strictEqual((yield* second.awaitSubmission(submission.id)).status, 'done')
      assert.strictEqual(yield* Ref.get(calls), 1)
      const snapshot = yield* second.snapshot(conversation.id)
      const entry = snapshot.entries.find((candidate) => candidate.kind === 'harness.tool')
      assert.isDefined(entry)
      const execution = yield* Schema.decodeUnknownEffect(
        Schema.toCodecJson(ToolRegistration.Execution),
      )(entry?.data)
      assert.strictEqual(execution.outcome, 'unavailable')
    }),
  )

  it.effect('places steering after tools and defers follow-up until the current answer', () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const prompts = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([])
      const { executor } = yield* Native.makeExecutor({
        provider: {
          ...Native.provider,
          streamText: ({ prompt }) =>
            Stream.unwrap(
              Ref.update(prompts, (values) => [
                ...values,
                prompt.content.flatMap((message) =>
                  message.role === 'user'
                    ? message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
                    : [],
                ),
              ]).pipe(
                Effect.as(
                  Stream.fromIterable(
                    Native.hasToolResult(prompt) ? Native.answer('HELLO') : Native.toolCall(),
                  ),
                ),
              ),
            ),
        },
        handle: Effect.fn('test.boundaries.tool')(function* ({ text }) {
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return text.toUpperCase()
        }),
      })
      const harness = yield* open(executor)
      const conversation = yield* harness.root
      const first = yield* Conversation.submit(conversation, 'first')
      yield* Deferred.await(entered)
      const steering = yield* Conversation.submit(conversation, 'steer', { mode: 'steering' })
      const followUp = yield* Conversation.submit(conversation, 'follow up', { mode: 'followUp' })
      yield* Deferred.succeed(release, undefined)
      const results = yield* Effect.all([
        Submission.wait(first),
        Submission.wait(steering),
        Submission.wait(followUp),
      ])
      assert.deepEqual(yield* Ref.get(prompts), [
        ['first'],
        ['first', 'steer'],
        ['first', 'steer', 'follow up'],
      ])
      assert.isTrue(results.every((result) => result.status === 'done'))
      if (
        results[0]?.status === 'done' &&
        results[1]?.status === 'done' &&
        results[2]?.status === 'done'
      ) {
        assert.strictEqual(results[0].answer, results[1].answer)
        assert.notStrictEqual(results[0].answer, results[2].answer)
      }
    }),
  )

  for (const testCase of [
    { name: 'all offered tools', tools: undefined, expected: undefined },
    { name: 'an exclusion selection', tools: { remove: ['bonus'] }, expected: { remove: [] } },
    { name: 'an explicit selection', tools: ['uppercase'], expected: ['uppercase', 'bonus'] },
  ] satisfies ReadonlyArray<{
    name: string
    tools: Agent.ToolSelection | undefined
    expected: Agent.ToolSelection | undefined
  }>) {
    it.effect(`tool addTools preserves ${testCase.name}`, () =>
      Effect.gen(function* () {
        const { executor, registry } = yield* Native.makeExecutor()
        yield* registry.install([
          {
            name: 'tool-control',
            hooks: [
              {
                operation: 'tool',
                handlers: {
                  afterTool: (_input, result) =>
                    Effect.succeed({ ...result, control: { addTools: ['bonus'] } }),
                },
              },
            ],
          },
        ])
        const harness = yield* open(
          executor,
          testCase.tools === undefined ? {} : { tools: testCase.tools },
        )
        const conversation = yield* harness.root
        yield* Submission.wait(yield* Conversation.submit(conversation, 'hello'))
        const state = yield* Conversation.document(conversation, Conversation.AgentDoc)
        assert.isTrue(Option.isSome(state))
        if (Option.isSome(state)) assert.deepEqual(state.value.value.tools, testCase.expected)
      }),
    )
  }
})
