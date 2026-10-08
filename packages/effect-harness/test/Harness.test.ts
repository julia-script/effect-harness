import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Conversation from '../src/Conversation.ts'
import * as Executor from '../src/Executor.ts'
import * as Harness from '../src/Harness.ts'
import * as Identity from '../src/Identity.ts'
import * as Invocation from '../src/Invocation.ts'
import * as Observation from '../src/Observation.ts'
import { Persistence } from '../src/Persistence.ts'
import * as ToolRegistration from '../src/ToolRegistration.ts'
import * as Submission from '../src/Submission.ts'
import * as Memory from '../src/storage/Memory.ts'
import * as Native from './embedded/NativeFixture.ts'

const open = Effect.fn('test.open')(function* (
  store: Persistence['Service'],
  options: Native.Options = {},
) {
  const { executor } = yield* Native.makeExecutor(options)
  return yield* Harness.make({
    agent: { model: Native.ref },
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
describe('embedded Harness', () => {
  it.effect('runs a native model/tool conversation and saves exactly one final answer', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const calls = yield* Ref.make(0)
      const harness = yield* open(store, {
        handle: ({ text }) => Ref.update(calls, (n) => n + 1).pipe(Effect.as(text.toUpperCase())),
      })
      const conversation = yield* harness.root
      const submission = yield* Conversation.submit(conversation, 'hello')
      const settled = yield* Submission.wait(submission)
      assert.strictEqual(settled.status, 'done')
      assert.strictEqual(yield* Ref.get(calls), 1)
      const snapshot = yield* Conversation.snapshot(conversation)
      assert.strictEqual(
        snapshot.entries.filter((entry) => entry.kind === 'harness.tool').length,
        1,
      )
      assert.strictEqual(
        snapshot.entries.filter((entry) => entry.kind === 'harness.assistant').length,
        2,
      )
      assert.isTrue(snapshot.tasks.every((task) => task.state.status === 'terminal'))
    }),
  )

  it.effect('admits a request ID exactly once, even after settlement', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const harness = yield* open(store)
      const conversation = yield* harness.root
      const options = { requestId: yield* Schema.decodeEffect(Identity.RequestId)('test/request') }
      const first = yield* Conversation.submit(conversation, 'hello', options)
      const second = yield* Conversation.submit(conversation, 'hello', options)
      assert.strictEqual(second.id, first.id)
      yield* Submission.wait(first)
      const third = yield* Conversation.submit(conversation, 'hello', options)
      assert.strictEqual(third.id, first.id)
      assert.strictEqual((yield* Conversation.snapshot(conversation)).submissions.length, 1)
    }),
  )

  it.effect('cancelling a submission waiter does not cancel admitted work', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const harness = yield* open(store, {
        handle: Effect.fn('test.waiter.tool')(function* ({ text }) {
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return text.toUpperCase()
        }),
      })
      const conversation = yield* harness.root
      const submission = yield* Conversation.submit(conversation, 'hello')
      const waiter = yield* Submission.wait(submission).pipe(Effect.forkChild)
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(waiter)
      assert.strictEqual(
        (yield* Conversation.snapshot(conversation)).tasks.some((task) => task.abortRequested),
        false,
      )
      yield* Deferred.succeed(release, undefined)
      assert.strictEqual((yield* Submission.wait(submission)).status, 'done')
    }),
  )

  it.effect('queues steering and follow-up messages while external work runs', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const harness = yield* open(store, {
        handle: Effect.fn('test.queue.tool')(function* ({ text }) {
          yield* Deferred.succeed(entered, undefined)
          yield* Deferred.await(release)
          return text.toUpperCase()
        }),
      })
      const conversation = yield* harness.root
      const first = yield* Conversation.submit(conversation, 'first')
      yield* Deferred.await(entered)
      const steering = yield* Conversation.submit(conversation, 'steer', { mode: 'steering' })
      const followUp = yield* Conversation.submit(conversation, 'follow up', { mode: 'followUp' })
      const snapshot = yield* Conversation.snapshot(conversation)
      assert.strictEqual(
        snapshot.submissions.filter((submission) => submission.status === 'queued').length,
        2,
      )
      yield* Deferred.succeed(release, undefined)
      const results = yield* Effect.all([
        Submission.wait(first),
        Submission.wait(steering),
        Submission.wait(followUp),
      ])
      assert.isTrue(results.every((result) => result.status === 'done'))
      assert.strictEqual((yield* Conversation.snapshot(conversation)).submissions.length, 3)
    }),
  )

  it.effect('watches snapshots and committed changes through round-trippable wire Schemas', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const harness = yield* open(store)
      const conversation = yield* harness.root
      const first = yield* Deferred.make<Observation.Change>()
      const changes = yield* Ref.make<ReadonlyArray<Observation.Change>>([])
      const watcher = yield* Conversation.watch(conversation).pipe(
        Stream.runForEach((change) =>
          Ref.update(changes, (values) => [...values, change]).pipe(
            Effect.andThen(Deferred.succeed(first, change)),
          ),
        ),
        Effect.forkChild,
      )
      const initial = yield* Deferred.await(first)
      assert.strictEqual(initial._tag, 'snapshot')
      const submission = yield* Conversation.submit(conversation, 'hello')
      yield* Submission.wait(submission)
      yield* Effect.yieldNow
      const collected = yield* Ref.get(changes)
      assert.isTrue(collected.some((change) => change._tag === 'commit'))
      for (const change of collected) {
        const encoded = yield* Schema.encodeEffect(Observation.Change)(change)
        assert.deepEqual(yield* Schema.decodeEffect(Observation.Change)(encoded), change)
      }
      yield* Fiber.interrupt(watcher)
    }),
  )
  for (const replay of ['safe', 'unsafe'] as const) {
    it.effect(
      `reopening ${replay} tool execution respects saved replay policy and committed partial output`,
      () =>
        Effect.gen(function* () {
          const store = yield* Memory.make
          const entered = yield* Deferred.make<void>()
          const calls = yield* Ref.make(0)
          const options: Native.Options = {
            replay,
            handle: Effect.fn('test.recovery.tool')(function* ({ text }) {
              const call = yield* Invocation.ToolCall
              const count = yield* Ref.updateAndGet(calls, (value) => value + 1)
              if (count === 1) {
                yield* call.output('partial output')
                yield* call.details({ committed: true })
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never
              }
              return text.toUpperCase()
            }),
          }
          const first = yield* open(store, options)
          const conversation = yield* first.root
          const submission = yield* Conversation.submit(conversation, 'hello')
          yield* Deferred.await(entered)
          yield* first.close
          const second = yield* open(store, options)
          yield* second.resume
          const settled = yield* second.awaitSubmission(submission.id)
          assert.strictEqual(settled.status, 'done')
          assert.strictEqual(yield* Ref.get(calls), replay === 'safe' ? 2 : 1)
          const snapshot = yield* second.snapshot(conversation.id)
          const tools = snapshot.entries.filter((entry) => entry.kind === 'harness.tool')
          assert.strictEqual(tools.length, 1)
          const data = yield* Schema.decodeUnknownEffect(
            Schema.toCodecJson(ToolRegistration.Execution),
          )(tools[0]?.data)
          assert.strictEqual(data.outcome, replay === 'safe' ? 'completed' : 'interrupted')
          if (replay === 'unsafe')
            assert.include(
              yield* Schema.encodeEffect(
                Schema.fromJsonString(Schema.toCodecJson(ToolRegistration.Execution)),
              )(data),
              'partial output',
            )
        }),
    )
  }

  it.effect('abort settles the submission and closes live observation and submission waiters', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const entered = yield* Deferred.make<void>()
      const harness = yield* open(store, {
        handle: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      })
      const conversation = yield* harness.root
      const submission = yield* Conversation.submit(conversation, 'hello')
      yield* Deferred.await(entered)
      yield* Conversation.abort(conversation)
      assert.strictEqual((yield* Submission.wait(submission)).status, 'unanswered')
      assert.isTrue(
        (yield* Conversation.snapshot(conversation)).tasks.every(
          (task) => task.state.status === 'terminal',
        ),
      )
    }),
  )

  it.effect(
    'an unexpected task fault settles admitted submissions instead of leaving a waiter hanging',
    () =>
      Effect.gen(function* () {
        const store = yield* Memory.make
        const model = {
          ...Native.provider,
          streamText: () => Stream.die('unexpected provider defect'),
        }
        const harness = yield* open(store, { provider: model })
        const conversation = yield* harness.root
        const submission = yield* Conversation.submit(conversation, 'hello')
        const settled = yield* Submission.wait(submission)
        assert.strictEqual(settled.status, 'unanswered')
        assert.isTrue(
          (yield* Conversation.snapshot(conversation)).tasks.every(
            (task) => task.state.status === 'terminal',
          ),
        )
      }),
  )

  it.effect(
    'a slow watcher receives a coherent reset snapshot after its bounded buffer overflows',
    () =>
      Effect.gen(function* () {
        const store = yield* Memory.make
        const harness = yield* open(store)
        const conversation = yield* harness.root
        const first = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const reset = yield* Deferred.make<Observation.Snapshot>()
        const watcher = yield* Conversation.watch(conversation).pipe(
          Stream.runForEach(
            Effect.fn('test.slowWatch')(function* (change) {
              if (change._tag === 'snapshot') {
                yield* Deferred.succeed(first, undefined)
                yield* Deferred.await(release)
              }
              if (change._tag === 'reset') yield* Deferred.succeed(reset, change.value)
            }),
          ),
          Effect.forkChild,
        )
        yield* Deferred.await(first)
        for (let index = 0; index < 140; index++)
          yield* Conversation.append(conversation, { kind: 'test/passive', data: index })
        yield* Deferred.succeed(release, undefined)
        const observed = yield* Deferred.await(reset)
        assert.strictEqual(observed.entries.length, 140)
        assert.deepEqual(
          yield* Schema.decodeEffect(Observation.Snapshot)(
            yield* Schema.encodeEffect(Observation.Snapshot)(observed),
          ),
          observed,
        )
        yield* Fiber.interrupt(watcher)
      }),
  )
})
