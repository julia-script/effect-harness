import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tracer from 'effect/Tracer'
import * as AiError from 'effect/ai/AiError'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'

import * as Task from 'effect-harness/Task'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const capture = () => {
  const spans: Tracer.Span[] = []
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options)
      spans.push(span)
      return span
    },
  })
  return { spans, tracer }
}
const named = (spans: ReadonlyArray<Tracer.Span>, name: string) => {
  const span = spans.find((item) => item.name === name)
  assert.isDefined(span, `Missing span ${name}`)
  return span!
}
const ended = (span: Tracer.Span) => {
  assert.strictEqual(span.status._tag, 'Ended', `${span.name} did not end`)
  if (span.status._tag !== 'Ended') throw new Error('Expected ended span')
  return span.status.exit
}
const parent = (span: Tracer.Span) => {
  assert.isTrue(Option.isSome(span.parent), `${span.name} has no parent`)
  const value = Option.getOrThrow(span.parent)
  if (value._tag !== 'Span') throw new Error('Expected local parent span')
  return value
}
const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 2, uncached: 2, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}

describe('durable tracing', () => {
  it.effect('commit spans retain the caller parent, write count, sequence and exact failure', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { spans, tracer } = capture()
        const session = yield* Session.make()
        const failure = { reason: 'callback rejected' }
        yield* Effect.gen(function* () {
          yield* Session.commit(session, Transaction.ensureRoot)
          const exit = yield* Session.commit(session, () => Effect.fail(failure)).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(exit))
          if (Exit.isFailure(exit))
            assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(exit.cause)), failure)
          yield* Session.commit(session, () => Effect.succeed('read only'))
        }).pipe(Effect.withSpan('caller'), Effect.withTracer(tracer))
        const commits = spans.filter((span) => span.name === 'Session.commit')
        assert.strictEqual(commits.length, 3)
        for (const commit of commits) assert.strictEqual(parent(commit), named(spans, 'caller'))
        assert.isAbove(Number(commits[0]!.attributes.get('commit.changes')), 0)
        assert.isDefined(commits[0]!.attributes.get('commit.sequence'))
        assert.isTrue(Exit.isSuccess(ended(commits[0]!)))
        const failed = ended(commits[1]!)
        assert.isTrue(Exit.isFailure(failed))
        if (Exit.isFailure(failed))
          assert.strictEqual(Option.getOrThrow(Cause.findErrorOption(failed.cause)), failure)
        assert.strictEqual(commits[2]!.attributes.get('commit.changes'), 0)
        assert.isFalse(commits[2]!.attributes.has('commit.sequence'))
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect('interrupted commits end their span and release the session line', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { spans, tracer } = capture()
        const session = yield* Session.make()
        const entered = yield* Deferred.make<void>()
        const fiber = yield* Session.commit(session, () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        ).pipe(Effect.withTracer(tracer), Effect.forkChild)
        yield* Deferred.await(entered)
        yield* Fiber.interrupt(fiber)
        const exit = ended(named(spans, 'Session.commit'))
        assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause))
        yield* Session.commit(session, Transaction.ensureRoot)
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  for (const enabled of [true, false])
    it.effect(`retry attempts and tool execution preserve behavior with tracing ${enabled}`, () => {
      const { spans, tracer } = capture()
      return Effect.scoped(
        Effect.gen(function* () {
          let calls = 0
          const transient = AiError.make({
            module: 'fixture',
            method: 'streamText',
            reason: new AiError.InternalProviderError({ description: 'retry me' }),
          })
          const response = Effect.fn('fixture.provider')(() =>
            Effect.sync(() => {
              calls++
              if (calls === 1) return Stream.fail(transient)
              if (calls === 2)
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: 'tool-call',
                    id: 'call-1',
                    name: 'echo',
                    params: { text: 'private argument' },
                  },
                  finish,
                ])
              return Stream.fromIterable<Response.StreamPartEncoded>([
                { type: 'text-start', id: 'answer' },
                { type: 'text-delta', id: 'answer', delta: 'private answer' },
                { type: 'text-end', id: 'answer' },
                finish,
              ])
            }),
          )
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('streaming required'),
            streamText: () => Stream.unwrap(response()),
          })
          const toolkit = Toolkit.make(
            Tool.make('echo', {
              parameters: Schema.Struct({ text: Schema.String }),
              success: Schema.String,
            }),
          )
          const settings = yield* Settings.make({
            retry: { baseDelayMs: 0, maxAgentDelayMs: 0, maxRetries: 1 },
          })
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideService(Settings.Settings, settings),
            Effect.provide(
              toolkit.toLayer({
                echo: Effect.fn('fixture.tool')(({ text }) => Effect.succeed(text)),
              }),
            ),
          )
          yield* runtime.backend.root
          const submission = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'private prompt' },
          })
          assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
          assert.strictEqual(calls, 3)
        }),
      ).pipe(
        Effect.provide(TemporaryStorage.layer),
        Effect.withSpan('runtime'),
        Effect.withTracer(tracer),
        Effect.withTracerEnabled(enabled),
        Effect.tap(() =>
          Effect.sync(() => {
            if (!enabled) {
              assert.deepStrictEqual(spans, [])
              return
            }
            const attempts = spans.filter((span) => span.name === 'Model.stream')
            assert.strictEqual(attempts.length, 3)
            assert.deepStrictEqual(
              attempts.map((span) => span.attributes.get('request.attempt')),
              [0, 1, 0],
            )
            assert.strictEqual(
              attempts[0]!.attributes.get('request.id'),
              attempts[1]!.attributes.get('request.id'),
            )
            assert.isTrue(Exit.isFailure(ended(attempts[0]!)))
            assert.isTrue(Exit.isSuccess(ended(attempts[1]!)))
            for (const attempt of attempts) assert.strictEqual(parent(attempt).name, 'RunLoop.run')
            const provider = spans.filter((span) => span.name === 'fixture.provider')
            assert.strictEqual(provider.length, 3)
            provider.forEach((span, index) => assert.strictEqual(parent(span), attempts[index]))
            const tool = named(spans, 'Tool.execute')
            assert.strictEqual(tool.attributes.get('tool.name'), 'echo')
            assert.strictEqual(tool.attributes.get('tool.call.id'), 'call-1')
            const inventory = named(spans, 'ToolInventory.invoke')
            assert.strictEqual(parent(named(spans, 'fixture.tool')), inventory)
            assert.strictEqual(parent(inventory), tool)
            assert.strictEqual(
              named(spans, 'Harness.admitSubmission').attributes.get('conversation.id'),
              root,
            )
            const runs = spans.filter((span) => span.name === 'RunLoop.run')
            assert.isAbove(runs.length, 0)
            for (const run of runs) assert.strictEqual(parent(run).name, 'TaskScheduler.invoke')
            for (const span of spans) ended(span)
            const attributes = JSON.stringify(spans.map((span) => [...span.attributes]))
            assert.notInclude(attributes, 'private prompt')
            assert.notInclude(attributes, 'private argument')
            assert.notInclude(attributes, 'private answer')
          }),
        ),
      )
    })

  it.effect(
    'custom phases use invocation tracing instead of their registration span and retain failures',
    () => {
      const { spans, tracer } = capture()
      return Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const registry = yield* Registry.make
          const definition = Task.make({
            kind: 'traced.task',
            version: 1,
            input: Schema.String,
            checkpoint: Schema.Struct({ phase: Schema.Literal('work') }),
            success: Schema.Void,
            failure: Schema.String,
            initial: () => ({ phase: 'work' as const }),
            phases: { work: () => Effect.fail('expected failure') },
          })
          yield* registry
            .install(
              Registry.withTask(
                Registry.toolkit({ id: 'tracing', version: 1 }, Toolkit.make()),
                definition,
              ),
            )
            .pipe(Effect.withSpan('registration'), Effect.withTracer(capture().tracer))
          const scheduler = yield* TaskScheduler.make(session).pipe(
            Effect.provideService(Registry.Registry, registry),
          )
          const task = yield* scheduler.start(definition, 'private input', { conversationId: root })
          const result = yield* scheduler.wait(task)
          assert.strictEqual(result.status, 'failed')
        }),
      ).pipe(
        Effect.provide(TemporaryStorage.layer),
        Effect.withTracer(tracer),
        Effect.tap(() =>
          Effect.sync(() => {
            const phase = named(spans, 'Task.runPhase')
            assert.strictEqual(parent(phase).name, 'TaskScheduler.invoke')
            const exit = ended(phase)
            assert.isTrue(Exit.isFailure(exit))
            if (Exit.isFailure(exit))
              assert.strictEqual(
                Option.getOrThrow(Cause.findErrorOption(exit.cause)),
                'expected failure',
              )
            assert.strictEqual(phase.attributes.get('task.phase'), 'work')
            assert.strictEqual(
              named(spans, 'TaskScheduler.invoke').attributes.get('task.result'),
              'failed',
            )
          }),
        ),
      )
    },
  )

  it.effect('cold reopen creates a new invocation trace correlated by the durable task ID', () =>
    Effect.gen(function* () {
      const firstTrace = capture()
      const secondTrace = capture()
      const entered = yield* Deferred.make<void>()
      const contract = {
        kind: 'traced.recovery',
        version: 1,
        input: Schema.Void,
        checkpoint: Schema.Struct({ phase: Schema.Literal('work') }),
        success: Schema.Void,
        failure: Schema.Never,
      }
      const definition = (complete: boolean) =>
        Task.make({
          ...contract,
          initial: () => ({ phase: 'work' as const }),
          phases: {
            work: () =>
              Effect.gen(function* () {
                if (!complete) {
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }
                const execution = yield* TaskExecution
                yield* execution.commit((tx) =>
                  execution.progress(tx, contract, {
                    status: 'completing',
                    outcome: { status: 'completed', result: undefined },
                  }),
                )
              }),
          },
        })
      const open = (task: ReturnType<typeof definition>) =>
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const registry = yield* Registry.make
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'recovery', version: 1 }, Toolkit.make()),
              task,
            ),
          )
          return yield* TaskScheduler.make(session).pipe(
            Effect.provideService(Registry.Registry, registry),
          )
        })
      const id = yield* Effect.scoped(
        Effect.gen(function* () {
          const task = definition(false)
          const scheduler = yield* open(task)
          const ref = yield* scheduler.start(task, undefined, { conversationId: root })
          yield* Deferred.await(entered)
          return ref.id
        }),
      ).pipe(Effect.withSpan('before-restart'), Effect.withTracer(firstTrace.tracer))
      yield* Effect.scoped(
        Effect.gen(function* () {
          const task = definition(true)
          const scheduler = yield* open(task)
          assert.strictEqual((yield* scheduler.wait({ id, definition: task })).status, 'completed')
        }),
      ).pipe(Effect.withSpan('after-restart'), Effect.withTracer(secondTrace.tracer))
      const before = named(firstTrace.spans, 'Task.runPhase')
      const after = named(secondTrace.spans, 'Task.runPhase')
      assert.strictEqual(before.attributes.get('task.id'), id)
      assert.strictEqual(after.attributes.get('task.id'), id)
      assert.notStrictEqual(before.traceId, after.traceId)
      assert.strictEqual(parent(after), named(secondTrace.spans, 'TaskScheduler.invoke'))
      assert.strictEqual(
        named(secondTrace.spans, 'TaskScheduler.resume').attributes.get('scheduler.recovery'),
        true,
      )
      const interrupted = ended(before)
      assert.isTrue(Exit.isFailure(interrupted) && Cause.hasInterruptsOnly(interrupted.cause))
      assert.isTrue(Exit.isSuccess(ended(after)))
    }).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect('Effect tracing can be disabled without changing commit behavior', () => {
    const { spans, tracer } = capture()
    return Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* Session.commit(session, Transaction.ensureRoot)
        assert.strictEqual(yield* Session.commit(session, () => Effect.succeed(42)), 42)
      }),
    ).pipe(
      Effect.provide(TemporaryStorage.layer),
      Effect.withTracer(tracer),
      Effect.withTracerEnabled(false),
      Effect.tap(() => Effect.sync(() => assert.deepStrictEqual(spans, []))),
    )
  })
})
