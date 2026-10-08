import { assert, describe, it } from '@effect/vitest'

import * as Context from 'effect/Context'

import * as DateTime from 'effect/DateTime'

import * as Duration from 'effect/Duration'

import * as Effect from 'effect/Effect'

import { assertSome, assertFailure } from '@effect/vitest/utils'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Result from 'effect/Result'

import * as SchemaIssue from 'effect/SchemaIssue'

import * as Agent from 'effect-harness/Agent'

import * as Hook from 'effect-harness/Hook'

import * as Model from 'effect-harness/Model'

import * as Output from 'effect-harness/Output'

import * as Progress from 'effect-harness/Progress'

import * as Time from 'effect-harness/Time'

import { Invocation } from 'effect-harness/Invocation'

describe('AgentTime', () => {
  class Example extends Context.Service<Example, string>()('StateTime/Example') {}
  const recoverWithContext = Hook.recover(Effect.map(Example, (value) => value.length))

  const containsCause = (issue: SchemaIssue.Issue, cause: unknown): boolean => {
    if ('annotations' in issue && issue.annotations?.cause === cause) return true
    if ('issue' in issue && SchemaIssue.isIssue(issue.issue))
      return containsCause(issue.issue, cause)
    if ('issues' in issue) return issue.issues.some((child) => containsCause(child, cause))
    return false
  }

  describe('state and time domain contracts', () => {
    it.effect('fractional epoch milliseconds roundtrip exactly and polling remains monotone', () =>
      Effect.gen(function* () {
        const instant = Time.fromEpochMillis(1000.5)
        const epoch = new TestSchema.Asserts(Time.DateTimeUtcFromEpochMillis)
        yield* epoch.decoding().succeedEffect(1000.5, instant)
        assert.strictEqual(DateTime.toEpochMillis(instant), 1000.5)
        yield* epoch.encoding().succeedEffect(instant, 1000.5)
        assert.strictEqual(DateTime.toEpochMillis(Model.pollAt(instant)), 6000.5)
        assert.strictEqual(
          DateTime.toEpochMillis(
            Model.pollAt(instant, Time.fromEpochMillis(1001.75), Duration.millis(-20)),
          ),
          1002.75,
        )
        const decision = { handle: 'job', pollAfterMs: Duration.millis(0.25) }
        const deferred = new TestSchema.Asserts(Model.DeferredDecision)
        yield* deferred.decoding().succeedEffect({ handle: 'job', pollAfterMs: 0.25 }, decision)
        assert.strictEqual(Duration.toMillis(decision.pollAfterMs), 0.25)
        yield* deferred.encoding().succeedEffect(decision, { handle: 'job', pollAfterMs: 0.25 })
      }),
    )
    it.effect(
      'duration options accept native input without losing bigint nanos and reject malformed markers in the typed channel',
      () =>
        Effect.gen(function* () {
          assert.strictEqual(Duration.toMillis(yield* Time.duration('1.5 seconds')), 1500)
          const nanos = 9007199254740993n
          assertSome(Duration.toNanos(yield* Time.duration(Duration.nanos(nanos))), nanos)
          const duration = new TestSchema.Asserts(Time.DurationFromUnknown)
          yield* duration.decoding().succeedEffect('1.5 seconds', Duration.millis(1500))
          yield* duration.decoding().succeedEffect(Duration.nanos(nanos), Duration.nanos(nanos))
          for (const [input, issue] of [
            [NaN, 'Expected a finite duration input'],
            [Infinity, 'Expected a finite duration input'],
            ['nonsense', 'Invalid duration input'],
            [{ '~effect/Duration': '~effect/Duration' }, 'Invalid duration input'],
          ] as const)
            yield* duration.decoding().failEffect(input, issue)
          // effect-nit-allow P8-testschema-asserts: the foreign getter cause identity belongs to runtime provenance; schema issue text alone cannot prove that identity.
          const cause = new Error('foreign duration getter failed')
          const forged = {
            '~effect/Duration': '~effect/Duration',
            get value(): never {
              throw cause
            },
          }
          const failure = yield* Effect.flip(Time.duration(forged))
          assert.strictEqual(containsCause(failure.issue, cause), true)
        }),
    )
    it.effect(
      'settings normalize inputs once, preserve numeric wire fields and retain bounds and undefined defaults',
      () =>
        Effect.gen(function* () {
          const settings = yield* Agent.settings({
            retry: { baseDelay: '3 seconds', maxAgentDelay: '5 seconds' },
            progress: { partialInterval: undefined, outputInterval: '20 millis' },
          })
          assert.strictEqual(Duration.toMillis(settings.progress.partialInterval), 100)
          assert.strictEqual(Duration.toMillis(Agent.retryDelay(settings.retry, 1)), 3000)
          assert.strictEqual(Duration.toMillis(Agent.retryDelay(settings.retry, 2)), 5000)
          const wire = {
            stream: {},
            retry: { enabled: true, maxRetries: 3, baseDelay: 3000, maxAgentDelay: 5000 },
            compaction: {
              enabled: true,
              reserveTokens: 16384,
              keepRecentTokens: 20000,
              backgroundTokens: 32768,
            },
            progress: { partialInterval: 100, outputInterval: 20 },
            toolExecution: 'parallel' as const,
            steeringMode: 'one-at-a-time' as const,
            followUpMode: 'one-at-a-time' as const,
          }
          const settingsCodec = new TestSchema.Asserts(Agent.Settings)
          yield* settingsCodec.encoding().succeedEffect(settings, wire)
          yield* settingsCodec.decoding().succeedEffect(wire, {
            ...wire,
            retry: {
              ...wire.retry,
              baseDelay: Duration.seconds(3),
              maxAgentDelay: Duration.seconds(5),
            },
            progress: {
              partialInterval: Duration.millis(100),
              outputInterval: Duration.millis(20),
            },
          })
          for (const input of [-1, 0.25, Number.MAX_SAFE_INTEGER + 1]) {
            const result = yield* Effect.result(
              Agent.settings({ progress: { outputInterval: input } }),
            )
            assertFailure(
              Result.mapError(result, (error) => error.message),
              'Expected nonnegative safe integer milliseconds\n  at ["progress"]["outputInterval"]',
            )
          }
          const command = new TestSchema.Asserts(Time.CommandTimeoutFromSeconds)
          const timeout = Duration.millis(125)
          yield* command.decoding().succeedEffect(0.125, timeout)
          assert.strictEqual(Duration.toMillis(timeout), 125)
          yield* command.encoding().succeedEffect(timeout, 0.125)
          for (const [input, issue] of [
            [0, 'Expected a value greater than 0'],
            [-1, 'Expected a value greater than 0'],
            [Infinity, 'Expected a finite number'],
            [2147483.648, 'Expected a value less than or equal to 2147483.647'],
          ] as const)
            yield* command.decoding().failEffect(input, issue)
        }),
    )
    it.effect(
      'Hook recovery preserves arbitrary success/context types and reports the exact foreign error',
      () =>
        Effect.gen(function* () {
          const cause = new Error('foreign failure')
          let reported: unknown
          const invocation = Invocation.of({
            cwd: '/',
            report: (error) =>
              Effect.sync(() => {
                reported = error
              }),
            progress: () => Effect.void,
          })
          assert.strictEqual(
            yield* recoverWithContext.pipe(
              Effect.provideService(Example, 'abc'),
              Effect.provideService(Invocation, invocation),
            ),
            3,
          )
          assert.strictEqual(
            yield* Hook.recover(Effect.fail(cause)).pipe(
              Effect.provideService(Invocation, invocation),
            ),
            undefined,
          )
          assert.strictEqual(reported, cause)
        }),
    )
    it.effect(
      'standalone progress pacing retains valid fractional native spans without applying Settings integer bounds',
      () =>
        Effect.gen(function* () {
          const writer = yield* Progress.make(Effect.succeed(0), {
            minInterval: Duration.nanos(500n),
          })
          yield* writer.markAndWait
          assert.deepStrictEqual(yield* writer.stop, [])
        }),
    )
    it.effect(
      'each lazy output window has isolated decoder/state and snapshots leave subsequent writes intact',
      () =>
        Effect.gen(function* () {
          const make = Output.makeWindow({ retain: 'tail', maxLines: 3, maxBytes: 100 })
          const first = yield* make
          const second = yield* make
          yield* first.push(Uint8Array.of(0xc3))
          yield* second.push('other')
          assert.strictEqual((yield* first.snapshot).text, '')
          yield* first.push(Uint8Array.of(0xa9))
          assert.strictEqual((yield* first.snapshot).text, 'é')
          assert.strictEqual((yield* second.snapshot).text, 'other')
          yield* first.reset
          yield* first.push('fresh')
          assert.strictEqual((yield* first.snapshot).text, 'fresh')
          assert.strictEqual((yield* second.snapshot).text, 'other')
        }),
    )
  })
})
