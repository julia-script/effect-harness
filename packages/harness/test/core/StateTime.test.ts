import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaIssue from 'effect/SchemaIssue'
import * as Agent from '../../src/Agent.ts'
import * as Hook from '../../src/Hook.ts'
import * as Model from '../../src/Model.ts'
import * as Output from '../../src/Output.ts'
import * as Progress from '../../src/Progress.ts'
import * as Time from '../../src/Time.ts'
import { Invocation } from '../../src/Invocation.ts'

class Example extends Context.Service<Example, string>()('StateTime/Example') {}
const recoverWithContext: Effect.Effect<number | undefined, never, Example | Invocation> =
  Hook.recover(Effect.map(Example, (value) => value.length))

const containsCause = (issue: SchemaIssue.Issue, cause: unknown): boolean => {
  if ('annotations' in issue && issue.annotations?.cause === cause) return true
  if ('issue' in issue && SchemaIssue.isIssue(issue.issue)) return containsCause(issue.issue, cause)
  if ('issues' in issue) return issue.issues.some((child) => containsCause(child, cause))
  return false
}

describe('state and time domain contracts', () => {
  it.effect('fractional epoch milliseconds roundtrip exactly and polling remains monotone', () =>
    Effect.gen(function* () {
      const instant = yield* Schema.decodeEffect(Time.EpochMillis)(1000.5)
      assert.strictEqual(DateTime.toEpochMillis(instant), 1000.5)
      assert.strictEqual(yield* Schema.encodeEffect(Time.EpochMillis)(instant), 1000.5)
      assert.strictEqual(DateTime.toEpochMillis(Model.pollAt(instant)), 6000.5)
      assert.strictEqual(
        DateTime.toEpochMillis(
          Model.pollAt(instant, Time.fromEpochMillis(1001.75), Duration.millis(-20)),
        ),
        1002.75,
      )
      const decision = yield* Schema.decodeEffect(Model.DeferredDecision)({
        handle: 'job',
        pollAfterMs: 0.25,
      })
      assert.strictEqual(Duration.toMillis(decision.pollAfterMs!), 0.25)
      assert.strictEqual(
        (yield* Schema.encodeEffect(Model.DeferredDecision)(decision)).pollAfterMs,
        0.25,
      )
    }),
  )
  it.effect(
    'duration options accept native input without losing bigint nanos and reject malformed markers in the typed channel',
    () =>
      Effect.gen(function* () {
        assert.strictEqual(Duration.toMillis(yield* Time.duration('1.5 seconds')), 1500)
        const nanos = 9007199254740993n
        assert.strictEqual(
          Option.getOrThrow(Duration.toNanos(yield* Time.duration(Duration.nanos(nanos)))),
          nanos,
        )
        for (const input of [
          NaN,
          Infinity,
          'nonsense',
          { '~effect/Duration': '~effect/Duration' },
        ]) {
          const exit = yield* Effect.exit(Time.duration(input))
          assert.strictEqual(Exit.isFailure(exit), true)
          if (Exit.isFailure(exit))
            assert.strictEqual(
              Exit.findErrorOption(exit).pipe(
                Option.map((error) => error instanceof Schema.SchemaError),
                Option.getOrElse(() => false),
              ),
              true,
            )
        }
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
          retry: { baseDelayMs: '3 seconds', maxAgentDelayMs: '5 seconds' },
          progress: { partialIntervalMs: undefined, outputIntervalMs: '20 millis' },
        })
        assert.strictEqual(Duration.toMillis(settings.progress.partialIntervalMs), 100)
        assert.strictEqual(Duration.toMillis(Agent.retryDelay(settings.retry, 1)), 3000)
        assert.strictEqual(Duration.toMillis(Agent.retryDelay(settings.retry, 2)), 5000)
        const encoded = yield* Schema.encodeEffect(Agent.Settings)(settings)
        assert.strictEqual(encoded.retry.baseDelayMs, 3000)
        assert.strictEqual(encoded.progress.outputIntervalMs, 20)
        for (const input of [-1, 0.25, Number.MAX_SAFE_INTEGER + 1])
          assert.strictEqual(
            (yield* Effect.flip(
              Agent.settings({ progress: { outputIntervalMs: input } }),
            )) instanceof Schema.SchemaError,
            true,
          )
        const timeout = yield* Schema.decodeEffect(Time.CommandTimeout)(0.125)
        assert.strictEqual(Duration.toMillis(timeout), 125)
        assert.strictEqual(yield* Schema.encodeEffect(Time.CommandTimeout)(timeout), 0.125)
        for (const input of [0, -1, Infinity, 2147483.648])
          assert.strictEqual(
            (yield* Effect.flip(Schema.decodeEffect(Time.CommandTimeout)(input))) instanceof
              Schema.SchemaError,
            true,
          )
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
      Effect.scoped(
        Effect.gen(function* () {
          const writer = yield* Progress.make(Effect.succeed(0), Duration.nanos(500n))
          yield* writer.markAndWait
          assert.deepStrictEqual(yield* writer.stop, [])
        }),
      ),
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
