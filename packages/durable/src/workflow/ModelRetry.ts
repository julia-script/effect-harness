/**
 * Native durable retry deadlines, schedules and receipt decisions.
 */
import * as Data from 'effect/Data'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Schedule from 'effect/Schedule'
import * as DurableClock from 'effect/workflow/DurableClock'

/**
 * Internal retry metadata is emitted only after a native Activity journals its decision.
 *
 * @category errors
 */
export class ModelRetry extends Data.TaggedError('ModelRetry')<{
  readonly name: string
  readonly at: DateTime.Utc
}> {}

/**
 * Sample the wall clock once; fractional epoch values remain exact.
 *
 * @category combinators
 */
export const remaining = Effect.fnUntraced(function* (
  at: DateTime.Utc,
): Effect.fn.Return<Duration.Duration> {
  const now = yield* DateTime.now
  return DateTime.isLessThanOrEqualTo(at, now) ? Duration.zero : DateTime.distance(now, at)
})

/**
 * Waits at retry time against the original cached deadline, never current policy settings.
 *
 * @category combinators
 */
export const policy = Schedule.forever.pipe(
  Schedule.while(({ input }) => Effect.succeed(input instanceof ModelRetry)),
  Schedule.modifyDelay(
    Effect.fnUntraced(function* ({ input }) {
      if (!(input instanceof ModelRetry)) return Duration.zero
      yield* DurableClock.sleep({
        name: input.name,
        duration: yield* remaining(input.at),
        inMemoryThreshold: 0,
      })
      return Duration.zero
    }),
  ),
)
