/**
 * Native durable retry deadlines, schedules and receipt decisions.
 */
import * as Schema from 'effect/Schema'
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
export class ModelRetryError extends Schema.TaggedError<ModelRetryError>(
  '@effect-harness/durable/workflow/ModelRetry/ModelRetryError',
)('ModelRetryError', {
  name: Schema.String,
  at: Schema.DateTimeUtc,
}) {}

/**
 * Returns the nonnegative duration remaining before the retry deadline.
 *
 * **Details**
 *
 * Samples the wall clock once and retains fractional epoch precision.
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
  Schedule.while(({ input }) => Effect.succeed(input instanceof ModelRetryError)),
  Schedule.modifyDelay(
    Effect.fnUntraced(function* ({ input }) {
      if (!(input instanceof ModelRetryError)) return Duration.zero
      yield* DurableClock.sleep({
        name: input.name,
        duration: yield* remaining(input.at),
        inMemoryThreshold: 0,
      })
      return Duration.zero
    }),
  ),
)
