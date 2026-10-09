/** Normalizes backend failures while preserving interruption and the stable client error. */
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import { HarnessError } from '../HarnessError.js'

const failure = (operation: string, cause: Cause.Cause<unknown>) =>
  new HarnessError({ reason: 'failed', operation, message: Cause.pretty(cause), cause })
export const protect = <A, E, R>(
  operation: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, HarnessError, R> =>
  effect.pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause)) return Effect.failCause(cause as Cause.Cause<HarnessError>)
      const error = Cause.findErrorOption(cause)
      if (Option.isSome(error) && Schema.is(HarnessError)(error.value))
        return Effect.fail(error.value)
      return Effect.fail(failure(operation, cause))
    }),
  )
export const absent = (operation: string, message: string) =>
  new HarnessError({ reason: 'notFound', operation, message })
