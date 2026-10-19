import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Model from '../Model.js'
import { ModelError } from '../ModelError.js'

/** Evidence from this accepted restore operation only, never from an earlier callback. */
export class PreparationRestoreFailure extends Schema.TaggedError<PreparationRestoreFailure>()(
  'PreparationRestoreFailure',
  { error: ModelError },
) {}

export const restore = (codec: Model.Configuration, value: Schema.Json) =>
  Effect.suspend(() => codec.restore(value)).pipe(
    Effect.catchCause((cause): Effect.Effect<never, ModelError | PreparationRestoreFailure> => {
      const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
      if (reason !== undefined && Cause.isFailReason(reason) && Schema.is(ModelError)(reason.error))
        return Effect.fail(new PreparationRestoreFailure({ error: reason.error }))
      return Effect.failCause(cause)
    }),
  )
