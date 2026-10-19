/**
 * Keep a Durable Object awake while its loop has work: Cloudflare evicts an object with no
 * incoming event, even in the middle of a model stream. While a task is live, a durable callback
 * fires on the object's alarm before each check, and the alarm is itself an event. When the object
 * was evicted anyway, the pending alarm wakes it, and building the loop resumes the tasks.
 */
import { makeCallback } from 'alchemy/Callback'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Duration, Effect, Layer, Schedule } from 'effect'
import { Harness } from '../../harness/Harness.ts'

/** The callback's name; jobs left under it by an earlier deployment must still find a handler. */
const callback = 'effect-harness/wake'

export const layer = (every: Duration.Input): Layer.Layer<never, never, Harness | RuntimeContext> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const harness = yield* Harness
      // The wake is the work: delivering it is an event, and an evicted object starts again.
      const wake = yield* makeCallback(callback, () => Effect.void)
      const after = Duration.times(Duration.fromInputUnsafe(every), 0.5)
      const check = harness.scheduler.inspect.pipe(
        Effect.map(({ tasks }) => tasks.some(({ state }) => state.kind !== 'blocked')),
        Effect.flatMap((busy) =>
          busy ? wake.schedule('heartbeat', { after, payload: null }) : Effect.void,
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning('effect-harness: keep-awake check failed', cause),
        ),
      )
      yield* Effect.forkScoped(Effect.repeat(check, Schedule.spaced(every)))
    }),
  )
