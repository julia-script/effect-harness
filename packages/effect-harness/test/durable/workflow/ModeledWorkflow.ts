import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import type * as Scope from 'effect/Scope'
import * as TestClock from 'effect/testing/TestClock'

/** Drives modeled native polling only after the operation's fiber is admitted. */
export const awaitTransition = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.gen(function* () {
    const fiber = yield* effect.pipe(Effect.forkScoped)
    for (let attempt = 0; attempt < 1000; attempt++) {
      if (fiber.pollUnsafe() !== undefined) return yield* Fiber.join(fiber)
      yield* TestClock.adjust('5 millis')
    }
    return yield* Effect.die('Admitted native workflow transition did not settle')
  })
