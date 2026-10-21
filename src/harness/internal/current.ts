/**
 * The `Harness` service key, and the Harness a task runs under: `make` provides it around its
 * Scheduler, whose invocations inherit it, and the tool task provides it to each handler.
 */
import { Context, Deferred, Effect } from 'effect'
import { cast } from 'effect/Function'
import type { Service } from '../Harness.ts'

export class Harness extends Context.Service<Harness, Service>()(
  'effect-harness/harness/Harness',
) {}

/**
 * The Harness its Scheduler's invocations run under. The Scheduler starts, and may recover tasks,
 * before the Harness exists, so they wait for it.
 */
export const Opening: Context.Reference<Deferred.Deferred<Service> | undefined> = Context.Reference(
  'effect-harness/harness/Harness/Opening',
  {
    defaultValue: (): Deferred.Deferred<Service> | undefined => undefined,
  },
)

/**
 * `effect` with the Harness it runs under. Outside one it runs as is, and reading the Harness
 * there fails as a missing service.
 */
export const provide = Effect.fnUntraced(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E, Exclude<R, Harness>> {
  const opening = yield* Opening
  if (opening === undefined) return yield* cast<typeof effect, Effect.Effect<A, E>>(effect)
  return yield* Effect.provideService(effect, Harness, yield* Deferred.await(opening))
})
