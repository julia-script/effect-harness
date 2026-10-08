/** Private serialized ownership of a View's authoritative refresh state. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as SynchronizedRef from 'effect/SynchronizedRef'

export interface Transition<A> {
  readonly get: Effect.Effect<A>
  readonly modify: <B, E, R>(
    f: (state: A) => Effect.Effect<readonly [B, A], E, R>,
  ) => Effect.Effect<B, E, R>
}
export class ViewTransition extends Context.Service<
  ViewTransition,
  {
    readonly make: <A>(initial: A) => Effect.Effect<Transition<A>>
  }
>()('effect-harness/durable/internal/ViewTransition') {}
export const native = ViewTransition.of({
  make: <A>(initial: A) =>
    Effect.map(SynchronizedRef.make(initial), (state) => ({
      get: SynchronizedRef.get(state),
      modify: <B, E, R>(f: (state: A) => Effect.Effect<readonly [B, A], E, R>) =>
        SynchronizedRef.modifyEffect(state, f),
    })),
})
export const make = <A>(initial: A): Effect.Effect<Transition<A>> =>
  Effect.flatMap(Effect.serviceOption(ViewTransition), (factory) =>
    Option.getOrElse(factory, () => native).make(initial),
  )
