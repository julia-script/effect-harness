/** Private, per-owner authorization-attempt table construction. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as HashMap from 'effect/HashMap'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'

export interface Table<K, V> {
  readonly read: Effect.Effect<HashMap.HashMap<K, V>>
  readonly set: (value: HashMap.HashMap<K, V>) => Effect.Effect<void>
  readonly update: (
    f: (value: HashMap.HashMap<K, V>) => HashMap.HashMap<K, V>,
  ) => Effect.Effect<void>
  readonly modify: <A>(
    f: (value: HashMap.HashMap<K, V>) => readonly [A, HashMap.HashMap<K, V>],
  ) => Effect.Effect<A>
}
export class PendingAuthorization extends Context.Service<
  PendingAuthorization,
  {
    readonly make: <K, V>() => Effect.Effect<Table<K, V>>
  }
>()('effect-harness/internal/PendingAuthorization') {}

export const native = PendingAuthorization.of({
  make: <K, V>() =>
    Effect.gen(function* () {
      const ref = yield* Ref.make(HashMap.empty<K, V>())
      return {
        read: Ref.get(ref),
        set: (value: HashMap.HashMap<K, V>) => Ref.set(ref, value),
        update: (f: (value: HashMap.HashMap<K, V>) => HashMap.HashMap<K, V>) => Ref.update(ref, f),
        modify: <A>(f: (value: HashMap.HashMap<K, V>) => readonly [A, HashMap.HashMap<K, V>]) =>
          Ref.modify(ref, f),
      }
    }),
})
export const make = <K, V>(): Effect.Effect<Table<K, V>> =>
  Effect.flatMap(Effect.serviceOption(PendingAuthorization), (service) =>
    Option.getOrElse(service, () => native).make<K, V>(),
  )
