import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import type * as Scope from 'effect/Scope'
import * as Layer from 'effect/Layer'
import * as Record from '../Record.ts'
import { Store } from '../Store.ts'
import * as Backend from './internal/backend.ts'
import { detachedEffect } from './internal/state.ts'

export const make: Effect.Effect<Store['Service'], never, Scope.Scope> = Effect.gen(function* () {
  const snapshot = yield* Ref.make<Backend.Snapshot>({ state: Record.emptyState(), frames: [] })
  return yield* Backend.make({
    load: Ref.get(snapshot).pipe(Effect.flatMap(detachedEffect)),
    committed: Ref.get(snapshot).pipe(Effect.flatMap(detachedEffect)),
    save: (next) => detachedEffect(next).pipe(Effect.flatMap((value) => Ref.set(snapshot, value))),
    atomic: (effect) => effect,
  })
})
export const layer: Layer.Layer<Store> = Layer.effect(Store, make)
