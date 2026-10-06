import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Record from '../Record.ts'
import { Store } from '../Store.ts'
import * as Backend from './Backend.ts'
import { detachedEffect } from './State.ts'

export const make = Effect.fnUntraced(function* () {
  let snapshot: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
  return yield* Effect.acquireRelease(
    Backend.make({
      load: Effect.suspend(() => detachedEffect(snapshot)),
      committed: Effect.suspend(() => detachedEffect(snapshot)),
      save: (next) =>
        Effect.gen(function* () {
          snapshot = yield* detachedEffect(next)
        }),
      atomic: (effect) => effect,
      close: Effect.void,
    }),
    (store) => store.close.pipe(Effect.orDie),
  )
})
export const layer = Layer.effect(Store, make())
