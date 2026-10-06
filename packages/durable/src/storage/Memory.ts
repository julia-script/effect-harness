import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Record from '../Record.ts'
import { Store } from '../Store.ts'
import * as Backend from './Backend.ts'
import { detached } from './State.ts'

export const make = Effect.fnUntraced(function* () {
  let snapshot: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
  return yield* Effect.acquireRelease(
    Backend.make({
      load: Effect.sync(() => detached(snapshot)),
      committed: Effect.sync(() => detached(snapshot)),
      save: (next) =>
        Effect.sync(() => {
          snapshot = detached(next)
        }),
      atomic: (effect) => effect,
      close: Effect.void,
    }),
    (store) => store.close.pipe(Effect.orDie),
  )
})
export const layer = Layer.effect(Store, make())
