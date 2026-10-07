import { identity } from 'effect/Function'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import type * as Scope from 'effect/Scope'
import * as Record from '../Record.ts'
import type { Store } from '../Store.ts'
import * as backend from '../storage/internal/backend.ts'
import { detachedEffect } from '../storage/internal/state.ts'

export const make: Effect.Effect<Store['Service'], never, Scope.Scope> = Effect.gen(function* () {
  const snapshot = yield* Ref.make<backend.Snapshot>({ state: Record.emptyState(), frames: [] })
  return yield* backend.make({
    load: Ref.get(snapshot).pipe(Effect.flatMap(detachedEffect)),
    committed: Ref.get(snapshot).pipe(Effect.flatMap(detachedEffect)),
    save: (next) => detachedEffect(next).pipe(Effect.flatMap((value) => Ref.set(snapshot, value))),
    atomic: identity,
  })
})
