/** Runner-owned storage fixtures retain a separately closeable backend scope. */
import * as Effect from 'effect/Effect'
import * as Scope from 'effect/Scope'
import * as Layer from 'effect/Layer'
import { ResourceScope, sessionLayer } from 'effect-harness/durable/testing/Storage'
import type * as Store from 'effect-harness/durable/Store'
import type * as Session from 'effect-harness/durable/Session'

export const withLayer = <A, E, R, O, E2, R2>(
  effect: Effect.Effect<A, E, R>,
  layer: Layer.Layer<O, E2, R2>,
): Effect.Effect<A, E | E2, R2 | Exclude<Exclude<R, O>, ResourceScope> | Scope.Scope> =>
  Effect.gen(function* () {
    const owned = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
      Scope.close(scope, exit),
    )
    const context = yield* Layer.buildWithScope(layer, owned)
    return yield* effect.pipe(
      Effect.provideContext(context),
      Effect.provideService(ResourceScope, owned),
    )
  })
export const withStorage = <A, E, R, E2, R2>(
  effect: Effect.Effect<A, E, R | Store.Store | Session.Session | ResourceScope>,
  backend: Layer.Layer<Store.Store, E2, R2>,
) => withLayer(effect, sessionLayer(backend))
