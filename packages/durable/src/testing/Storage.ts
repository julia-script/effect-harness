import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Scope from 'effect/Scope'
import * as Layer from 'effect/Layer'
import * as Session from '../Session.ts'
import { Store } from '../Store.ts'
import type { StorageError } from '../StorageError.ts'

/** Composes a storage backend with the ordinary domain Session service. */
export const sessionLayer = <E, R>(
  backend: Layer.Layer<Store, E, R>,
): Layer.Layer<Store | Session.Session, E, R> => Session.layer.pipe(Layer.provideMerge(backend))

/** Child resource Scope available to conformance cases for explicit early release. */
export class ResourceScope extends Context.Service<ResourceScope, Scope.Closeable>()(
  '@effect-harness/durable/testing/ResourceScope',
) {}

/** Builds resources in a child Scope while the test body retains its separate waiting Scope. */
export const withLayer = <A, E, R, O, E2, R2>(
  effect: Effect.Effect<A, E, R>,
  layer: Layer.Layer<O, E2, R2>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
        Scope.close(owned, exit),
      )
      const context = yield* Layer.buildWithScope(layer, scope)
      return yield* effect.pipe(
        Effect.provide(context),
        Effect.provideService(ResourceScope, scope),
      )
    }),
  )

/** Runner independent conformance cases keep the native Effect environment visible. */
export interface Case<R = Store | Session.Session | ResourceScope> {
  readonly name: string
  readonly run: Effect.Effect<void, StorageError, R>
}
export interface Assertions {
  readonly strictEqual: (actual: unknown, expected: unknown) => void
  readonly deepStrictEqual: (actual: unknown, expected: unknown) => void
  readonly notStrictEqual: (actual: unknown, expected: unknown) => void
  readonly throws: (evaluate: () => unknown, matcher?: RegExp) => void
  readonly ok: (condition: unknown, message?: string) => asserts condition
}

/** Every case gets its own resource scope and backend instance. */
export const withStorage = <A, E, R, E2, R2>(
  effect: Effect.Effect<A, E, R | Store | Session.Session | ResourceScope>,
  backend: Layer.Layer<Store, E2, R2>,
) => withLayer(effect, sessionLayer(backend))
