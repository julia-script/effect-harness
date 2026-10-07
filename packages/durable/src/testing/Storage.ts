/**
 * Scoped backend fixtures and conformance case contracts.
 */
import { dual } from 'effect/Function'
import { identity } from 'effect/Function'
import type * as Types from 'effect/Types'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Scope from 'effect/Scope'
import * as Layer from 'effect/Layer'
import * as Session from '../Session.ts'
import { Store } from '../Store.ts'
import type { StorageError } from '../StorageError.ts'

/**
 * Composes a storage backend with the ordinary domain Session service.
 *
 * @category combinators
 */
export const sessionLayer = <E, R>(
  backend: Layer.Layer<Store, E, R>,
): Layer.Layer<Store | Session.Session, E, R> => Session.layer.pipe(Layer.provideMerge(backend))

/**
 * Child resource Scope available to conformance cases for explicit early release.
 *
 * @category services
 */
export class ResourceScope extends Context.Service<ResourceScope, Scope.Closeable>()(
  '@effect-harness/durable/testing/ResourceScope',
) {}

/**
 * Builds resources in a child Scope while the test body retains its separate waiting Scope.
 *
 * @category combinators
 */
export const withLayer = <A, E, R, O, E2, R2>(
  effect: Effect.Effect<A, E, R>,
  layer: Layer.Layer<O, E2, R2>,
): Effect.Effect<A, E | E2, Exclude<R2 | Exclude<Exclude<R, O>, ResourceScope>, Scope.Scope>> =>
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
const CaseTypeId = '~@effect-harness/durable/testing/Storage/Case'
/**
 * Named Effect-based storage conformance case.
 *
 * @category models
 */
export interface Case<out R = Store | Session.Session | ResourceScope> {
  readonly [CaseTypeId]: { readonly _R: Types.Covariant<R> }
  readonly name: string
  readonly run: Effect.Effect<void, StorageError, R>
}
/**
 * Runner-independent assertions for storage conformance.
 *
 * @category models
 */
export interface Assertions {
  readonly strictEqual: (actual: unknown, expected: unknown) => void
  readonly deepStrictEqual: (actual: unknown, expected: unknown) => void
  readonly notStrictEqual: (actual: unknown, expected: unknown) => void
  readonly throws: (evaluate: () => unknown, matcher?: RegExp) => void
  readonly ok: (condition: unknown, message?: string) => asserts condition
}

/**
 * Every case gets its own resource scope and backend instance.
 *
 * @category combinators
 */
export const withStorage: {
  <E2, R2>(
    backend: Layer.Layer<Store, E2, R2>,
  ): <A, E, R>(
    self: Effect.Effect<A, E, R | Store | Session.Session | ResourceScope>,
  ) => Effect.Effect<
    A,
    E | E2,
    Exclude<R2 | Exclude<Exclude<R, Store | Session.Session>, ResourceScope>, Scope.Scope>
  >
  <A, E, R, E2, R2>(
    self: Effect.Effect<A, E, R | Store | Session.Session | ResourceScope>,
    backend: Layer.Layer<Store, E2, R2>,
  ): Effect.Effect<
    A,
    E | E2,
    Exclude<R2 | Exclude<Exclude<R, Store | Session.Session>, ResourceScope>, Scope.Scope>
  >
} = dual(
  2,
  <A, E, R, E2, R2>(
    self: Effect.Effect<A, E, R | Store | Session.Session | ResourceScope>,
    backend: Layer.Layer<Store, E2, R2>,
  ) => withLayer(self, sessionLayer(backend)),
)

/**
 * Creates a named storage conformance case.
 *
 * @category constructors
 */
export const makeCase = <R>(input: Omit<Case<R>, typeof CaseTypeId>): Case<R> => {
  const value: Case<R> = { ...input, [CaseTypeId]: { _R: identity } }
  Object.defineProperty(value, CaseTypeId, { enumerable: false })
  return value
}
