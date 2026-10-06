import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Session from '../Session.ts'
import { Store } from '../Store.ts'
import type { StorageError } from '../StorageError.ts'

/** Composes a storage backend with the ordinary domain Session service. */
export const sessionLayer = <E, R>(
  backend: Layer.Layer<Store, E, R>,
): Layer.Layer<Store | Session.Session, E, R> => Session.layer.pipe(Layer.provideMerge(backend))

/** Runner independent conformance cases keep the native Effect environment visible. */
export interface Case<R = Store | Session.Session> {
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
  effect: Effect.Effect<A, E, R | Store | Session.Session>,
  backend: Layer.Layer<Store, E2, R2>,
): Effect.Effect<A, E | E2, R | R2> =>
  Effect.scoped(effect.pipe(Effect.provide(sessionLayer(backend))))
