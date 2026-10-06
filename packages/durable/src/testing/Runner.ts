import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type { StorageError } from '../StorageError.ts'
import type { Store } from '../Store.ts'
import { createStorageConformance } from './Conformance.ts'
import { sessionLayer, type Assertions } from './Storage.ts'

export interface Runner<E, R> {
  readonly describe: (name: string, suite: () => void) => unknown
  readonly test: (name: string, run: () => Effect.Effect<void, StorageError | E, R>) => unknown
}
/** Registers conformance with Effect-native runners; every case owns a fresh scoped Layer. */
export const registerStorageConformance = <E, R>(
  runner: Runner<E, R>,
  assertions: Assertions,
  name: string,
  backend: Layer.Layer<Store, E, R>,
): void => {
  runner.describe(name, () => {
    for (const test of createStorageConformance(assertions))
      runner.test(test.name, () =>
        Effect.scoped(test.run.pipe(Effect.provide(sessionLayer(backend)))),
      )
  })
}
