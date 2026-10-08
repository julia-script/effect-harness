/**
 * Scoped storage conformance runner.
 */
import * as handle from '../internal/handle.ts'
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
import { identity } from 'effect/Function'
import type * as Types from 'effect/Types'
import type * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type { StorageError } from '../StorageError.ts'
import type { Store } from '../Store.ts'
import { makeStorageConformance } from './Conformance.ts'
import { withStorage, type Assertions } from './Storage.ts'

const RunnerProto = handle.prototype({
  id: '@effect-harness/durable/testing/Runner',
  fields: ['describe', 'test'],
})
const RunnerTypeId = '~@effect-harness/durable/testing/Runner'
/**
 * Test registration functions used to install conformance cases.
 *
 * @category models
 */
export interface Runner<in E, in R> extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [RunnerTypeId]: {
    readonly _E: Types.Contravariant<E>
    readonly _R: Types.Contravariant<R>
  }
  readonly describe: (name: string, suite: () => void) => unknown
  readonly test: (name: string, run: () => Effect.Effect<void, StorageError | E, R>) => unknown
}
/**
 * Registers conformance with Effect-native runners; every case owns a fresh scoped Layer.
 *
 * @category combinators
 */
export const registerStorageConformance = <E, R>(
  runner: Runner<E, R>,
  assertions: Assertions,
  name: string,
  backend: Layer.Layer<Store, E, R>,
): void => {
  runner.describe(name, () => {
    for (const test of makeStorageConformance(assertions))
      runner.test(test.name, () => withStorage<void, StorageError, never, E, R>(test.run, backend))
  })
}

/**
 * Creates a scoped storage conformance runner.
 *
 * @category constructors
 */
export const makeRunner = <E, R>(
  input: handle.Input<Runner<E, R>, typeof RunnerTypeId>,
): Runner<E, R> => {
  const runner = handle.make(RunnerProto, {
    ...input,
    [RunnerTypeId]: { _E: identity, _R: identity },
  })
  Object.defineProperty(runner, RunnerTypeId, { enumerable: false })
  return runner
}

/** Checks the runner identity at the safe contravariant error/environment target.
 * @category guards
 */
export const isRunner = (u: unknown): u is Runner<never, never> =>
  Predicate.hasProperty(u, RunnerTypeId)
