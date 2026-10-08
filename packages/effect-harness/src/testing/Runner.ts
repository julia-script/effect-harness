/**
 * Runner adapters that register portable environment conformance cases.
 */
import type * as Duration from 'effect/Duration'
import type * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type { Env } from '../Env.ts'
import type { ExecutionError } from '../ExecutionError.ts'
import type { FileError } from '../FileError.ts'

import { makeEnvConformance, withEnv } from './EnvConformance.ts'

/**
 * Test registration functions used to install conformance cases.
 *
 * @category models
 */
export interface Runner<in E, in R> {
  readonly describe: (name: string, suite: () => void) => unknown
  readonly test: (
    name: string,
    run: () => Effect.Effect<void, E | FileError | ExecutionError, R>,
    timeout?: Duration.Input,
  ) => unknown
}
/**
 * Registers conformance cases with independent scoped adapter Layers.
 *
 * **Details**
 *
 * Preserves runner timing metadata for each test.
 *
 * @category combinators
 */
export const registerEnvConformance = <E, R>(
  self: Runner<E, R>,
  name: string,
  adapter: Layer.Layer<Env, E, R>,
  options: makeEnvConformance.Options,
): void => {
  self.describe(name, () => {
    for (const test of makeEnvConformance(options))
      self.test(test.name, () => withEnv(test.run, adapter), test.timeout)
  })
}
