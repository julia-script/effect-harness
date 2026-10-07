/**
 * Runner adapters that register portable environment conformance cases.
 *
 * @since 0.0.0
 */
import type * as Duration from 'effect/Duration'
import type * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type { Env, ExecutionError, FileError } from '../Env.ts'
import { makeEnvConformance, withEnv, type Options } from './EnvConformance.ts'

/**
 * Runner runner contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Runner<in E, in R> {
  readonly describe: (name: string, suite: () => void) => unknown
  readonly test: (
    name: string,
    run: () => Effect.Effect<void, E | FileError | ExecutionError, R>,
    timeoutMs?: Duration.Input,
  ) => unknown
}
/**
 * Each test builds an independent scoped adapter Layer; runner timing metadata is preserved.
 *
 * @category combinators
 * @since 0.0.0
 */
export const registerEnvConformance = <E, R>(
  runner: Runner<E, R>,
  name: string,
  adapter: Layer.Layer<Env, E, R>,
  options: Options,
): void => {
  runner.describe(name, () => {
    for (const test of makeEnvConformance(options))
      runner.test(test.name, () => withEnv(test.run, adapter), test.timeoutMs)
  })
}
