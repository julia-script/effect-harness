import type * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type { Env, ExecutionError, FileError } from '../Env.ts'
import { createEnvConformance, withEnv, type Options } from './EnvConformance.ts'

export interface Runner<E, R> {
  readonly describe: (name: string, suite: () => void) => unknown
  readonly test: (
    name: string,
    run: () => Effect.Effect<void, E | FileError | ExecutionError, R>,
    timeoutMs?: number,
  ) => unknown
}
/** Each test builds an independent scoped adapter Layer; runner timing metadata is preserved. */
export const registerEnvConformance = <E, R>(
  runner: Runner<E, R>,
  name: string,
  adapter: Layer.Layer<Env, E, R>,
  options: Options,
): void => {
  runner.describe(name, () => {
    for (const test of createEnvConformance(options))
      runner.test(test.name, () => withEnv(test.run, adapter), test.timeoutMs)
  })
}
