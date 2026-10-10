import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import { ShellProcess } from './ShellProcess.js'
import { ToolExecution } from '../ToolExecution.js'
import { ToolSpill } from '../ToolSpill.js'
import * as Tool from '../Tool.js'
import * as Toolkit from '../Toolkit.js'
import * as Shell from '../internal/Shell.js'

export { Parameters, Outcome, FailureSchema, ShellError, failureOutput } from '../internal/Shell.js'
export type { Prepared, Options, Failure } from '../internal/Shell.js'

/** Script strings use the selected Bash program; arrays are literal direct argv. Unsafe replay by default. */
export const tool = Tool.makeResult('bash', {
  description:
    'Execute Bash script text or direct literal argv through the bound process backend. Returns retained output and full raw spill metadata on truncation.',
  parameters: Shell.Parameters,
  success: Shell.Outcome,
  failure: Shell.FailureSchema,
})
  .addDependency(ShellProcess)
  .addDependency(FileSystem.FileSystem)
  .addDependency(Path.Path)
  .addDependency(ToolSpill)
  .addDependency(ToolExecution)
export const toolkit = Toolkit.make(tool)
export const run = Effect.fn('Bash.run')(function* <E = never, R = never>(
  input: Shell.Parameters,
  options: Shell.Options<E, R> = {},
) {
  return yield* Shell.execute('bash', input, options)
})
/** Supply coherent bound native services and a longer-lived ToolSpill provider. */
export const layer = <E = never, R = never>(options: Shell.Options<E, R> = {}) => {
  const frozen = Shell.freeze(options)
  return toolkit.toLayer(
    Effect.gen(function* () {
      const local = yield* Shell.bindPreparation(frozen)
      return {
        bash: (input) =>
          run(input, local).pipe(
            Effect.mapError((error) =>
              error instanceof Shell.ShellError ? Shell.failureOutput(error) : error,
            ),
          ),
      } satisfies Toolkit.HandlersFrom<typeof toolkit.tools>
    }),
  )
}
