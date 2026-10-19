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
/** Tries pwsh, then powershell only on spawn NotFound; no ambient alternate shell. */
export const tool = Tool.makeResult('powershell', {
  description:
    'Execute PowerShell script text through the bound backend, or direct literal argv. Missing executables are explicit typed failures.',
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
export const run = Effect.fn('PowerShell.run')(function* <E = never, R = never>(
  input: Shell.Parameters,
  options: Shell.Options<E, R> = {},
) {
  return yield* Shell.execute('powershell', input, options)
})
export const layer = <E = never, R = never>(options: Shell.Options<E, R> = {}) => {
  const frozen = Shell.freeze(options)
  return toolkit.toLayer(
    Effect.gen(function* () {
      const local = yield* Shell.bindPreparation(frozen)
      return {
        powershell: (input) =>
          run(input, local).pipe(
            Effect.mapError((error) =>
              error instanceof Shell.ShellError ? Shell.failureOutput(error) : error,
            ),
          ),
      } satisfies Toolkit.HandlersFrom<typeof toolkit.tools>
    }),
  )
}
