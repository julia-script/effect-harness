import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Retention from 'effect-harness/OutputRetention'
import type * as Execution from 'effect-harness/ExecutionError'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Spill from 'effect-harness/ToolSpill'
import type * as Toolkit from 'effect-harness/Toolkit'
import * as Bash from 'effect-harness/coding/Bash'
import * as PowerShell from 'effect-harness/coding/PowerShell'
import * as ShellProcess from 'effect-harness/coding/ShellProcess'

class Credentials extends Context.Service<Credentials, string>()(
  'typetest/ShellTools/Credentials',
) {}
class PrepareError extends Schema.TaggedError<PrepareError>()('ShellPrepareError', {}) {}
declare const backend: Layer.Layer<
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner,
  PrepareError,
  Credentials
>
declare const writer: Spill.Writer
declare const process: ShellProcess.ShellProcess['Service']

test('retention and spill expose semantic errors and explicit scoped writers', () => {
  expect(Retention.make()).type.toBe<
    Effect.Effect<Retention.OutputBuffer, Retention.OutputRetentionError>
  >()
  expect(writer.append('stdout', new Uint8Array())).type.toBe<
    Effect.Effect<void, Spill.ToolSpillError>
  >()
  expect(writer.append).type.not.toBeCallableWith('all', 'text')
  expect(writer.commit).type.toBe<Effect.Effect<Spill.Reference, Spill.ToolSpillError>>()
  expect(Effect.flatMap(Spill.ToolSpill, (spill) => spill.open)).type.toBe<
    Effect.Effect<Spill.Writer, Spill.ToolSpillError, Spill.ToolSpill | Scope.Scope>
  >()
  expect(
    Spill.layer({ provider: 'test', directory: '/bound/spills', lifetime: 'persistent' }),
  ).type.toBe<
    Layer.Layer<Spill.ToolSpill, Spill.ToolSpillError, FileSystem.FileSystem | Path.Path>
  >()
  expect(Spill.layer).type.not.toBeCallableWith({ provider: 'test', directory: '/bound' })
  expect(ShellProcess.layerNative).type.toBe<
    Layer.Layer<ShellProcess.ShellProcess, never, ChildProcessSpawner.ChildProcessSpawner>
  >()
  expect(process.spawn).type.not.toBeCallableWith('echo')
})
test('shell operations retain invocation reporting, native binding inputs and prepare R/E', () => {
  const run = Bash.run({ command: ['program', 'literal;arg'] })
  expect(run).type.toBeAssignableTo<
    Effect.Effect<
      unknown,
      Bash.ShellError | Execution.Failure,
      | ToolExecution
      | FileSystem.FileSystem
      | Path.Path
      | ShellProcess.ShellProcess
      | Spill.ToolSpill
    >
  >()
  expect(run).type.not.toBeAssignableTo<Effect.Effect<unknown, Bash.ShellError>>()
  expect(Bash.run).type.not.toBeCallableWith({ command: 1 })
  expect(Bash.run).type.not.toBeCallableWith({ command: 'echo', timeout: 'seconds' })
  const prepared = Bash.run(
    { command: 'echo' },
    {
      prepare: (value) =>
        Effect.flatMap(Credentials, () => Effect.fail(new PrepareError())).pipe(Effect.as(value)),
    },
  )
  expect(prepared).type.toBeAssignableTo<
    Effect.Effect<
      unknown,
      PrepareError | Bash.ShellError | Execution.Failure,
      | ToolExecution
      | FileSystem.FileSystem
      | Path.Path
      | ShellProcess.ShellProcess
      | Spill.ToolSpill
      | Credentials
    >
  >()
  expect(prepared).type.not.toBeAssignableTo<typeof run>()
})
test('ready-made layers capture preparation services but never hide missing backend acquisition', () => {
  expect(Bash.layer({ prepare: (value) => Effect.as(ToolExecution, value) })).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bash'>,
      never,
      FileSystem.FileSystem | Path.Path | ShellProcess.ShellProcess | Spill.ToolSpill
    >
  >()
  expect(Bash.layer()).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bash'>,
      never,
      FileSystem.FileSystem | Path.Path | ShellProcess.ShellProcess | Spill.ToolSpill
    >
  >()
  expect(PowerShell.layer()).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'powershell'>,
      never,
      FileSystem.FileSystem | Path.Path | ShellProcess.ShellProcess | Spill.ToolSpill
    >
  >()
  const withPrepare = Bash.layer({ prepare: (value) => Effect.as(Credentials, value) })
  expect(withPrepare).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bash'>,
      never,
      FileSystem.FileSystem | Path.Path | ShellProcess.ShellProcess | Spill.ToolSpill | Credentials
    >
  >()
  const services = Layer.merge(
    ShellProcess.layerNative,
    Spill.layer({ provider: 'test', directory: '/bound/spills', lifetime: 'persistent' }),
  ).pipe(Layer.provideMerge(backend))
  const handlers = Bash.layer().pipe(Layer.provide(services))
  expect(handlers).type.toBe<
    Layer.Layer<Toolkit.Handler<'bash'>, PrepareError | Spill.ToolSpillError, Credentials>
  >()
  expect(Bash.layer().pipe(Layer.provide(ShellProcess.layerNative))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'bash'>>
  >()
})
