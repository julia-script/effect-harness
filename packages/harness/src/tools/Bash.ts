import * as SchemaField from '../SchemaField.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
import { Env, ExecutionError, ExecutionCallbackError } from '../Env.ts'
import { ToolError, ToolExecution, ToolInvalidParameters } from '../Error.ts'
import { Invocation, ToolCall, Result, type ToolResult } from '../Invocation.ts'
import * as Metadata from '../Tool.ts'
import * as Truncate from './Truncate.ts'
export const Parameters = Schema.Struct({
  command: Schema.String,
  timeout: SchemaField.optional(
    Schema.Finite.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(2147483.647)),
  ),
})
export type Input = typeof Parameters.Type
export interface Execution {
  command: string
  cwd: string
  env: Record<string, string>
  inheritEnv: boolean
}
export interface Options {
  readonly commandPrefix?: string | undefined
  readonly prepare?:
    | ((execution: Execution) => Effect.Effect<void, ToolError, Invocation | ToolCall>)
    | undefined
}
export interface PowerShellOptions extends Options {
  readonly programs?: ReadonlyArray<string> | undefined
}
const project = (result: unknown) => Metadata.decodeResult('bash', result)
export const tool = AiTool.make('bash', {
  description:
    'Execute a shell command. Combined stdout/stderr streams with a 2000-line/50KB tail; larger complete output spills to a diagnostic temp file.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .addDependency(ToolCall)
  .annotate(Metadata.Metadata, { replay: 'unsafe', output: { retain: 'tail' }, project })
export const powershell = AiTool.make('powershell', {
  description:
    'Execute PowerShell using pwsh or powershell directly with UTF8 output and the same bounded tail/full-output diagnostics.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .addDependency(ToolCall)
  .annotate(Metadata.Metadata, {
    replay: 'unsafe',
    output: { retain: 'tail' },
    project: (result) => Metadata.decodeResult('powershell', result),
  })
const execute = (name: 'bash' | 'powershell', options: PowerShellOptions) =>
  Effect.fnUntraced(function* (
    input: Input,
  ): Effect.fn.Return<ToolResult, ToolError, Env | Invocation | ToolCall> {
    const env = yield* Env
    const invocation = yield* Invocation
    const api = yield* ToolCall
    if (
      input.timeout !== undefined &&
      (!Number.isFinite(input.timeout) || input.timeout <= 0 || input.timeout > 2147483.647)
    )
      return yield* new ToolError({
        reason: new ToolInvalidParameters({ name, message: 'Invalid timeout' }),
      })
    const execution: Execution = {
      command: options.commandPrefix ? `${options.commandPrefix}\n${input.command}` : input.command,
      cwd: invocation.cwd,
      env: {},
      inheritEnv: true,
    }
    if (options.prepare !== undefined) yield* options.prepare(execution)
    const commands: ReadonlyArray<string | ReadonlyArray<string>> =
      name === 'bash'
        ? [execution.command]
        : (options.programs ?? ['pwsh', 'powershell']).map((program) => [
            program,
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-Command',
            `try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}\n${execution.command}`,
          ])
    let result: import('../Env.ts').ShellExecResult | undefined
    let last: ExecutionError | undefined
    const reported = new Set<string>()
    const diagnostic = (path: string): Effect.Effect<void, ExecutionError> =>
      Effect.suspend(() => {
        if (reported.has(path)) return Effect.void
        return api
          .diagnostic({
            kind: 'full_output',
            message: `Full output: ${path}`,
            detail: { path, severity: 'info' },
          })
          .pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                reported.add(path)
              }),
            ),
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new ExecutionCallbackError({
                    message: cause.message,
                    spillPath: path,
                    cause: cause,
                  }),
                }),
            ),
          )
      })
    for (const command of commands) {
      const outcome = yield* env
        .exec(command, {
          cwd: execution.cwd,
          env: execution.env,
          inheritEnv: execution.inheritEnv,
          ...(input.timeout === undefined ? {} : { timeout: input.timeout }),
          onOutput: (text, info) =>
            api.output(text, info.skipped).pipe(
              Effect.mapError(
                (cause) =>
                  new ExecutionError({
                    reason: new ExecutionCallbackError({ message: cause.message, cause: cause }),
                  }),
              ),
            ),
          onSpill: diagnostic,
          spill: { afterBytes: Truncate.DEFAULT_MAX_BYTES, afterLines: Truncate.DEFAULT_MAX_LINES },
          ...(api.outputWindow === undefined ? {} : { window: api.outputWindow }),
        })
        .pipe(Effect.result)
      const spill =
        outcome._tag === 'Success' ? outcome.success.spillPath : outcome.failure.spillPath
      if (spill !== undefined)
        yield* diagnostic(spill).pipe(
          Effect.mapError(
            (cause) =>
              new ToolError({
                reason: new ToolExecution({ name, message: cause.message, cause: cause }),
              }),
          ),
        )
      if (outcome._tag === 'Success') {
        result = outcome.success
        break
      }
      last = outcome.failure
      if (last.code !== 'spawn_error') break
    }
    if (result === undefined)
      return yield* new ToolError({
        reason: new ToolExecution({
          name,
          message: last?.message ?? 'No command to run',
          ...(last === undefined ? {} : { cause: last }),
        }),
      })
    if (result.exitCode !== 0)
      return yield* new ToolError({
        reason: new ToolExecution({ name, message: `Command exited with code ${result.exitCode}` }),
      })
    return {}
  })
export const handler = (options: Options = {}) => execute('bash', options)
export const powerShellHandler = (options: PowerShellOptions = {}) => execute('powershell', options)
