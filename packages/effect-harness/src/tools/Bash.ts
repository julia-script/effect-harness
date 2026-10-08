/**
 * Bash and PowerShell tools with bounded output and spill diagnostics.
 */
import * as Result from 'effect/Result'
import * as Time from '../Time.ts'
import * as Ref from 'effect/Ref'
import * as HashSet from 'effect/HashSet'
import * as SchemaField from '../SchemaField.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import { Env } from '../Env.ts'
import { ExecutionError, ExecutionCallbackError } from '../ExecutionError.ts'

import { ToolError, ToolExecutionError, ToolInvalidParametersError } from '../ToolError.ts'
import {
  Invocation,
  ToolCall,
  Result as ToolResultSchema,
  type Result as InvocationResult,
} from '../Invocation.ts'
import * as ToolRegistration from '../ToolRegistration.ts'
import * as Truncate from './Truncate.ts'
/**
 * Schema for shell command and optional timeout in seconds.
 *
 * @category schemas
 */
export const Parameters = Schema.Struct({
  command: Schema.String,
  timeout: SchemaField.optional(Time.CommandTimeoutFromSeconds),
})
/**
 * Resolved shell command, cwd and environment passed to the host process.
 *
 * @category models
 */
export interface Execution {
  command: string
  cwd: string
  env: Record<string, string>
  inheritEnv: boolean
}
const project = (result: unknown) => ToolRegistration.decodeResult('bash', result)
/**
 * Native bash tool running a command through the configured environment shell.
 *
 * **Details**
 *
 * timeout is expressed in seconds. Output is bounded with spill files and optional progress
 * callbacks.
 *
 * **Gotchas**
 *
 * Shell execution has unsafe replay. Scoped cancellation terminates process work and joins
 * cleanup; nonzero exits are command results, while spawn, timeout and callback failures
 * remain typed errors.
 *
 * @category constants
 */
export const tool = Tool.make('bash', {
  description:
    'Execute a shell command. Combined stdout/stderr streams with a 2000-line/50KB tail; larger complete output spills to a diagnostic temp file.',
  parameters: Parameters,
  success: ToolResultSchema,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .addDependency(ToolCall)
  .annotate(ToolRegistration.Metadata, { replay: 'unsafe', output: { retain: 'tail' }, project })
/**
 * Native PowerShell tool declaration with bounded output and full-output spill diagnostics.
 *
 * @category constants
 */
export const powershell = Tool.make('powershell', {
  description:
    'Execute PowerShell using pwsh or powershell directly with UTF8 output and the same bounded tail/full-output diagnostics.',
  parameters: Parameters,
  success: ToolResultSchema,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .addDependency(ToolCall)
  .annotate(ToolRegistration.Metadata, {
    replay: 'unsafe',
    output: { retain: 'tail' },
    project: (result) => ToolRegistration.decodeResult('powershell', result),
  })
const execute = (name: 'bash' | 'powershell', options: powerShellHandler.Options) =>
  Effect.fnUntraced(function* (
    input: Parameters,
  ): Effect.fn.Return<InvocationResult, ToolError, Env | Invocation | ToolCall> {
    const env = yield* Env
    const invocation = yield* Invocation
    const api = yield* ToolCall
    if (input.timeout !== undefined)
      yield* Schema.decodeEffect(Schema.toType(Time.CommandTimeoutFromSeconds))(input.timeout).pipe(
        Effect.mapError(
          (cause) =>
            new ToolError({
              reason: new ToolInvalidParametersError({ name, message: 'Invalid timeout', cause }),
            }),
        ),
      )
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
    let result: import('../Env.ts').Env.ShellExecResult | undefined
    let last: ExecutionError | undefined
    const reported = yield* Ref.make(HashSet.empty<string>())
    const diagnostic = Effect.fnUntraced(
      function* (path: string): Effect.fn.Return<void, ToolError> {
        if (HashSet.has(yield* Ref.get(reported), path)) return
        return yield* api
          .diagnostic({
            kind: 'full_output',
            severity: 'info',
            message: `Full output: ${path}`,
            detail: { path },
          })
          .pipe(Effect.tap(() => Ref.update(reported, HashSet.add(path))))
      },
      (effect, path) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ExecutionError({
                reason: new ExecutionCallbackError({
                  message: cause.message,
                  spillPath: path,
                  cause,
                }),
              }),
          ),
        ),
    )
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
      const spill = Result.match(outcome, {
        onSuccess: (self) => self.spillPath,
        onFailure: (self) => self.spillPath,
      })
      if (spill !== undefined)
        yield* diagnostic(spill).pipe(
          Effect.mapError(
            (cause) =>
              new ToolError({
                reason: new ToolExecutionError({ name, message: cause.message, cause: cause }),
              }),
          ),
        )
      const next = Result.match(outcome, {
        onSuccess: (self) => ({ result: self, failure: undefined }),
        onFailure: (self) => ({ result: undefined, failure: self }),
      })
      if (next.result !== undefined) {
        result = next.result
        break
      }
      last = next.failure
      if (last === undefined) break
      if (last.code !== 'spawn_error') break
    }
    if (result === undefined)
      return yield* new ToolError({
        reason: new ToolExecutionError({
          name,
          message: last?.message ?? 'No command to run',
          ...(last === undefined ? {} : { cause: last }),
        }),
      })
    if (result.exitCode !== 0)
      return yield* new ToolError({
        reason: new ToolExecutionError({
          name,
          message: `Command exited with code ${result.exitCode}`,
        }),
      })
    return {}
  })
/**
 * Creates the Bash handler using supplied shell execution options.
 *
 * @category combinators
 */
export const handler = (
  options: handler.Options = {},
): ((
  input: Parameters,
) => Effect.Effect<InvocationResult, ToolError, Env | Invocation | ToolCall>) =>
  execute('bash', options)
/**
 * Creates the PowerShell handler using supplied shell execution options.
 *
 * @category combinators
 */
export const powerShellHandler = (
  options: powerShellHandler.Options = {},
): ((
  input: Parameters,
) => Effect.Effect<InvocationResult, ToolError, Env | Invocation | ToolCall>) =>
  execute('powershell', options)

/**
 * Checks whether a value satisfies the decoded `Parameters` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isParameters: (u: unknown) => u is Parameters = Schema.is(Parameters)

/**
 * Shell command and optional timeout in seconds.
 *
 * @category models
 */
export type Parameters = typeof Parameters.Type

/**
 * Type-level contracts for `handler`.
 *
 */
export declare namespace handler {
  /**
   * Configuration accepted by handler.
   *
   * @category models
   */
  interface Options {
    readonly commandPrefix?: string | undefined
    readonly prepare?:
      | ((execution: Execution) => Effect.Effect<void, ToolError, Invocation | ToolCall>)
      | undefined
  }
}

/**
 * Type-level contracts for `powerShellHandler`.
 *
 */
export declare namespace powerShellHandler {
  /**
   * Configuration accepted by powerShellHandler.
   *
   * @category models
   */
  interface Options extends handler.Options {
    readonly programs?: ReadonlyArray<string> | undefined
  }
}
