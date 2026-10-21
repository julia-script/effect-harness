/**
 * The `bash` handler, ported from pi's `tools/bash.ts` over `ChildProcessSpawner`: the command
 * runs through `bash -c` in the call's working directory, with stdout and stderr combined. The
 * tool's output limits keep the tail; output beyond them is also written whole to a temp file.
 *
 * Unlike pi, output is collected and returned at the end rather than streamed live. An
 * interruption, such as an abort, closes the process's scope, which kills it.
 */
import { Duration, Effect, Option, Result, Stream } from 'effect'
import { ChildProcess } from 'effect/process'
import { DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES } from '../CodingTools.ts'
import type { Config } from '../CodingTools.ts'
import type { Diagnostic, Output } from '../../tool/ToolResult.ts'
import { error, text } from '../../tool/ToolResult.ts'
import { utf8ByteLength } from './truncate.ts'
import type { Workspace } from './workspace.ts'
import { cwdOf } from './workspace.ts'

const MAX_TIMEOUT_SECONDS = 2_147_483_647 / 1000

export interface Params {
  readonly command: string
  readonly timeout?: number
}

export type BashConfig = Config & { readonly commandPrefix?: string }

const invalidTimeout = (timeout: number | undefined): string | undefined => {
  if (timeout === undefined) return undefined
  if (!Number.isFinite(timeout) || timeout <= 0)
    return 'Invalid timeout: must be a finite number of seconds'
  if (timeout > MAX_TIMEOUT_SECONDS)
    return `Invalid timeout: maximum is ${MAX_TIMEOUT_SECONDS} seconds`
  return undefined
}

const exceedsLimits = (output: string) =>
  utf8ByteLength(output) > DEFAULT_MAX_BYTES || output.split('\n').length > DEFAULT_MAX_LINES

/** The command's combined output, collected into `chunks` as it arrives, then its exit code. */
const run = Effect.fnUntraced(function* (
  workspace: Workspace,
  command: string,
  cwd: string,
  chunks: Array<string>,
) {
  const handle = yield* workspace.spawner.spawn(
    ChildProcess.make('bash', ['-c', command], { cwd, extendEnv: true, stdin: 'ignore' }),
  )
  yield* Stream.runForEach(Stream.decodeText(handle.all), (chunk) =>
    Effect.sync(() => chunks.push(chunk)),
  )
  return yield* handle.exitCode
}, Effect.scoped)

/** A file with all of `output`, when it exceeds the limits; spilling is best-effort. */
const spill = (workspace: Workspace, output: string): Effect.Effect<Option.Option<string>> =>
  exceedsLimits(output)
    ? Effect.option(
        Effect.tap(workspace.fs.makeTempFile({ prefix: 'pi-bash-', suffix: '.log' }), (path) =>
          workspace.fs.writeFileString(path, output),
        ),
      )
    : Effect.succeedNone

export const bash = Effect.fnUntraced(function* (
  workspace: Workspace,
  params: Params,
  config: BashConfig,
): Effect.fn.Return<Output, Output> {
  const invalid = invalidTimeout(params.timeout)
  if (invalid !== undefined) return yield* Effect.fail(text(invalid))
  const prefix = config.commandPrefix
  const command =
    prefix === undefined || prefix === '' ? params.command : `${prefix}\n${params.command}`
  const chunks: Array<string> = []
  const running = run(workspace, command, cwdOf(workspace, config), chunks)
  const finished = yield* Effect.result(
    params.timeout === undefined
      ? Effect.asSome(running)
      : Effect.timeoutOption(running, Duration.seconds(params.timeout)),
  )
  const all = chunks.join('')
  const spilled = yield* spill(workspace, all)
  const diagnostics: Array<Diagnostic> = Option.match(spilled, {
    onNone: () => [],
    onSome: (path) => [{ severity: 'info', code: 'full_output', message: `Full output: ${path}` }],
  })
  const fullOutputPath = Option.match(spilled, {
    onNone: () => ({}),
    onSome: (path) => ({ fullOutputPath: path }),
  })
  if (Result.isFailure(finished))
    return yield* Effect.fail(
      text(all, {
        diagnostics: [...diagnostics, error('spawn_error', finished.failure.message)],
      }),
    )
  if (Option.isNone(finished.success)) {
    const timedOut = error('timeout', `Command timed out after ${params.timeout} seconds`)
    return yield* Effect.fail(text(all, { diagnostics: [...diagnostics, timedOut] }))
  }
  const exitCode = finished.success.value
  const details = { exitCode, ...fullOutputPath }
  if (exitCode === 0) return text(all, { diagnostics, details })
  const exited = error('exit_code', `Command exited with code ${exitCode}`)
  return yield* Effect.fail(text(all, { diagnostics: [...diagnostics, exited], details }))
})
