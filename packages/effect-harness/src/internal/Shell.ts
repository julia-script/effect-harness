import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Scope from 'effect/Scope'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as ChildProcess from 'effect/process/ChildProcess'
import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as OutputRetention from '../OutputRetention.js'
import * as ToolSpill from '../ToolSpill.js'
import { ToolExecution } from '../ToolExecution.js'
import type * as ToolResult from '../ToolResult.js'
import * as ShellProcess from '../coding/ShellProcess.js'

export const Parameters = Schema.Struct({
  command: Schema.Union([Schema.String, Schema.Array(Schema.String)]),
  timeout: Schema.optionalKey(Schema.Finite),
  cwd: Schema.optionalKey(Schema.String),
  env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  extendEnv: Schema.optionalKey(Schema.Boolean),
})
export type Parameters = typeof Parameters.Type
export const Outcome = Schema.Struct({
  output: Schema.String,
  truncated: Schema.Boolean,
  retention: OutputRetention.Snapshot,
  exitCode: Schema.NullOr(Schema.Finite),
  spill: Schema.optionalKey(ToolSpill.Reference),
  cleanup: ShellProcess.Cleanup,
})
export type Outcome = typeof Outcome.Type
export class ShellError extends Schema.TaggedError<ShellError>()('ShellError', {
  reason: Schema.Literals([
    'invalidParameters',
    'prepare',
    'spawn',
    'unavailable',
    'process',
    'nonzero',
    'timeout',
    'retention',
    'spill',
    'unsupportedCleanup',
  ]),
  message: Schema.String,
  outcome: Schema.optionalKey(Outcome),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export const FailureSchema = Schema.Struct({
  reason: ShellError.fields.reason,
  message: Schema.String,
  outcome: Schema.optionalKey(Outcome),
})
export type Failure = typeof FailureSchema.Type
export const failureOutput = (error: ShellError): ToolResult.Output<Failure> => ({
  content: [
    Prompt.textPart({ text: [error.outcome?.output, error.message].filter(Boolean).join('\n') }),
  ],
  structuredOutput: {
    reason: error.reason,
    message: error.message,
    ...(error.outcome === undefined ? {} : { outcome: error.outcome }),
  },
  diagnostics: [{ severity: 'error', code: error.reason, message: error.message }],
})

export interface Prepared {
  readonly command: string | ReadonlyArray<string>
  readonly cwd: string
  readonly env?: Readonly<Record<string, string | undefined>> | undefined
  readonly extendEnv?: boolean | undefined
}
export interface Options<E = never, R = never> {
  /** Strict cleanup refuses native best effort before any process is started. */
  readonly requireOwnedTree?: boolean | undefined
  readonly programs?: ReadonlyArray<string> | undefined
  readonly commandPrefix?: string | undefined
  readonly cwd?: string | undefined
  readonly env?: Readonly<Record<string, string | undefined>> | undefined
  readonly extendEnv?: boolean | undefined
  readonly retention?: OutputRetention.Policy | undefined
  /** Effectful preparation remains local to the handler Layer; its R/E stay visible. */
  readonly prepare?: ((execution: Prepared) => Effect.Effect<Prepared, E, R>) | undefined
}
const check = (
  command: Prepared['command'],
  timeout: number | undefined,
  prefix: string | undefined,
) =>
  Effect.suspend(() => {
    if (
      timeout !== undefined &&
      (!Number.isFinite(timeout) || timeout <= 0 || timeout > 2147483.647)
    )
      return Effect.fail(
        new ShellError({
          reason: 'invalidParameters',
          message: 'Timeout must be positive finite seconds, at most 2147483.647.',
        }),
      )
    if (typeof command !== 'string' && (command.length === 0 || !command[0]))
      return Effect.fail(
        new ShellError({
          reason: 'invalidParameters',
          message: 'Direct argv requires a nonempty executable.',
        }),
      )
    if (typeof command !== 'string' && prefix)
      return Effect.fail(
        new ShellError({
          reason: 'invalidParameters',
          message: 'A script prefix requires a string command; direct argv is never shell parsed.',
        }),
      )
    if ((typeof command === 'string' ? [command] : command).some((s) => s.includes('\0')))
      return Effect.fail(
        new ShellError({
          reason: 'invalidParameters',
          message: 'Command and argv cannot contain NUL.',
        }),
      )
    return Effect.void
  })

export const execute = Effect.fnUntraced(function* <E, R>(
  kind: 'bash' | 'powershell',
  input: Parameters,
  options: Options<E, R> = {},
) {
  yield* check(input.command, input.timeout, options.commandPrefix)
  const path = yield* Path.Path
  const fs = yield* FileSystem.FileSystem
  const reporter = yield* ToolExecution
  const processes = yield* ShellProcess.ShellProcess
  if (options.requireOwnedTree && processes.cleanup !== 'owned-tree')
    return yield* new ShellError({
      reason: 'unsupportedCleanup',
      message:
        'Strict owned-tree cleanup requires an injected ShellProcess capability; native process-group cleanup is best effort.',
    })
  const provider = yield* ToolSpill.ToolSpill
  const buffer = yield* OutputRetention.make(options.retention).pipe(
    Effect.mapError(
      (cause) => new ShellError({ reason: 'retention', message: cause.message, cause }),
    ),
  )
  let prepared: Prepared = {
    command:
      typeof input.command === 'string' && options.commandPrefix
        ? `${options.commandPrefix}\n${input.command}`
        : input.command,
    cwd: path.resolve(input.cwd ?? options.cwd ?? '.'),
    env: input.env ?? options.env,
    extendEnv: input.extendEnv ?? options.extendEnv,
  }
  if (options.prepare !== undefined) prepared = yield* options.prepare(prepared)
  yield* check(prepared.command, input.timeout, undefined)
  const cwd = path.resolve(prepared.cwd)
  yield* fs
    .access(cwd)
    .pipe(
      Effect.mapError(
        (cause) => new ShellError({ reason: 'process', message: cause.message, cause }),
      ),
    )
  const programs = options.programs ?? (kind === 'bash' ? ['bash'] : ['pwsh', 'powershell'])
  if (
    typeof prepared.command === 'string' &&
    (programs.length === 0 || programs.some((p) => !p || p.includes('\0')))
  )
    return yield* new ShellError({
      reason: 'invalidParameters',
      message: 'At least one nonempty shell executable is required.',
    })
  const commandOptions: ChildProcess.CommandOptions = {
    cwd,
    env: prepared.env === undefined ? undefined : { ...prepared.env },
    extendEnv: prepared.extendEnv,
    shell: false,
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
    forceKillAfter: '100 millis',
  }
  const commands: ChildProcess.Command[] = []
  if (typeof prepared.command !== 'string') {
    const [program, ...args] = prepared.command
    if (program === undefined)
      return yield* new ShellError({ reason: 'invalidParameters', message: 'Empty argv.' })
    commands.push(ChildProcess.make(program, args, commandOptions))
  } else
    for (const program of programs) {
      const args =
        kind === 'bash'
          ? ['-c', prepared.command]
          : [
              '-NoProfile',
              '-NonInteractive',
              '-ExecutionPolicy',
              'Bypass',
              '-Command',
              `try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}\n${prepared.command}`,
            ]
      commands.push(ChildProcess.make(program, args, commandOptions))
    }
  let exitCode: number | null = null
  let spill: ToolSpill.Reference | undefined
  const outcome = (): Outcome => {
    const retention = OutputRetention.snapshot(buffer)
    return {
      output: retention.text,
      truncated: retention.truncated,
      retention,
      exitCode,
      cleanup: processes.cleanup,
      ...(spill === undefined ? {} : { spill }),
    }
  }
  const operation = Effect.scoped(
    Effect.gen(function* () {
      const writer = yield* provider.open.pipe(
        Effect.mapError(
          (cause) => new ShellError({ reason: 'spill', message: cause.message, cause }),
        ),
      )
      const decoders = {
        stdout: new TextDecoder('utf-8', { ignoreBOM: true }),
        stderr: new TextDecoder('utf-8', { ignoreBOM: true }),
      }
      const mutex = yield* Semaphore.make(1)
      const emit = Effect.fnUntraced(function* (text: string) {
        if (!text) return
        yield* OutputRetention.append(buffer, text).pipe(
          Effect.mapError(
            (cause) => new ShellError({ reason: 'retention', message: cause.message, cause }),
          ),
        )
        yield* reporter.output(text)
      })
      const finalize = mutex
        .withPermit(
          Effect.gen(function* () {
            yield* emit(decoders.stdout.decode())
            yield* emit(decoders.stderr.decode())
            const finished = yield* OutputRetention.finish(buffer).pipe(Effect.result)
            if (outcome().truncated) {
              const ref = yield* writer.commit.pipe(
                Effect.mapError(
                  (cause) => new ShellError({ reason: 'spill', message: cause.message, cause }),
                ),
              )
              spill = ref
              yield* reporter.diagnostic({
                severity: 'info',
                code: 'full_output',
                message: `Full raw output: ${ref.path} (${ref.lifetime} lifetime).`,
              })
            }
            yield* Effect.fromResult(finished).pipe(
              Effect.mapError(
                (cause) => new ShellError({ reason: 'retention', message: cause.message, cause }),
              ),
            )
          }),
        )
        .pipe(Effect.onExit(() => reporter.details(outcome())))
      const run = Effect.scoped(
        Effect.gen(function* () {
          let handle: ChildProcessSpawner.ChildProcessHandle | undefined
          for (const command of commands) {
            const candidate = yield* processes.spawn(command).pipe(Effect.result)
            if (Result.isFailure(candidate)) {
              if (candidate.failure.reason === 'notFound') continue
              return yield* new ShellError({
                reason: 'spawn',
                message: candidate.failure.message,
                cause: candidate.failure,
              })
            }
            handle = candidate.success
            break
          }
          if (handle === undefined)
            return yield* new ShellError({
              reason: 'unavailable',
              message: `No ${kind} executable could start through the supplied spawner.`,
            })
          const drain = (stream: 'stdout' | 'stderr') =>
            Stream.runForEach(handle[stream], (bytes) =>
              mutex.withPermit(
                Effect.uninterruptible(
                  Effect.gen(function* () {
                    yield* writer
                      .append(stream, bytes)
                      .pipe(
                        Effect.mapError(
                          (cause) =>
                            new ShellError({ reason: 'spill', message: cause.message, cause }),
                        ),
                      )
                    yield* emit(decoders[stream].decode(bytes, { stream: true }))
                  }),
                ),
              ),
            ).pipe(
              Effect.mapError((cause) =>
                cause instanceof ShellError || cause._tag !== 'PlatformError'
                  ? cause
                  : new ShellError({ reason: 'process', message: cause.message, cause }),
              ),
            )
          // Never merge raw streams before decoding; both pipes drain independently with backpressure.
          const results = yield* Effect.all(
            [
              handle.exitCode.pipe(
                Effect.mapError(
                  (cause) => new ShellError({ reason: 'process', message: cause.message, cause }),
                ),
              ),
              drain('stdout'),
              drain('stderr'),
            ],
            { concurrency: 'unbounded' },
          )
          exitCode = results[0]
        }),
      )
      const timed =
        input.timeout === undefined
          ? run
          : run.pipe(
              Effect.timeoutOrElse({
                duration: `${input.timeout} seconds`,
                orElse: () =>
                  Effect.fail(
                    new ShellError({
                      reason: 'timeout',
                      message: `Command timed out after ${input.timeout} seconds.`,
                    }),
                  ),
              }),
            )
      yield* timed.pipe(Effect.onExit(() => finalize))
    }),
  )
  yield* operation.pipe(
    Effect.catchTag('ShellError', (error) =>
      Effect.fail(
        new ShellError({
          reason: error.reason,
          message: error.message,
          cause: error.cause,
          outcome: outcome(),
        }),
      ),
    ),
  )
  const result = outcome()
  if (exitCode !== 0)
    return yield* new ShellError({
      reason: 'nonzero',
      message: `Command exited with code ${exitCode}.`,
      outcome: result,
    })
  return {
    content: [Prompt.textPart({ text: result.output })],
    structuredOutput: result,
    diagnostics:
      spill === undefined
        ? []
        : [
            {
              severity: 'info' as const,
              code: 'full_output',
              message: `Full raw output: ${spill.path}`,
            },
          ],
  } satisfies ToolResult.Output<Outcome>
})

/** Freeze mutable options at Layer construction, without ambient configuration. */
export const freeze = <E, R>(options: Options<E, R>): Options<E, R> => ({
  ...options,
  programs: options.programs === undefined ? undefined : [...options.programs],
  env: options.env === undefined ? undefined : { ...options.env },
  retention: options.retention === undefined ? undefined : { ...options.retention },
})

/** Capture toolkit inputs, preserving the invocation's current reporter and Scope. */
export const bindPreparation = Effect.fnUntraced(function* <E, R>(options: Options<E, R>) {
  type Invocation = ToolExecution | Scope.Scope
  const captured = yield* Effect.context<Exclude<R, Invocation>>()
  const context = Context.omit(ToolExecution, Scope.Scope)(captured)
  const prepare = options.prepare
  const local: Options<ShellError, Invocation> = {
    ...options,
    prepare:
      prepare === undefined
        ? undefined
        : Effect.fnUntraced(function* (execution: Prepared) {
            const reporter = yield* ToolExecution
            const scope = yield* Scope.Scope
            // Exclude<R, Invocation> plus the current invocation provides every R.
            // TypeScript cannot prove that generic conditional-set identity for Context.
            const supplied = context.pipe(
              Context.add(ToolExecution, reporter),
              Context.add(Scope.Scope, scope),
            ) as Context.Context<R>
            return yield* prepare(execution).pipe(
              Effect.provideContext(supplied),
              Effect.mapError(
                (cause) =>
                  new ShellError({
                    reason: 'prepare',
                    message: 'Shell preparation failed.',
                    cause,
                  }),
              ),
            )
          }),
  }
  return local
})
