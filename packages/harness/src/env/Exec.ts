import * as Serialization from '../Serialization.ts'
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import type * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Pull from 'effect/Pull'
import type * as Path from 'effect/Path'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import type { ChildProcessSpawner, ChildProcessHandle } from 'effect/process/ChildProcessSpawner'
import {
  ExecutionError,
  type Options,
  type ShellExecOptions,
  type ShellExecResult,
  ExecutionCallbackError,
  ExecutionSpawnError,
  ExecutionTimeout,
  ExecutionUnknown,
} from '../Env.ts'
import * as Decode from './Decode.ts'

export const make = Effect.fnUntraced(function* (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  spawner: ChildProcessSpawner['Service'],
  defaults: Options,
) {
  const active = new Set<ChildProcessHandle>()
  const exec = (
    command: string | ReadonlyArray<string>,
    options: ShellExecOptions = {},
  ): Effect.Effect<ShellExecResult, ExecutionError> =>
    Effect.scoped(
      Effect.gen(function* () {
        if (typeof command !== 'string' && command.length === 0)
          return yield* new ExecutionError({
            reason: new ExecutionSpawnError({ message: 'Empty argv' }),
          })
        if (
          options.timeout !== undefined &&
          (!Number.isFinite(options.timeout) ||
            options.timeout <= 0 ||
            options.timeout > 2147483.647)
        )
          return yield* new ExecutionError({
            reason: new ExecutionTimeout({ message: 'Invalid timeout' }),
          })
        if (
          options.spill !== undefined &&
          (!Number.isSafeInteger(options.spill.afterBytes) ||
            options.spill.afterBytes < 0 ||
            !Number.isSafeInteger(options.spill.afterLines) ||
            options.spill.afterLines < 0)
        )
          return yield* new ExecutionError({
            reason: new ExecutionUnknown({ message: 'Invalid spill threshold' }),
          })
        const nativeOptions = {
          cwd: path.resolve(defaults.cwd, options.cwd ?? defaults.cwd),
          env:
            options.inheritEnv === false ? { ...options.env } : { ...defaults.env, ...options.env },
          extendEnv: options.inheritEnv !== false,
          forceKillAfter: 1000,
        }
        yield* fs.access(nativeOptions.cwd).pipe(
          Effect.mapError(
            (error) =>
              new ExecutionError({
                reason: new ExecutionSpawnError({ message: error.message, cause: error }),
              }),
          ),
        )
        let instruction: ChildProcess.StandardCommand
        if (typeof command === 'string') {
          const shell =
            defaults.resolveShell === undefined
              ? { program: defaults.shell ?? 'sh', args: ['-c'], commandOnStdin: false }
              : yield* defaults.resolveShell
          instruction = ChildProcess.make(
            shell.program,
            shell.commandOnStdin === true ? shell.args : [...shell.args, command],
            {
              ...nativeOptions,
              ...(shell.commandOnStdin === true
                ? { stdin: { stream: Stream.succeed(new TextEncoder().encode(command + '\n')) } }
                : {}),
            },
          )
        } else instruction = ChildProcess.make(command[0] ?? '', command.slice(1), nativeOptions)

        const handle = yield* spawner.spawn(instruction).pipe(
          Effect.mapError(
            (error) =>
              new ExecutionError({
                reason: new ExecutionSpawnError({ message: error.message, cause: error }),
              }),
          ),
        )
        active.add(handle)
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            active.delete(handle)
          }),
        )
        let spillPath: string | undefined
        let chunks: Uint8Array[] = []
        let bytes = 0
        let newlines = 0
        let lastByte = 0
        let lastAt = yield* Clock.currentTimeMillis
        const decoders = { stdout: Decode.make(), stderr: Decode.make() }
        const spillError = (error: { readonly message: string }) =>
          new ExecutionError({
            reason: new ExecutionUnknown({
              message: error.message,
              ...(spillPath === undefined ? {} : { spillPath }),
              cause: error,
            }),
          })
        const callback = (operation: () => Effect.Effect<void, ExecutionError>) =>
          Effect.suspend(operation).pipe(
            Effect.catchCause((cause) =>
              Cause.hasInterrupts(cause)
                ? Effect.failCause(
                    Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
                  )
                : Effect.fail(
                    new ExecutionError({
                      reason: new ExecutionCallbackError({
                        message: Serialization.errorText(Cause.squash(cause)),
                        ...(spillPath === undefined ? {} : { spillPath }),
                        cause: cause,
                      }),
                    }),
                  ),
            ),
          )
        const processChunk = Effect.fnUntraced(function* (chunk: {
          readonly bytes: Uint8Array
          readonly stream: 'stdout' | 'stderr'
        }) {
          bytes += chunk.bytes.length
          for (const byte of chunk.bytes) if (byte === 10) newlines++
          if (chunk.bytes.length > 0) lastByte = chunk.bytes[chunk.bytes.length - 1] ?? 0
          if (spillPath === undefined && options.spill !== undefined) {
            chunks.push(chunk.bytes.slice())
            const lines = bytes === 0 ? 0 : newlines + (lastByte === 10 ? 0 : 1)
            if (
              options.spill !== undefined &&
              (bytes > options.spill.afterBytes || lines > options.spill.afterLines)
            ) {
              yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  spillPath = yield* fs
                    .makeTempFile({ prefix: 'effect-harness-output-', suffix: '.log' })
                    .pipe(Effect.mapError(spillError))
                  const prefix = new Uint8Array(bytes)
                  let offset = 0
                  for (const value of chunks) {
                    prefix.set(value, offset)
                    offset += value.length
                  }
                  chunks = []
                  yield* Effect.uninterruptible(
                    fs.writeFile(spillPath, prefix).pipe(Effect.mapError(spillError)),
                  )
                  if (options.onSpill !== undefined) {
                    const published = spillPath
                    yield* callback(() => options.onSpill?.(published) ?? Effect.void)
                  }
                }),
              )
            }
          } else if (spillPath !== undefined)
            yield* Effect.uninterruptible(
              fs.writeFile(spillPath, chunk.bytes, { flag: 'a' }).pipe(Effect.mapError(spillError)),
            )
          const text = Decode.decode(decoders[chunk.stream], chunk.bytes)
          if (text !== '' && options.onOutput !== undefined)
            yield* callback(() => options.onOutput?.(text, { stream: chunk.stream }) ?? Effect.void)
        })
        const output = handle.stdout.pipe(
          Stream.map((bytes) => ({ bytes, stream: 'stdout' as const })),
          Stream.merge(
            handle.stderr.pipe(Stream.map((bytes) => ({ bytes, stream: 'stderr' as const }))),
          ),
        )
        const run = Effect.gen(function* () {
          const failed = yield* Deferred.make<never, ExecutionError>()
          const pull = yield* Stream.toPull(output.pipe(Stream.mapError(spillError)))
          let waiting:
            | Fiber.Fiber<
                Option.Option<
                  ReadonlyArray<{
                    readonly bytes: Uint8Array
                    readonly stream: 'stdout' | 'stderr'
                  }>
                >,
                ExecutionError
              >
            | undefined
          let idleClosed = false
          const reading = yield* Effect.gen(function* () {
            while (!idleClosed) {
              lastAt = yield* Clock.currentTimeMillis
              waiting = yield* pull.pipe(
                Effect.asSome,
                Pull.catchDone(() => Effect.succeedNone),
                Effect.forkChild,
              )
              const batch = yield* Fiber.join(waiting)
              waiting = undefined
              if (Option.isNone(batch)) return
              // Every pulled chunk is admitted, including the rest of this batch.
              for (const chunk of batch.value) yield* processChunk(chunk)
            }
          }).pipe(
            Effect.catchCauseIf(
              (cause) => idleClosed && Cause.hasInterrupts(cause),
              () => Effect.void,
            ),
            Effect.tapError((error) => Deferred.fail(failed, error)),
            Effect.forkScoped,
          )
          const status = yield* handle.exitCode.pipe(
            Effect.mapError(spillError),
            Effect.raceFirst(Deferred.await(failed)),
          )
          while (true) {
            const joined = yield* Fiber.join(reading).pipe(Effect.timeoutOption(100))
            if (Option.isSome(joined)) break
            const now = yield* Clock.currentTimeMillis
            if (waiting !== undefined && now - lastAt >= 100) {
              idleClosed = true
              // Interrupt only the pending pull, never the admitted consumer.
              yield* Fiber.interrupt(waiting)
              yield* Fiber.join(reading)
              break
            }
          }
          for (const stream of ['stdout', 'stderr'] as const) {
            const text = Decode.decode(decoders[stream])
            if (text !== '' && options.onOutput !== undefined)
              yield* callback(() => options.onOutput?.(text, { stream }) ?? Effect.void)
          }
          return { exitCode: Number(status), ...(spillPath === undefined ? {} : { spillPath }) }
        })
        const finished =
          options.timeout === undefined
            ? run
            : run.pipe(
                Effect.timeoutOrElse({
                  duration: options.timeout * 1000,
                  orElse: () =>
                    Effect.fail(
                      new ExecutionError({
                        reason: new ExecutionTimeout({
                          message: `Command timed out after ${options.timeout} seconds`,
                          ...(spillPath === undefined ? {} : { spillPath }),
                        }),
                      }),
                    ),
                }),
              )
        return yield* finished
      }),
    )
  const cleanup = Effect.suspend(() =>
    Effect.asVoid(
      Effect.forEach(
        [...active],
        (handle) => handle.kill({ forceKillAfter: 1000 }).pipe(Effect.ignore),
        { concurrency: 'unbounded' },
      ),
    ),
  )
  yield* Effect.addFinalizer(() => cleanup)
  return { exec, cleanup }
})
