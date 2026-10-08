import { constant } from 'effect/Function'
import * as Ref from 'effect/Ref'
import * as HashSet from 'effect/HashSet'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Schema from 'effect/Schema'
import * as Time from '../../Time.ts'
import * as Serialization from '../../Serialization.ts'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import type * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Pull from 'effect/Pull'
import type * as Path from 'effect/Path'
import * as Stream from 'effect/Stream'
import * as Semaphore from 'effect/Semaphore'
import * as ChildProcess from 'effect/process/ChildProcess'
import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import {
  ExecutionError,
  type Options,
  type ShellExecOptions,
  type ShellExecResult,
  ExecutionCallbackError,
  ExecutionSpawnError,
  ExecutionTimeout,
  ExecutionUnknown,
} from '../../Env.ts'
import * as Decode from '../Decode.ts'

export const make = Effect.fnUntraced(function* (input: {
  readonly fs: FileSystem.FileSystem
  readonly path: Path.Path
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner['Service']
  readonly defaults: Options
}): Effect.fn.Return<
  Pick<import('../../Env.ts').Env['Service'], 'exec'>,
  never,
  import('effect/Scope').Scope
> {
  const { fs, path, spawner, defaults } = input
  const active = yield* Ref.make(HashSet.empty<ChildProcessSpawner.ChildProcessHandle>())
  const admission = yield* Semaphore.make(1)
  const closed = yield* Ref.make(false)
  const exec = Effect.fnUntraced(function* (
    command: string | ReadonlyArray<string>,
    options: ShellExecOptions = {},
  ): Effect.fn.Return<ShellExecResult, ExecutionError, import('effect/Scope').Scope> {
    if (yield* Ref.get(closed))
      return yield* new ExecutionError({
        reason: new ExecutionUnknown({ message: 'Execution owner is closed' }),
      })
    if (typeof command !== 'string' && command.length === 0)
      return yield* new ExecutionError({
        reason: new ExecutionSpawnError({ message: 'Empty argv' }),
      })
    const timeout =
      options.timeout === undefined
        ? undefined
        : yield* Time.duration(options.timeout).pipe(
            Effect.flatMap(Schema.decodeEffect(Schema.toType(Time.CommandTimeout))),
            Effect.mapError(
              (cause) =>
                new ExecutionError({
                  reason: new ExecutionTimeout({ message: 'Invalid timeout', cause }),
                }),
            ),
          )
    if (options.window !== undefined)
      yield* Time.duration(options.window.minIntervalMs).pipe(
        Effect.flatMap(Schema.decodeEffect(Schema.toType(Time.NonnegativeMillis))),
        Effect.mapError(
          (cause) =>
            new ExecutionError({
              reason: new ExecutionUnknown({ message: 'Invalid output window interval', cause }),
            }),
        ),
      )
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
      env: options.inheritEnv === false ? { ...options.env } : { ...defaults.env, ...options.env },
      extendEnv: options.inheritEnv !== false,
      forceKillAfter: '1 second' as const,
    }
    // Cwd access is a fresh admission check for this process; no bulk access operation exists,
    // and sharing earlier success across runtime boundaries could miss deletion or permission changes.
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

    const handle = yield* admission.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (yield* Ref.get(closed))
            return yield* new ExecutionError({
              reason: new ExecutionUnknown({ message: 'Execution owner is closed' }),
            })
          const handle = yield* spawner.spawn(instruction).pipe(
            Effect.mapError(
              (error) =>
                new ExecutionError({
                  reason: new ExecutionSpawnError({ message: error.message, cause: error }),
                }),
            ),
          )
          yield* Ref.update(active, HashSet.add(handle))
          yield* Effect.addFinalizer(() => Ref.update(active, HashSet.remove(handle)))
          return handle
        }),
      ),
    )
    const spillPath = yield* Ref.make<string | undefined>(undefined)
    let chunks: Array<Uint8Array> = []
    let bytes = 0
    let newlines = 0
    let lastByte = 0
    const decoders = { stdout: Decode.make(), stderr: Decode.make() }
    const spillError = (error: { readonly message: string }) =>
      new ExecutionError({
        reason: new ExecutionUnknown({
          message: error.message,
          ...(Ref.getUnsafe(spillPath) === undefined
            ? {}
            : { spillPath: Ref.getUnsafe(spillPath) }),
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
                    ...(Ref.getUnsafe(spillPath) === undefined
                      ? {}
                      : { spillPath: Ref.getUnsafe(spillPath) }),
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
      if ((yield* Ref.get(spillPath)) === undefined && options.spill !== undefined) {
        chunks.push(chunk.bytes.slice())
        const lines = bytes === 0 ? 0 : newlines + (lastByte === 10 ? 0 : 1)
        if (
          options.spill !== undefined &&
          (bytes > options.spill.afterBytes || lines > options.spill.afterLines)
        ) {
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              const published = yield* fs
                .makeTempFile({ prefix: 'effect-harness-output-', suffix: '.log' })
                .pipe(Effect.mapError(spillError))
              yield* Ref.set(spillPath, published)
              const prefix = new Uint8Array(bytes)
              let offset = 0
              for (const value of chunks) {
                prefix.set(value, offset)
                offset += value.length
              }
              chunks = []
              yield* Effect.uninterruptible(
                fs.writeFile(published, prefix).pipe(Effect.mapError(spillError)),
              )
              if (options.onSpill !== undefined) {
                yield* callback(() => options.onSpill?.(published) ?? Effect.void)
              }
            }),
          )
        }
      } else {
        const destination = yield* Ref.get(spillPath)
        if (destination !== undefined)
          yield* Effect.uninterruptible(
            fs.writeFile(destination, chunk.bytes, { flag: 'a' }).pipe(Effect.mapError(spillError)),
          )
      }
      const text = Decode.decodeUnsafe(decoders[chunk.stream], chunk.bytes)
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
      const readerState = yield* Ref.make<{
        readonly waiting:
          | Fiber.Fiber<
              Option.Option<
                ReadonlyArray<{ readonly bytes: Uint8Array; readonly stream: 'stdout' | 'stderr' }>
              >,
              ExecutionError
            >
          | undefined
        readonly idleClosed: boolean
        readonly lastAt: DateTime.Utc
      }>({ waiting: undefined, idleClosed: false, lastAt: yield* DateTime.now })
      const reading = yield* Effect.gen(function* () {
        while (!(yield* Ref.get(readerState)).idleClosed) {
          const lastAt = yield* DateTime.now
          const waiting = yield* pull.pipe(
            Effect.asSome,
            Pull.catchDone(() => Effect.succeedNone),
            Effect.forkChild,
          )
          yield* Ref.update(readerState, (value) => ({ ...value, waiting, lastAt }))
          const batch = yield* Fiber.join(waiting)
          yield* Ref.update(readerState, (value) => ({ ...value, waiting: undefined }))
          if (Option.isNone(batch)) return
          // Every pulled chunk is admitted, including the rest of this batch.
          for (const chunk of batch.value) yield* processChunk(chunk)
        }
      }).pipe(
        Effect.catchCauseIf(
          (cause) => Ref.getUnsafe(readerState).idleClosed && Cause.hasInterrupts(cause),
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
        const joined = yield* Fiber.join(reading).pipe(Effect.timeoutOption('100 millis'))
        if (Option.isSome(joined)) break
        const now = yield* DateTime.now
        const current = yield* Ref.get(readerState)
        if (
          current.waiting !== undefined &&
          Duration.toMillis(DateTime.distance(current.lastAt, now)) >= 100
        ) {
          yield* Ref.update(readerState, (value) => ({ ...value, idleClosed: true }))
          // Interrupt only the pending pull, never the admitted consumer.
          yield* Fiber.interrupt(current.waiting)
          yield* Fiber.join(reading)
          break
        }
      }
      for (const stream of ['stdout', 'stderr'] as const) {
        const text = Decode.decodeUnsafe(decoders[stream])
        if (text !== '' && options.onOutput !== undefined)
          yield* callback(() => options.onOutput?.(text, { stream }) ?? Effect.void)
      }
      return {
        exitCode: Number(status),
        ...(Ref.getUnsafe(spillPath) === undefined ? {} : { spillPath: Ref.getUnsafe(spillPath) }),
      }
    })
    const finished =
      timeout === undefined
        ? run
        : run.pipe(
            Effect.timeoutOrElse({
              duration: timeout,
              orElse: () =>
                Effect.fail(
                  new ExecutionError({
                    reason: new ExecutionTimeout({
                      message: `Command timed out after ${Duration.toSeconds(timeout)} seconds`,
                      ...(Ref.getUnsafe(spillPath) === undefined
                        ? {}
                        : { spillPath: Ref.getUnsafe(spillPath) }),
                    }),
                  }),
                ),
            }),
          )
    return yield* finished
  }, Effect.scoped)
  const cleanup = Effect.gen(function* () {
    yield* Ref.set(closed, true)
    const handles = yield* admission.withPermit(Ref.get(active))
    // P5-explicit-concurrency-option: every child must receive termination promptly;
    // serial waves would add a forceKillAfter wait per wave before later children are signaled.
    yield* Effect.forEach(
      handles,
      (handle) => handle.kill({ forceKillAfter: '1 second' }).pipe(Effect.ignore),
      { concurrency: 'unbounded', discard: true },
    )
  }).pipe(Effect.withSpan('Env.cleanup'))
  yield* Effect.addFinalizer(constant(cleanup))
  return { exec }
})
