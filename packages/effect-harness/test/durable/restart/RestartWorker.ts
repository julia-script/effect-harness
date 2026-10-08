import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type * as PlatformError from 'effect/PlatformError'
import type * as Option from 'effect/Option'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as Spawner from 'effect/process/ChildProcessSpawner'
import * as Stream from 'effect/Stream'

/** Each test owns its directory and native handles; SIGKILL remains a deliberate fault injection. */
export class RestartWorker extends Context.Service<
  RestartWorker,
  {
    readonly filename: string
    readonly spawn: (
      phase: string,
    ) => Effect.Effect<Spawner.ChildProcessHandle, PlatformError.PlatformError, Scope.Scope>
    readonly marker: (
      handle: Spawner.ChildProcessHandle,
      prefix: string,
    ) => Effect.Effect<
      Option.Option<string>,
      PlatformError.PlatformError | import('effect/Cause').TimeoutError
    >
  }
>()('test/RestartWorker') {
  static layer(options: {
    readonly fixture: string
    readonly databaseEnv: string
    readonly phaseEnv: string
    readonly extraEnv?: Readonly<Record<string, string>> | undefined
  }): Layer.Layer<
    RestartWorker,
    PlatformError.PlatformError,
    FileSystem.FileSystem | Path.Path | Spawner.ChildProcessSpawner
  > {
    return Layer.effect(
      RestartWorker,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const spawner = yield* Spawner.ChildProcessSpawner
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'durable-restart-' })
        const filename = path.join(directory, 'workflow.sqlite')
        return RestartWorker.of({
          filename,
          spawn: (phase) =>
            spawner.spawn(
              ChildProcess.make('bun', [options.fixture], {
                env: {
                  ...options.extraEnv,
                  [options.databaseEnv]: filename,
                  [options.phaseEnv]: phase,
                },
                extendEnv: true,
                stdin: 'ignore',
                stderr: 'inherit',
                forceKillAfter: 1000,
              }),
            ),
          marker: (handle, prefix) =>
            handle.stdout.pipe(
              Stream.decodeText,
              Stream.splitLines,
              Stream.filter((line) => line.startsWith(prefix)),
              Stream.runHead,
              Effect.timeout('15 seconds'),
            ),
        })
      }),
    )
  }
}
