/**
 * Toolkit-local working-directory defaults for a native child process backend.
 * Canonical registry invocation selection remains a later integration dependency.
 */
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import type { Options as FileSystemOptions } from './ToolkitFileSystem.ts'

/** Defaults copied when constructing the Layer, before backend acquisition. */
export interface Options extends FileSystemOptions {
  /** Default environment; a caller's explicit env replaces this entire map. */
  readonly env?: Readonly<Record<string, string | undefined>> | undefined
  /** Extend an explicit env with backend defaults; defaults to false. */
  readonly extendEnv?: boolean | undefined
}

/**
 * Bind a supplied spawner to an absolute root in its own backend namespace.
 *
 * Compose with ToolkitFileSystem.layer using the same root, policy and coherent
 * backend. The wrapper preserves command argv, stdin/stdout/stderr, pipelines,
 * explicit shell selection, kill options and scoped handles. It never introduces
 * shell parsing, reads host environment variables or changes the host cwd.
 * A caller cwd overrides the root default and is resolved against the root;
 * explicit env and extendEnv override defaults independently. Native backend
 * errors and scoped process cleanup propagate without fallback.
 * within-root constrains cwd lexically, not executable paths or process effects.
 */
export const layer = (
  options: Options,
): Layer.Layer<
  ChildProcessSpawner.ChildProcessSpawner,
  PlatformError.PlatformError,
  ChildProcessSpawner.ChildProcessSpawner | Path.Path
> => {
  const { root, pathPolicy = 'unrestricted', extendEnv = false } = options
  const env = options.env === undefined ? undefined : { ...options.env }
  return Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.gen(function* () {
      const backend = yield* ChildProcessSpawner.ChildProcessSpawner
      const path = yield* Path.Path
      if (!path.isAbsolute(root)) {
        return yield* PlatformError.badArgument({
          module: 'ToolkitProcess',
          method: 'layer',
          description: 'root must be absolute',
        })
      }
      const boundRoot = path.resolve(root)
      const prepare = (
        command: ChildProcess.Command,
      ): Effect.Effect<ChildProcess.Command, PlatformError.PlatformError> =>
        Effect.suspend((): Effect.Effect<ChildProcess.Command, PlatformError.PlatformError> => {
          if (command._tag === 'PipedCommand') {
            return Effect.all([prepare(command.left), prepare(command.right)]).pipe(
              Effect.map(([left, right]) => ChildProcess.pipeTo(left, right, command.options)),
            )
          }
          const cwd = path.resolve(boundRoot, command.options.cwd ?? '.')
          const relative = path.relative(boundRoot, cwd)
          if (
            pathPolicy === 'within-root' &&
            (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
          ) {
            return Effect.fail(
              PlatformError.systemError({
                _tag: 'PermissionDenied',
                module: 'ToolkitProcess',
                method: 'spawn',
                pathOrDescriptor: cwd,
                description: 'cwd is outside the bound root',
              }),
            )
          }
          return Effect.succeed(
            ChildProcess.make(command.command, command.args, {
              ...command.options,
              cwd,
              env: command.options.env ?? (env === undefined ? undefined : { ...env }),
              extendEnv: command.options.extendEnv ?? extendEnv,
            }),
          )
        })
      return ChildProcessSpawner.make((command) =>
        Effect.flatMap(prepare(command), (prepared) => backend.spawn(prepared)),
      )
    }),
  )
}
