/**
 * Node environment construction with injected platform services and decoded host configuration.
 *
 * @since 0.0.0
 */
import { resolveShell, watchMode } from './internal/nodeEnv.ts'
import * as Effect from 'effect/Effect'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Os from 'node:os'
import * as NodePath from 'node:path'
// effect-review-allow P9-namespace-alias-equals-module: node:path and effect/Path both bind Path; NodePath preserves the checked imported-name collision.
import {
  Env,
  make as makeEnvironment,
  layer as environmentLayer,
  type WatchTarget,
  type Options as EnvOptions,
} from './Env.ts'
import { layerNative } from './NodeNativeFiles.ts'
/**
 * NodeEnv host contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Host {
  readonly platform: string
  readonly cwd: string
  readonly home: string
  readonly searchPathDelimiter: string
}
/**
 * NodeEnv options contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Options extends Partial<EnvOptions> {
  readonly host?: Host | undefined
}
/**
 * Narrow Node host adapter, evaluated at Layer construction rather than factory declaration.
 *
 * **Details**
 *
 * Shell environment is supplied by ConfigProvider; this adapter reads only runtime identity,
 * cwd/home and the host search-path delimiter absent from stable Effect Path.
 *
 * @category combinators
 * @since 0.0.0
 */
export const hostDefaults: Effect.Effect<Host> = Effect.sync(() => ({
  platform: process.platform,
  cwd: process.cwd(),
  home: Os.homedir(),
  searchPathDelimiter: NodePath.delimiter,
}))
/**
 * Native statfs policy accessor; normal parent traversal requires the supplied Path service.
 *
 * @category combinators
 * @since 0.0.0
 */
export const resolveWatchMode = Effect.fnUntraced(function* (
  targets: ReadonlyArray<WatchTarget>,
): Effect.fn.Return<'native' | 'polling', never, Path.Path> {
  const path = yield* Path.Path
  const host = yield* hostDefaults
  return yield* watchMode(path, host.platform, targets)
})
/**
 * Constructs Env with caller-supplied FileSystem, Path and ChildProcessSpawner.
 *
 * **Details**
 *
 * Only the narrow NativeFiles adapter is provided here; platform services belong at the application edge.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  options: Options = {},
): Layer.Layer<
  Env,
  never,
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const host = options.host ?? (yield* hostDefaults)
      return environmentLayer({
        id: options.id ?? 'node:local',
        cwd: options.cwd ?? host.cwd,
        home: options.home ?? host.home,
        resolveShell: options.resolveShell ?? resolveShell(fs, path, host, options.shell),
        resolveWatchMode:
          options.resolveWatchMode ?? ((targets) => watchMode(path, host.platform, targets)),
        ...(options.watch === undefined ? {} : { watch: options.watch }),
        ...(options.env === undefined ? {} : { env: options.env }),
      })
    }),
  ).pipe(Layer.provide(layerNative))
/**
 * Layer for NodeEnv config capabilities.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerConfig = (
  config: Config.Wrap<Options>,
): Layer.Layer<
  Env,
  Config.ConfigError,
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
> => Layer.unwrap(Config.unwrap(config).pipe(Effect.map(layer)))

/**
 * Acquires Env using the supplied host defaults and injected platform services.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = (
  options: Options = {},
): Effect.Effect<
  Env['Service'],
  never,
  | FileSystem.FileSystem
  | Path.Path
  | ChildProcessSpawner.ChildProcessSpawner
  | import('effect/Scope').Scope
> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const host = options.host ?? (yield* hostDefaults)
    return yield* makeEnvironment({
      id: options.id ?? 'node:local',
      cwd: options.cwd ?? host.cwd,
      home: options.home ?? host.home,
      resolveShell: options.resolveShell ?? resolveShell(fs, path, host, options.shell),
      resolveWatchMode:
        options.resolveWatchMode ?? ((targets) => watchMode(path, host.platform, targets)),
      ...(options.watch === undefined ? {} : { watch: options.watch }),
      ...(options.env === undefined ? {} : { env: options.env }),
    }).pipe(Effect.provide(layerNative))
  })
