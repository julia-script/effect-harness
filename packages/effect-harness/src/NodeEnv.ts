/**
 * Node environment construction with injected platform services and decoded host configuration.
 */
import { makeShellResolver, makeWatchMode } from './internal/nodeEnv.ts'
import * as Effect from 'effect/Effect'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import type * as FileSystem from 'effect/FileSystem'
import type * as Path from 'effect/Path'
import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  type Env,
  make as makeEnvironment,
  layer as environmentLayer,
  type WatchTarget,
} from './Env.ts'
import * as NodeNativeFiles from './NodeNativeFiles.ts'
/**
 * Platform, cwd, home and search-path conventions supplied by the host.
 *
 * @category models
 */
export interface Host {
  readonly platform: string
  readonly cwd: string
  readonly home: string
  readonly searchPathDelimiter: string
}
/**
 * Node environment configuration with optional host overrides.
 *
 * @category models
 */
type OptionalEnvOptions = { readonly [K in keyof Env.Options]?: Env.Options[K] | undefined }
export interface Options extends OptionalEnvOptions {
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
 */
export const hostDefaults: Effect.Effect<Host> = Effect.sync(() => ({
  platform: process.platform,
  cwd: process.cwd(),
  home: os.homedir(),
  searchPathDelimiter: path.delimiter,
}))
/**
 * Selects native or polling watching for the supplied targets.
 *
 * **Details**
 *
 * Samples Node host defaults and the native statfs policy. Parent-directory traversal requires the supplied Path service.
 *
 * @category combinators
 */
export const resolveWatchMode = Effect.fnUntraced(function* (
  targets: ReadonlyArray<WatchTarget>,
): Effect.fn.Return<'native' | 'polling', never, Path.Path> {
  const host = yield* hostDefaults
  const watchMode = yield* makeWatchMode(host.platform)
  return yield* watchMode(targets)
})
/**
 * Provides Env with Node-native file capabilities.
 *
 * **Details**
 *
 * Consumes FileSystem, Path and ChildProcessSpawner from the caller. Host defaults are
 * obtained when the Layer is built; options can override them.
 *
 * **Gotchas**
 *
 * Provide a shared MutationLocks manager when binding mutation tools. Environment
 * construction does not enforce a filesystem access sandbox.
 *
 * @category layers
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
      const host = options.host ?? (yield* hostDefaults)
      const resolveShell = yield* makeShellResolver(host)
      const watchMode = yield* makeWatchMode(host.platform)
      return environmentLayer({
        id: options.id ?? 'node:local',
        cwd: options.cwd ?? host.cwd,
        home: options.home ?? host.home,
        resolveShell: options.resolveShell ?? resolveShell(options.shell),
        resolveWatchMode: options.resolveWatchMode ?? watchMode,
        ...(options.watch === undefined ? {} : { watch: options.watch }),
        ...(options.env === undefined ? {} : { env: options.env }),
      })
    }),
  ).pipe(Layer.provide(NodeNativeFiles.layer))
/**
 * Resolves Node environment options through the active ConfigProvider.
 *
 * **Details**
 *
 * Shell discovery uses configured search paths and supplied FileSystem/Path services. Build
 * this Layer in the desired ConfigProvider context.
 *
 * @category layers
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
 */
export const make = Effect.fnUntraced(function* (
  options: Options = {},
): Effect.fn.Return<
  Env['Service'],
  never,
  | FileSystem.FileSystem
  | Path.Path
  | ChildProcessSpawner.ChildProcessSpawner
  | import('effect/Scope').Scope
> {
  const host = options.host ?? (yield* hostDefaults)
  const resolveShell = yield* makeShellResolver(host)
  const watchMode = yield* makeWatchMode(host.platform)
  return yield* makeEnvironment({
    id: options.id ?? 'node:local',
    cwd: options.cwd ?? host.cwd,
    home: options.home ?? host.home,
    resolveShell: options.resolveShell ?? resolveShell(options.shell),
    resolveWatchMode: options.resolveWatchMode ?? watchMode,
    ...(options.watch === undefined ? {} : { watch: options.watch }),
    ...(options.env === undefined ? {} : { env: options.env }),
  }).pipe(Effect.provide(NodeNativeFiles.layer))
})
