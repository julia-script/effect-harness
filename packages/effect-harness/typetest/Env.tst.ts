import type * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import type * as Path from 'effect/Path'
import type * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type * as Config from 'effect/Config'
import type * as PlatformError from 'effect/PlatformError'
import type * as Option from 'effect/Option'

import * as Context from 'effect/Context'
import * as Env from 'effect-harness/Env'
import * as NodeEnv from 'effect-harness/NodeEnv'
// effect-nit-allow P8-tests-import-public-specifiers: Exec is private construction wiring; this declaration proof deliberately targets its internal contract.
// effect-nit-allow P9-no-internal-cross-import: same-package proof of a deliberately null-exported seam; Exec is private construction wiring; this declaration proof deliberately targets its internal contract.
import * as exec from '../src/env/internal/exec.ts'
// effect-nit-allow P8-tests-import-public-specifiers: Watch is private construction wiring; this declaration proof deliberately targets its internal contract.
// effect-nit-allow P9-no-internal-cross-import: same-package exact private Watch constructor channels; the emitted package denies this owner and public adapter proofs remain separate.
import * as watch from '../src/env/internal/watch.ts'
import * as EnvConformance from 'effect-harness/testing/EnvConformance'
import * as Runner from 'effect-harness/testing/Runner'

class AdapterSettings extends Context.Service<AdapterSettings, { readonly refuse: boolean }>()(
  'typetest/AdapterSettings',
) {}
declare const fs: FileSystem.FileSystem
declare const path: Path.Path
declare const spawner: ChildProcessSpawner.ChildProcessSpawner['Service']
declare const native: NativeFiles.NativeFiles['Service']
declare const reader: Env.TextLineReader
declare const adapter: Layer.Layer<Env.Env, FileError.FileError, AdapterSettings>
declare const runner: Runner.Runner<FileError.FileError, AdapterSettings>

test('construction keeps platform and scope requirements and reader EOF is Option', () => {
  expect(Env.make({ id: 'test', cwd: '/' })).type.toBe<
    Effect.Effect<
      Env.Env['Service'],
      never,
      | FileSystem.FileSystem
      | Path.Path
      | NativeFiles.NativeFiles
      | ChildProcessSpawner.ChildProcessSpawner
      | Scope.Scope
    >
  >()
  expect(Env.layer({ id: 'test', cwd: '/' })).type.toBe<
    Layer.Layer<
      Env.Env,
      never,
      | FileSystem.FileSystem
      | Path.Path
      | NativeFiles.NativeFiles
      | ChildProcessSpawner.ChildProcessSpawner
    >
  >()
  expect(NodeEnv.layer()).type.toBe<
    Layer.Layer<
      Env.Env,
      never,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
    >
  >()
  expect(NodeEnv.layerConfig({})).type.toBe<
    Layer.Layer<
      Env.Env,
      Config.ConfigError,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
    >
  >()
  expect(
    exec.make({ fs: fs, path: path, spawner: spawner, defaults: { id: 'test', cwd: '/' } }),
  ).type.toBe<Effect.Effect<Pick<Env.Env['Service'], 'exec'>, never, Scope.Scope>>()
  expect(watch.make({ fs: fs, path: path, native: native, targets: [] })).type.toBe<
    Effect.Effect<Env.Watcher, FileError.FileError, Scope.Scope>
  >()
  expect(reader.readLine).type.toBe<
    Effect.Effect<Option.Option<Env.TextLine>, FileError.FileError>
  >()
  expect(Env.make).type.not.toBeCallableWith({ cwd: '/' })
  expect(watch.make).type.not.toBeCallableWith(fs, path, native, [{ recursive: true }])
})

test('conformance initialization retains exact errors and adapter service', () => {
  const program = EnvConformance.withEnv(Effect.andThen(Env.Env, Effect.void), adapter)
  expect(program).type.toBe<Effect.Effect<void, FileError.FileError, AdapterSettings>>()
  expect(EnvConformance.withEnv(adapter)(Effect.andThen(Env.Env, Effect.void))).type.toBe<
    Effect.Effect<void, FileError.FileError, AdapterSettings>
  >()
  expect(program).type.not.toBeAssignableTo<Effect.Effect<void, FileError.FileError>>()
  expect(EnvConformance.freshLayer(() => adapter)).type.toBe<
    Layer.Layer<
      Env.Env,
      FileError.FileError | PlatformError.PlatformError,
      AdapterSettings | FileSystem.FileSystem
    >
  >()
  expect(
    Runner.registerEnvConformance(runner, 'adapter', adapter, {
      assertions: { strictEqual: () => {}, deepStrictEqual: () => {}, ok: () => {} },
    }),
  ).type.toBe<void>()
})

import type * as NativeFiles from 'effect-harness/NativeFiles'
import type * as FileError from 'effect-harness/FileError'
