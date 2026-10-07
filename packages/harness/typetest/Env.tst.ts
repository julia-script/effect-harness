import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import type * as Path from 'effect/Path'
import type * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type * as Config from 'effect/Config'
import type * as PlatformError from 'effect/PlatformError'
import type * as Option from 'effect/Option'
import type { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import * as Context from 'effect/Context'
import * as Env from '@effect-harness/harness/Env'
import * as NodeEnv from '@effect-harness/harness/env/Node'
import * as Exec from '@effect-harness/harness/env/Exec'
import * as Watch from '@effect-harness/harness/env/Watch'
import * as Conformance from '@effect-harness/harness/testing/EnvConformance'
import * as Runner from '@effect-harness/harness/testing/Runner'

class AdapterSettings extends Context.Service<AdapterSettings, { readonly refuse: boolean }>()(
  'typetest/AdapterSettings',
) {}
declare const fs: FileSystem.FileSystem
declare const path: Path.Path
declare const spawner: ChildProcessSpawner['Service']
declare const native: Env.NativeFiles['Service']
declare const reader: Env.TextLineReader
declare const adapter: Layer.Layer<Env.Env, Env.FileError, AdapterSettings>
declare const runner: Runner.Runner<Env.FileError, AdapterSettings>

test('construction keeps platform and scope requirements and reader EOF is Option', () => {
  expect(Env.make({ id: 'test', cwd: '/' })).type.toBe<
    Effect.Effect<
      Env.Env['Service'],
      never,
      FileSystem.FileSystem | Path.Path | Env.NativeFiles | ChildProcessSpawner | Scope.Scope
    >
  >()
  expect(Env.layer({ id: 'test', cwd: '/' })).type.toBe<
    Layer.Layer<
      Env.Env,
      never,
      FileSystem.FileSystem | Path.Path | Env.NativeFiles | ChildProcessSpawner
    >
  >()
  expect(NodeEnv.layer()).type.toBe<
    Layer.Layer<Env.Env, never, FileSystem.FileSystem | Path.Path | ChildProcessSpawner>
  >()
  expect(NodeEnv.layerConfig({})).type.toBe<
    Layer.Layer<
      Env.Env,
      Config.ConfigError,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner
    >
  >()
  expect(Exec.make(fs, path, spawner, { id: 'test', cwd: '/' })).type.toBe<
    Effect.Effect<Pick<Env.Env['Service'], 'exec'>, never, Scope.Scope>
  >()
  expect(Watch.make(fs, path, native, [])).type.toBe<
    Effect.Effect<Env.Watcher, Env.FileError, Scope.Scope>
  >()
  expect(reader.readLine).type.toBe<Effect.Effect<Option.Option<Env.TextLine>, Env.FileError>>()
  expect(Env.make).type.not.toBeCallableWith({ cwd: '/' })
  expect(Watch.make).type.not.toBeCallableWith(fs, path, native, [{ recursive: true }])
})

test('conformance initialization retains exact errors and adapter service', () => {
  const program = Conformance.withEnv(Effect.andThen(Env.Env, Effect.void), adapter)
  expect(program).type.toBe<Effect.Effect<void, Env.FileError, AdapterSettings>>()
  expect(program).type.not.toBeAssignableTo<Effect.Effect<void, Env.FileError>>()
  expect(Conformance.freshLayer(() => adapter)).type.toBe<
    Layer.Layer<
      Env.Env,
      Env.FileError | PlatformError.PlatformError,
      AdapterSettings | FileSystem.FileSystem
    >
  >()
  expect(
    Runner.registerEnvConformance(runner, 'adapter', adapter, {
      assertions: { strictEqual: () => {}, deepStrictEqual: () => {}, ok: () => {} },
    }),
  ).type.toBe<void>()
})
