import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
// Source imports permit focused validation before the integration owner builds.
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ToolkitProcess from 'effect-harness/ToolkitProcess'

class Credentials extends Context.Service<Credentials, string>()('typetest/bindings/Credentials') {}
class Audit extends Context.Service<Audit, string>()('typetest/bindings/Audit') {}
class AcquisitionError extends Schema.TaggedError<AcquisitionError>()('AcquisitionError', {}) {}
declare const backend: Layer.Layer<
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner,
  AcquisitionError,
  Credentials
>
declare const fsBackend: Layer.Layer<
  FileSystem.FileSystem | Path.Path,
  AcquisitionError,
  Credentials
>
declare const audit: Layer.Layer<Audit>

const options = { root: '/workspace' } as const
const fileLayer = ToolkitFileSystem.layer(options)
const processLayer = ToolkitProcess.layer(options)
const binding = Layer.merge(fileLayer, processLayer).pipe(Layer.provide(backend))

test('native tags retain their backend requirements before provision', () => {
  expect(fileLayer).type.toBe<
    Layer.Layer<
      FileSystem.FileSystem | Path.Path,
      PlatformError.PlatformError,
      FileSystem.FileSystem | Path.Path
    >
  >()
  expect(processLayer).type.toBe<
    Layer.Layer<
      ChildProcessSpawner.ChildProcessSpawner,
      PlatformError.PlatformError,
      ChildProcessSpawner.ChildProcessSpawner | Path.Path
    >
  >()
  expect(binding).type.toBe<
    Layer.Layer<
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner,
      PlatformError.PlatformError | AcquisitionError,
      Credentials
    >
  >()
  expect(ChildProcess.make('program', ['literal'])).type.toBe<ChildProcess.StandardCommand>()
  expect(Effect.scoped(ChildProcess.make('program'))).type.toBe<
    Effect.Effect<
      ChildProcessSpawner.ChildProcessHandle,
      PlatformError.PlatformError,
      ChildProcessSpawner.ChildProcessSpawner
    >
  >()
})

const tool = Tool.make('bound_read', { success: Schema.String, failure: Schema.String })
  .addDependency(FileSystem.FileSystem)
  .addDependency(Path.Path)
  .addDependency(ChildProcessSpawner.ChildProcessSpawner)
const toolkit = Toolkit.make(tool)
const handlers = toolkit.toLayer(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const label = yield* Audit
    return {
      bound_read: () =>
        Effect.gen(function* () {
          const text = yield* fs.readFileString(path.resolve('file.txt'))
          yield* spawner.string(ChildProcess.make('program', [text]))
          return label
        }).pipe(Effect.mapError(String)),
    }
  }),
)

test('toolkit Layers remove only provided services and preserve unrelated requirements', () => {
  expect(handlers).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bound_read'>,
      never,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner | Audit
    >
  >()
  const filesOnly = fileLayer.pipe(Layer.provide(fsBackend))
  expect(handlers.pipe(Layer.provide(filesOnly))).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bound_read'>,
      PlatformError.PlatformError | AcquisitionError,
      ChildProcessSpawner.ChildProcessSpawner | Audit | Credentials
    >
  >()
  expect(handlers.pipe(Layer.provide(binding))).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bound_read'>,
      PlatformError.PlatformError | AcquisitionError,
      Audit | Credentials
    >
  >()
  expect(handlers.pipe(Layer.provide(binding), Layer.provide(audit))).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'bound_read'>,
      PlatformError.PlatformError | AcquisitionError,
      Credentials
    >
  >()
  // Supplying an unrelated service cannot make this toolkit runnable.
  expect(handlers.pipe(Layer.provide(audit))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'bound_read'>>
  >()
  expect(ToolkitProcess.layer).type.not.toBeCallableWith({ root: 42 })
  expect(Effect.provide(toolkit, handlers)).type.toBe<
    Effect.Effect<
      Toolkit.WithHandler<typeof toolkit.tools>,
      never,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner | Audit
    >
  >()
})

test('native process handles still require invocation scope after binding', () => {
  expect(ChildProcess.make('program').pipe(Effect.provide(binding))).type.toBe<
    Effect.Effect<
      ChildProcessSpawner.ChildProcessHandle,
      PlatformError.PlatformError | AcquisitionError,
      Scope.Scope | Credentials
    >
  >()
})
