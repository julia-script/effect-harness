import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as FileReader from 'effect-harness/FileReader'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import type * as ToolResult from 'effect-harness/ToolResult'
import type { CodingFileError } from 'effect-harness/coding/FileError'
import * as MutationCoordinator from 'effect-harness/coding/MutationCoordinator'
import * as Read from 'effect-harness/coding/Read'
import * as Write from 'effect-harness/coding/Write'

class Credentials extends Context.Service<Credentials, string>()(
  'typetest/CodingFiles/Credentials',
) {}
class BackendError extends Schema.TaggedError<BackendError>()('CodingBackendError', {}) {}
declare const backend: Layer.Layer<FileSystem.FileSystem | Path.Path, BackendError, Credentials>
declare const reader: FileReader.Reader
declare const coordinator: MutationCoordinator.MutationCoordinator['Service']

test('positional reader and direct operations expose scope, semantic errors and native services', () => {
  expect(FileReader.layer).type.toBe<
    Layer.Layer<FileReader.FileReader, never, FileSystem.FileSystem>
  >()
  expect(Effect.flatMap(FileReader.FileReader, (service) => service.open('file'))).type.toBe<
    Effect.Effect<
      FileReader.Reader,
      FileReader.FileReaderError,
      FileReader.FileReader | Scope.Scope
    >
  >()
  expect(reader.readAt(0n, 64)).type.toBe<Effect.Effect<Uint8Array, FileReader.FileReaderError>>()
  expect(reader.readAt).type.not.toBeCallableWith(0, 64)
  expect(Read.read({ path: 'file' })).type.toBeAssignableTo<
    Effect.Effect<
      ToolResult.Output<Read.Outcome>,
      CodingFileError,
      FileReader.FileReader | FileSystem.FileSystem | Path.Path | Scope.Scope
    >
  >()
  expect(Write.write({ path: 'file', content: 'literal' })).type.toBeAssignableTo<
    Effect.Effect<
      ToolResult.Output<Write.Outcome>,
      CodingFileError,
      FileSystem.FileSystem | Path.Path | MutationCoordinator.MutationCoordinator
    >
  >()
  expect(Read.read).type.not.toBeCallableWith({ path: 1 })
  expect(Write.write).type.not.toBeCallableWith({ path: 'file', content: 1 })
})

test('ready-made handler Layers retain native requirements and unrelated backend errors', () => {
  expect(Read.layer()).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'read'>,
      never,
      FileReader.FileReader | FileSystem.FileSystem | Path.Path
    >
  >()
  expect(Write.layer).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'write'>,
      never,
      FileSystem.FileSystem | Path.Path | MutationCoordinator.MutationCoordinator
    >
  >()
  const files = ToolkitFileSystem.layer({ root: '/workspace' }).pipe(Layer.provide(backend))
  const services = Layer.merge(FileReader.layer, MutationCoordinator.layer).pipe(
    Layer.provideMerge(files),
  )
  const handlers = Layer.merge(Read.layer(), Write.layer).pipe(Layer.provideMerge(services))
  const tools = Toolkit.merge(Read.toolkit, Write.toolkit)
  expect(Effect.provide(tools, handlers)).type.toBe<
    Effect.Effect<
      Toolkit.WithHandler<typeof tools.tools>,
      PlatformError.PlatformError | BackendError,
      Credentials
    >
  >()
  expect(Read.layer().pipe(Layer.provide(FileReader.layer))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'read'>>
  >()
  expect(Write.layer.pipe(Layer.provide(FileReader.layer))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'write'>>
  >()
  expect(
    coordinator.withPath('file', () =>
      Effect.flatMap(Credentials, () => Effect.fail(new BackendError())),
    ),
  ).type.toBe<Effect.Effect<never, BackendError | CodingFileError, Credentials>>()
})
