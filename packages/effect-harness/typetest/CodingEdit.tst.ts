import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import type * as ToolResult from 'effect-harness/ToolResult'
import * as Edit from 'effect-harness/coding/Edit'
import type { CodingFileError } from 'effect-harness/coding/FileError'
import * as MutationCoordinator from 'effect-harness/coding/MutationCoordinator'
import * as Write from 'effect-harness/coding/Write'

class Credentials extends Context.Service<Credentials, string>()(
  'typetest/CodingEdit/Credentials',
) {}
class BackendError extends Schema.TaggedError<BackendError>()('CodingEditBackendError', {}) {}
declare const backend: Layer.Layer<FileSystem.FileSystem | Path.Path, BackendError, Credentials>

test('direct editing exposes semantic failures and bound native service requirements', () => {
  expect(
    Edit.edit({ path: 'file', edits: [{ oldText: 'old', newText: 'new' }] }),
  ).type.toBeAssignableTo<
    Effect.Effect<
      ToolResult.Output<Edit.Outcome>,
      Edit.CodingEditError | CodingFileError,
      FileSystem.FileSystem | Path.Path | MutationCoordinator.MutationCoordinator
    >
  >()
  expect(Edit.edit).type.not.toBeCallableWith({ path: 'file', oldText: 'old', newText: 'new' })
  expect(Edit.edit).type.not.toBeCallableWith({
    path: 'file',
    edits: [{ oldText: 1, newText: 'new' }],
  })
  expect(Schema.decodeEffect(Edit.Parameters)({ path: 'file', edits: '[]' })).type.toBe<
    Effect.Effect<Edit.Parameters, Schema.SchemaError>
  >()
  expect(Edit.layer()).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'edit'>,
      never,
      FileSystem.FileSystem | Path.Path | MutationCoordinator.MutationCoordinator
    >
  >()
})

test('ready-made layers share the coordinator and preserve acquisition errors and backend requirements', () => {
  const files = ToolkitFileSystem.layer({ root: '/workspace' }).pipe(Layer.provide(backend))
  const services = MutationCoordinator.layer.pipe(Layer.provideMerge(files))
  const handlers = Layer.merge(Edit.layer({ writeMode: 'rename' }), Write.layer).pipe(
    Layer.provideMerge(services),
  )
  const tools = Toolkit.merge(Edit.toolkit, Write.toolkit)
  expect(Effect.provide(tools, handlers)).type.toBe<
    Effect.Effect<
      Toolkit.WithHandler<typeof tools.tools>,
      PlatformError.PlatformError | BackendError,
      Credentials
    >
  >()
  expect(Edit.layer().pipe(Layer.provide(files))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'edit'>>
  >()
  expect(Edit.layer().pipe(Layer.provide(MutationCoordinator.layer))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'edit'>>
  >()
})
