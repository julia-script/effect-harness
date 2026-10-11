import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as FileReader from 'effect-harness/FileReader'
import * as Images from 'effect-harness/ImagePreparation'
import type { ImagePreparationError } from 'effect-harness/ImagePreparationError'
import type { ToolExecution } from 'effect-harness/ToolExecution'
import type * as ToolResult from 'effect-harness/ToolResult'
import type * as Toolkit from 'effect-harness/Toolkit'
import type { CodingFileError } from 'effect-harness/coding/FileError'
import type { ImageReadError } from 'effect-harness/coding/ImageRead'
import * as Policy from 'effect-harness/coding/ImageReadPolicy'
import * as Read from 'effect-harness/coding/Read'
import * as NodeImages from 'effect-harness/images-node'
import * as WasmImages from 'effect-harness/images-wasm'

class Credentials extends Context.Service<Credentials, string>()('CodingImages/Credentials') {}
class BackendError extends Schema.TaggedError<BackendError>()('CodingImagesBackendError', {}) {}
declare const backend: Layer.Layer<FileSystem.FileSystem | Path.Path, BackendError, Credentials>
const tools = Read.withImages({ maxSourceBytes: 1024 * 1024 })
const limits: Images.Limits = {
  maxWidth: 100,
  maxHeight: 100,
  maxBytes: 10000,
  maxInputPixels: 100000,
}
const policy = Policy.fromCapabilities({ images: true }, limits)

test('text Read keeps exact portable requirements', () => {
  expect(Read.layer()).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'read'>,
      never,
      FileReader.FileReader | FileSystem.FileSystem | Path.Path
    >
  >()
  expect(Read.read({ path: 'text' })).type.toBeAssignableTo<
    Effect.Effect<
      ToolResult.Output<Read.Outcome>,
      CodingFileError,
      FileReader.FileReader | FileSystem.FileSystem | Path.Path | Scope.Scope
    >
  >()
})

test('image Read requires explicit file, codec and invocation policy services', () => {
  expect(tools.layer).type.toBe<
    Layer.Layer<
      Toolkit.Handler<'read'>,
      never,
      | FileReader.FileReader
      | FileSystem.FileSystem
      | Path.Path
      | Images.ImagePreparation
      | Policy.ImageReadPolicy
    >
  >()
  expect(
    Read.readWithImages({ path: 'image.png' }, policy, { maxSourceBytes: 10000 }),
  ).type.toBeAssignableTo<
    Effect.Effect<
      ToolResult.Output<Read.ImageOutcome>,
      CodingFileError | ImageReadError | ImagePreparationError,
      | FileReader.FileReader
      | FileSystem.FileSystem
      | Path.Path
      | Images.ImagePreparation
      | Scope.Scope
    >
  >()
  expect(Effect.flatMap(Policy.ImageReadPolicy, (resolver) => resolver.current)).type.toBe<
    Effect.Effect<Policy.Policy, never, Policy.ImageReadPolicy | ToolExecution>
  >()
  expect(Read.withImages).type.not.toBeCallableWith({})
  expect(Read.readWithImages).type.not.toBeCallableWith(
    { path: 'image.png' },
    { vision: true },
    { maxSourceBytes: 100 },
  )
  expect(tools.layer.pipe(Layer.provide(backend))).type.not.toBeAssignableTo<
    Layer.Layer<Toolkit.Handler<'read'>, BackendError, Credentials>
  >()
  expect(
    tools.layer.pipe(
      Layer.provide(
        Images.layer({
          capabilities: WasmImages.capabilities,
          decode: () => Effect.die('type-only'),
        }),
      ),
    ),
  ).type.not.toBeAssignableTo<Layer.Layer<Toolkit.Handler<'read'>>>()
})

test('provided Layers retain unrelated failures and unsupplied backend credentials', () => {
  const files = FileReader.layer.pipe(Layer.provideMerge(backend))
  const services = Layer.mergeAll(files, NodeImages.layer, Policy.layer(policy))
  const ready = Effect.provide(tools.toolkit, tools.layer.pipe(Layer.provideMerge(services)))
  expect(ready).type.toBeAssignableTo<
    Effect.Effect<
      Toolkit.WithHandler<typeof tools.toolkit.tools>,
      BackendError | ImagePreparationError,
      Credentials
    >
  >()
  expect(ready).type.not.toBeAssignableTo<
    Effect.Effect<Toolkit.WithHandler<typeof tools.toolkit.tools>>
  >()
  expect(NodeImages.layerWithPhoton).type.toBeAssignableTo<
    Layer.Layer<Images.ImagePreparation, ImagePreparationError, WasmImages.Photon>
  >()
})
