import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Registry from 'effect-harness/Registry'
import type * as RegistryScope from 'effect-harness/RegistryScope'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'

// After integration build: node apps/example/dist/tour/ScopedRegistry.js
// This is a usable standalone registry. Harness adoption and execution wrappers are later consumers.
const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const read = Tool.make('read', { success: Schema.String, failure: Schema.String })
  .addDependency(FileSystem.FileSystem)
  .addDependency(Path.Path)
const toolkit = Toolkit.make(read)
const handlers = toolkit.toLayer({
  read: () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      return yield* fs.readFileString('message.txt').pipe(Effect.mapError(String))
    }),
})
const registration = (root: string, version: number) =>
  Registry.provide(
    Registry.toolkit({ id: 'files', version }, toolkit),
    handlers.pipe(Layer.provideMerge(ToolkitFileSystem.layer({ root }).pipe(Layer.provide(files)))),
  )

// A consumer can inspect the exact binding and its captured services under its lease.
// Invocation consumers use lease.invokeTool and supply their fresh ToolExecution service.
const readBinding = (item: RegistryScope.BoundTool) =>
  Context.get(item.context, FileSystem.FileSystem).readFileString('message.txt')

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const directory = yield* fs.makeTempDirectoryScoped()
      const root = yield* fs.realPath(directory)
      const first = path.join(root, 'first')
      const second = path.join(root, 'second')
      yield* fs.makeDirectory(first)
      yield* fs.makeDirectory(second)
      yield* fs.writeFileString(path.join(first, 'message.txt'), 'old root')
      yield* fs.writeFileString(path.join(second, 'message.txt'), 'new root')
      const registry = yield* Registry.Registry
      yield* registry.install(registration(first, 1))
      yield* Effect.scoped(
        Effect.gen(function* () {
          const previous = yield* registry.acquire
          const oldBinding = yield* previous.resolveTool('read', { binding: 'files', version: 1 })
          yield* registry.install(registration(second, 2))
          yield* Effect.scoped(
            Effect.gen(function* () {
              const current = yield* registry.acquire
              const newBinding = yield* current.resolveTool('read', { version: 2 })
              yield* Effect.log(yield* readBinding(newBinding))
            }),
          )
          yield* registry.uninstall('files')
          // Reload and uninstall did not invalidate the leased old binding.
          yield* Effect.log(yield* readBinding(oldBinding))
        }),
      )
      yield* Effect.log(
        `Installed registrations: ${(yield* registry.snapshot).registrations.length}`,
      )
    }),
  ).pipe(Effect.provide([files, Registry.layer])),
)
