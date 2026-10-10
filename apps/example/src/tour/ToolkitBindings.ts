import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ToolkitProcess from 'effect-harness/ToolkitProcess'

// After integration build: node apps/example/dist/tour/ToolkitBindings.js
// Each selected toolkit must retain its own Layer context at invocation. Registry
// integration is separate; this example demonstrates native service isolation.
const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const backend = NodeChildProcessSpawner.layer.pipe(Layer.provideMerge(files))
const binding = (root: string) => {
  const fileBinding: Layer.Layer<
    FileSystem.FileSystem | Path.Path,
    PlatformError.PlatformError,
    FileSystem.FileSystem | Path.Path
  > = ToolkitFileSystem.layer({ root })
  const processBinding: Layer.Layer<
    ChildProcessSpawner.ChildProcessSpawner,
    PlatformError.PlatformError,
    ChildProcessSpawner.ChildProcessSpawner | Path.Path
  > = ToolkitProcess.layer({ root })
  return Layer.merge(fileBinding, processBinding).pipe(Layer.provide(backend))
}

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const temporary = yield* fs.makeTempDirectoryScoped()
      const root = yield* fs.realPath(temporary)
      const roots = [path.join(root, 'first'), path.join(root, 'second')]
      yield* Effect.forEach(roots, (root) => fs.makeDirectory(root))
      const useBinding = (root: string, text: string) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
          yield* fs.writeFileString('message.txt', text)
          // Native direct argv; the wrapper never turns arguments into a shell script.
          const output = yield* spawner.string(
            ChildProcess.make(process.execPath, [
              '-e',
              "process.stdout.write(require('node:fs').readFileSync('message.txt', 'utf8'))",
            ]),
          )
          yield* Effect.log(output)
        }).pipe(Effect.provide(binding(root)))
      yield* Effect.forEach(roots, (root, i) => useBinding(root, `toolkit ${i + 1}`), {
        concurrency: 'unbounded',
      })
    }),
  ).pipe(Effect.provide(files)),
)
