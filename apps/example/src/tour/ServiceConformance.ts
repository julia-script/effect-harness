/** Run reusable cases with an ordinary Effect runtime; no test runner dependency. */
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Reader from 'effect-harness/FileReader'
import * as Directory from 'effect-harness/DirectoryReader'
import * as DirectoryNode from 'effect-harness/DirectoryReaderNode'
import * as ToolkitFiles from 'effect-harness/ToolkitFileSystem'
import * as Checks from 'effect-harness/testing/FileSystem'

const backend = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
  const bound = ToolkitFiles.layer({ root }).pipe(Layer.provide(backend))
  const services = yield* Layer.build(
    Layer.mergeAll(bound, Reader.layer.pipe(Layer.provide(bound)), DirectoryNode.layer({ root })),
  )
  return {
    root,
    cleanup: Effect.void,
    fs: Context.get(services, FileSystem.FileSystem),
    path: Context.get(services, Path.Path),
    reader: Context.get(services, Reader.FileReader),
    directory: Context.get(services, Directory.DirectoryReader),
  } satisfies Checks.FileSystemFixture
}).pipe(Effect.provide(backend))

const cases = Checks.fileSystemConformance({
  make,
  capabilities: { noFollow: 'unsupported', boundedDirectory: true },
})

await Effect.runPromise(
  Effect.forEach(cases, (c) => c.run.pipe(Effect.tap(() => Effect.log(`Passed ${c.name}`))), {
    discard: true,
  }),
)
