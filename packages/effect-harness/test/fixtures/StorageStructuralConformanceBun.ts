import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

// Public imports; Bun filesystem services and fresh JSONL stores for every case.
const names = new Set([
  'storage: prototype-sensitive JSON keys',
  'storage: lossless indexed string identities',
  'storage: out-of-ID-order entry streams',
  'storage: multigeneration fork cutoffs',
  'storage: waiting and completing task records',
  'storage: passive write submission records',
  'storage: long document replacement tails',
])
const directories: Array<string> = []
const make = () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-structural-bun-' })
    directories.push(directory)
    const filename = path.join(directory, 'storage')
    const layer = Storage.layerJsonl({ filePath: filename }).pipe(
      Layer.provide(Layer.succeed(FileSystem.FileSystem, fs)),
    )
    return { open: Layer.build(layer).pipe(Effect.map((c) => Context.get(c, Storage.Storage))) }
  })
await Effect.runPromise(
  Effect.gen(function* () {
    for (const backend of ['jsonl'] as const) {
      const cases = Testing.storageConformance({
        make: make(),
        capabilities: { history: true },
      }).filter((c) => names.has(c.name))
      if (cases.length !== names.size)
        return yield* new Testing.StorageConformanceError({
          caseName: 'structural case inventory',
          assertion: 'all structural cases are exposed',
          message: 'Missing structural storage cases',
          actual: cases.map((c) => c.name),
          expected: [...names],
        })
      for (const c of cases) {
        yield* c.run
        yield* Effect.log(`${backend}: ${c.name}`)
      }
    }
    const fs = yield* FileSystem.FileSystem
    for (const directory of directories)
      if (yield* fs.exists(directory))
        return yield* new Testing.StorageConformanceError({
          caseName: 'structural storage cleanup',
          assertion: 'factory scope removes temporary storage',
          message: 'Temporary storage remains after conformance',
          actual: directory,
          expected: 'removed',
        })
  }).pipe(Effect.provide(Layer.merge(BunFileSystem.layer, BunPath.layer))),
)
