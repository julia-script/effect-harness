import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import { Testing as RootTesting } from 'effect-harness'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

// Executes against package exports and dist, outside Vitest's source aliases.
const program = Effect.gen(function* () {
  if (RootTesting.storageConformance !== Testing.storageConformance)
    return yield* Effect.fail('Root and subpath Testing exports disagree')
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directories = []
  let count = 0
  for (const backend of ['memory', 'sqlite', 'jsonl']) {
    const make = Effect.gen(function* () {
      let layer = Storage.layerMemory
      if (backend !== 'memory') {
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-consumer-' })
        directories.push(directory)
        const filename = path.join(directory, 'storage')
        layer =
          backend === 'sqlite'
            ? Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))
            : Storage.layerJsonl({ filePath: filename }).pipe(
                Layer.provide(Layer.succeed(FileSystem.FileSystem, fs)),
              )
      }
      return {
        open: Layer.build(layer).pipe(
          Effect.map((context) => Context.get(context, Storage.Storage)),
        ),
      }
    })
    for (const test of Testing.storageConformance({
      make,
      capabilities: { history: true, reopen: backend !== 'memory' },
    })) {
      yield* test.run
      count++
    }
  }
  for (const directory of directories)
    if (yield* fs.exists(directory)) return yield* Effect.fail('Consumer fixture cleanup failed')
  return count
}).pipe(Effect.provide(NodeServices.layer))

const count = await Effect.runPromise(program)
console.log(`public storage consumer: ${count} cases passed; temporary stores removed`)
