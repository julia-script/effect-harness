import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
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
  const executed = []
  for (const backend of ['memory', 'sqlite', 'jsonl']) {
    const make = Effect.gen(function* () {
      const layer = yield* Effect.gen(function* () {
        if (backend === 'memory') return Storage.layerMemory
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-consumer-' })
        directories.push(directory)
        const filename = path.join(directory, 'storage')
        return backend === 'sqlite'
          ? Storage.layerJournal().pipe(
              Layer.provide(SqlEventJournal.layer()),
              Layer.provide(SqliteClient.layer({ filename })),
            )
          : Storage.layerJsonl({ filePath: filename }).pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, fs)),
            )
      })
      return {
        open: Layer.build(layer).pipe(
          Effect.map((context) => Context.get(context, Storage.Storage)),
        ),
      }
    })
    // Source conformance owns the complete corpus. This process checks package exports,
    // execution, and scoped factory/reopen wiring on the built public modules.
    const name =
      backend === 'memory'
        ? 'storage: historical document revisions and copies'
        : 'storage: reopened state and reserved identities'
    const test = Testing.storageConformance({
      make,
      capabilities: { history: true, reopen: backend !== 'memory' },
    }).find((test) => test.name === name)
    if (test === undefined) return yield* Effect.fail(`Missing public case: ${name}`)
    yield* test.run
    executed.push(backend)
  }
  for (const directory of directories)
    if (yield* fs.exists(directory)) return yield* Effect.fail('Consumer fixture cleanup failed')
  return executed
}).pipe(Effect.provide(NodeServices.layer))

const executed = await Effect.runPromise(program)
console.log(`public storage consumer: ${executed.join(', ')}; temporary stores removed`)
