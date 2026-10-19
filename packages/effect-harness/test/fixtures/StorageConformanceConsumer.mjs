import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import { Testing as RootTesting } from 'effect-harness'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

const { SqliteClient } =
  'Bun' in globalThis
    ? await import('@effect/sql-sqlite-bun')
    : await import('@effect/sql-sqlite-node')

// Executes against package exports and dist, outside Vitest's source aliases.
const program = Effect.gen(function* () {
  if (RootTesting.storageConformance !== Testing.storageConformance)
    return yield* Effect.fail('Root and subpath Testing exports disagree')
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directories = []
  const executed = []
  for (const backend of ['memory', 'jsonl', 'sqlite']) {
    const make = Effect.gen(function* () {
      const layer = yield* Effect.gen(function* () {
        if (backend === 'memory') return Storage.layerMemory
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-consumer-' })
        directories.push(directory)
        const filename = path.join(directory, 'storage')
        if (backend === 'sqlite')
          return Storage.layerSql().pipe(Layer.provide(SqliteClient.layer({ filename })))
        return Storage.layerJsonl({ filePath: filename }).pipe(
          Layer.provide(Layer.succeed(FileSystem.FileSystem, fs)),
        )
      })
      return {
        open: Layer.build(Layer.fresh(layer)).pipe(
          Effect.map((context) => Context.get(context, Storage.Storage)),
        ),
      }
    })
    // Source conformance owns the complete corpus. This process checks package exports,
    // execution, and scoped factory/reopen wiring on the built public modules.
    const name =
      backend === 'memory'
        ? 'storage: mixed batch rollback'
        : 'storage: reopened state and committed identities'
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
