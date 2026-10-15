import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Transaction from 'effect-harness/Transaction'

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  it.live(
    `${backend}: runtime rejects nonempty format11 before mutation or provider execution`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
          const layer = (() => {
            if (backend === 'sqlite')
              return Storage.layerSql.pipe(
                Layer.provide(SqliteClient.layer({ filename: `${directory}/old.sqlite` })),
              )
            if (backend === 'jsonl')
              return Storage.layerJsonl({ filePath: `${directory}/old.jsonl` })
            return Storage.layerMemory
          })()
          const services = yield* Layer.build(layer)
          const session = yield* Session.make().pipe(Effect.provideContext(services))
          yield* Session.commit(session, Transaction.ensureRoot)
          yield* StorageFormat.requireSupportedFormat({
            formatVersion: 11,
            minimumReaderVersion: 11,
          }).pipe(Effect.provideContext(services))
          const actual = Context.get(services, Storage.Storage)
          const format = Context.get(services, StorageFormat.StorageFormat)
          let writes = 0
          let allocations = 0
          let upgrades = 0
          let calls = 0
          const observed: Storage.Storage['Service'] = {
            ...actual,
            commit: (batch) => {
              writes++
              return actual.commit(batch)
            },
            mintId: () => {
              allocations++
              return actual.mintId()
            },
          }
          const guarded = Context.make(Storage.Storage, observed).pipe(
            Context.add(StorageFormat.StorageFormat, {
              ...format,
              storage: observed,
              requireSupportedFormat: (requested) => {
                upgrades++
                return format.requireSupportedFormat(requested)
              },
            }),
          )
          const model = yield* LanguageModel.make({
            generateText: () => {
              calls++
              return Effect.die('unexpected')
            },
            streamText: () => {
              calls++
              return Stream.die('unexpected')
            },
          })
          const failure = yield* HarnessRuntime.make().pipe(
            Effect.provideContext(guarded),
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.flip,
          )
          assert.strictEqual(failure.operation, 'runtime.make')
          assert.include(failure.message, 'runtime format 12')
          assert.deepEqual([writes, allocations, upgrades, calls], [0, 0, 0, 0])
          assert.deepEqual(yield* format.inspect, { formatVersion: 11, minimumReaderVersion: 11 })
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}
