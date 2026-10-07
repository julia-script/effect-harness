import { NodeFileSystem } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as EventJournal from 'effect/eventlog/EventJournal'
import * as SnapshotStore from '@effect-harness/durable/storage/SnapshotStore'
import * as Path from 'effect/Path'
import * as Jsonl from '@effect-harness/durable/storage/JsonlStore'
import * as Memory from '@effect-harness/durable/storage/Memory'
import * as Sqlite from '../storage/TestStore.ts'
import { makeStorageConformance } from '@effect-harness/durable/testing/Conformance'
import { withStorage } from '@effect-harness/durable/testing/Storage'
import type { Store } from '@effect-harness/durable/Store'

const env = Layer.merge(NodeFileSystem.layer, Path.layer)
const jsonl = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const directory = yield* fs.makeTempDirectoryScoped()
    return Jsonl.layer({ directory })
  }),
).pipe(Layer.provide(env))
const sqlite = Sqlite.layer.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' })))
const backends: ReadonlyArray<{
  readonly name: string
  readonly layer: Layer.Layer<Store, Layer.Error<typeof jsonl> | Layer.Error<typeof sqlite>>
}> = [
  { name: 'Memory', layer: Memory.layer },
  {
    name: 'Effect snapshot persistence (memory)',
    layer: SnapshotStore.layer.pipe(
      Layer.provide(Layer.mergeAll(KeyValueStore.layerMemory, EventJournal.layerMemory)),
    ),
  },
  { name: 'JSONL', layer: jsonl },
  { name: 'Effect snapshot persistence (SQLite)', layer: sqlite },
]

describe('Conformance', () => {
  for (const backend of backends)
    describe(backend.name, () => {
      for (const test of makeStorageConformance(assert))
        it.effect(test.name, () => withStorage(test.run, backend.layer))
    })
})
