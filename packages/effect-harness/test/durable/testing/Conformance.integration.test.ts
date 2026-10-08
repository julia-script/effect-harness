import * as DirectoryFixture from '../DirectoryFixture.ts'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as EventJournal from 'effect/eventlog/EventJournal'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import * as Path from 'effect/Path'
import * as JsonlStore from 'effect-harness/durable/storage/JsonlStore'
import * as StoreModule from 'effect-harness/durable/Store'
import * as TestStore from '../storage/TestStore.ts'
import { makeStorageConformance } from 'effect-harness/durable/testing/Conformance'
import { withStorage } from '../StorageFixture.ts'
import type { Store } from 'effect-harness/durable/Store'

const env = Layer.merge(NodeFileSystem.layer, Path.layer)
const jsonl = Layer.unwrap(
  Effect.gen(function* () {
    const directory = yield* DirectoryFixture.make()
    return JsonlStore.layer({ directory })
  }),
).pipe(Layer.provide(env))
const sqlite = TestStore.layer.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' })))
const backends: ReadonlyArray<{
  readonly name: string
  readonly layer: Layer.Layer<Store, Layer.Error<typeof jsonl> | Layer.Error<typeof sqlite>>
}> = [
  { name: 'Memory', layer: StoreModule.layerMemory },
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
