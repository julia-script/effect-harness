import { NodeFileSystem } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Jsonl from '../../src/storage/Jsonl.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Sqlite from '../../src/storage/Sqlite.ts'
import { createStorageConformance } from '../../src/testing/Conformance.ts'
import { sessionLayer } from '../../src/testing/Storage.ts'

const env = Layer.merge(NodeFileSystem.layer, Path.layer)
const jsonl = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const directory = yield* fs.makeTempDirectoryScoped()
    return Jsonl.layer({ directory })
  }),
).pipe(Layer.provide(env))
const sqlite = Sqlite.layer.pipe(Layer.provide(SqliteClient.layer({ filename: ':memory:' })))
const backends = [
  { name: 'Memory', layer: Memory.layer },
  { name: 'JSONL', layer: jsonl },
  { name: 'SQLite', layer: sqlite },
]

for (const backend of backends)
  describe(backend.name, () => {
    for (const test of createStorageConformance(assert))
      it.effect(test.name, () =>
        Effect.scoped(test.run.pipe(Effect.provide(sessionLayer(backend.layer)))),
      )
  })
