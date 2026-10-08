import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as SqlError from 'effect/sql/SqlError'
import * as Record from 'effect-harness/durable/Record'
import * as Store from 'effect-harness/durable/Store'
import type { StorageError } from 'effect-harness/durable/StorageError'
import * as Jsonl from 'effect-harness/durable/storage/JsonlStore'
import * as SnapshotStore from './TestStore.ts'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

const platform = Layer.merge(NodeFileSystem.layer, Path.layer)
const provider = (env: Readonly<Record<string, string>>) =>
  ConfigProvider.layer(ConfigProvider.fromEnv({ env }))
const nodeConfig: Layer.Layer<Store.Store, StorageError | Config.ConfigError | SqlError.SqlError> =
  SnapshotStore.layer.pipe(
    Layer.provide(SqliteClient.layerConfig({ filename: Config.String('DURABLE_SQLITE') })),
  )

describe('Config', () => {
  it.effect(
    'uses the supplied ConfigProvider for native SQLite and preserves missing ConfigError',
    () =>
      Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const store = yield* Store.Store
          assert.strictEqual(yield* Store.mintId(Record.TaskId), 2)
          assert.strictEqual((yield* store.read).nextId, 3)
        }).pipe(
          Effect.provide(nodeConfig.pipe(Layer.provide(provider({ DURABLE_SQLITE: ':memory:' })))),
        )
        const missing = yield* Store.Store.pipe(
          Effect.provide(nodeConfig.pipe(Layer.provide(provider({})))),
          Effect.flip,
        )
        assert.ok(missing instanceof Config.ConfigError)
      }),
  )

  it.effect('retains native SqlError acquisition failures alongside Config and StorageError', () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const failed = yield* Store.Store.pipe(
        Effect.provide(nodeConfig.pipe(Layer.provide(provider({ DURABLE_SQLITE: directory })))),
        Effect.flip,
      )
      assert.ok(failed instanceof SqlError.SqlError)
    }).pipe(Effect.provide(platform)),
  )

  it.effect(
    'resolves complete JSONL options lazily and persists into the configured directory',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const parent = yield* fs.makeTempDirectoryScoped()
        const directory = path.join(parent, 'configured')
        const configured = Jsonl.layerConfig({
          directory: Config.String('DURABLE_JSONL'),
          fsync: Config.Boolean('DURABLE_FSYNC'),
        })
        assert.strictEqual(yield* fs.exists(directory), false)
        yield* Store.mintId(Record.TaskId).pipe(
          Effect.provide(
            configured.pipe(
              Layer.provide(provider({ DURABLE_JSONL: directory, DURABLE_FSYNC: 'true' })),
            ),
          ),
        )
        assert.strictEqual(yield* fs.exists(path.join(directory, 'commits.jsonl')), true)
        const reopened = yield* Jsonl.make({ directory })
        assert.strictEqual((yield* reopened.read).nextId, 3)
        const missing = yield* Store.Store.pipe(
          Effect.provide(configured.pipe(Layer.provide(provider({})))),
          Effect.flip,
        )
        assert.ok(missing instanceof Config.ConfigError)
        const invalid = yield* Store.Store.pipe(
          Effect.provide(
            configured.pipe(
              Layer.provide(provider({ DURABLE_JSONL: directory, DURABLE_FSYNC: 'neither' })),
            ),
          ),
          Effect.flip,
        )
        assert.ok(invalid instanceof Config.ConfigError)
      }).pipe(Effect.provide(platform)),
  )
})
