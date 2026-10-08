import * as DirectoryFixture from '../DirectoryFixture.ts'
import { assert, describe, it } from '@effect/vitest'

import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'

import * as Config from 'effect/Config'

import * as ConfigProvider from 'effect/ConfigProvider'

import * as Effect from 'effect/Effect'

import * as Layer from 'effect/Layer'

import * as Path from 'effect/Path'

import * as SqlError from 'effect/sql/SqlError'

import * as Record from 'effect-harness/durable/Record'

import * as Store from 'effect-harness/durable/Store'

import type { StorageError } from 'effect-harness/durable/StorageError'

import * as TestStore from './TestStore.ts'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

const platform = Layer.merge(NodeFileSystem.layer, Path.layer)

const provider = (env: Readonly<Record<string, string>>) =>
  ConfigProvider.layer(ConfigProvider.fromEnv({ env }))

const nodeConfig: Layer.Layer<Store.Store, StorageError | Config.ConfigError | SqlError.SqlError> =
  TestStore.layer.pipe(
    Layer.provide(SqliteClient.layerConfig({ filename: Config.String('DURABLE_SQLITE') })),
  )

describe('SnapshotStoreConfig', () => {
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
      const directory = yield* DirectoryFixture.make()
      const failed = yield* Store.Store.pipe(
        Effect.provide(nodeConfig.pipe(Layer.provide(provider({ DURABLE_SQLITE: directory })))),
        Effect.flip,
      )
      assert.ok(failed instanceof SqlError.SqlError)
    }).pipe(Effect.provide(platform)),
  )
})
