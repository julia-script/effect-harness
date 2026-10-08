import * as DirectoryFixture from '../DirectoryFixture.ts'
import { assert, describe, it } from '@effect/vitest'

import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'

import * as Config from 'effect/Config'

import * as ConfigProvider from 'effect/ConfigProvider'

import * as Effect from 'effect/Effect'

import * as FileSystem from 'effect/FileSystem'

import * as Layer from 'effect/Layer'

import * as Path from 'effect/Path'

import * as Record from 'effect-harness/durable/Record'

import * as Store from 'effect-harness/durable/Store'

import * as JsonlStore from 'effect-harness/durable/storage/JsonlStore'

const platform = Layer.merge(NodeFileSystem.layer, Path.layer)

const provider = (env: Readonly<Record<string, string>>) =>
  ConfigProvider.layer(ConfigProvider.fromEnv({ env }))

describe('JsonlStoreConfig', () => {
  it.effect(
    'resolves complete JSONL options lazily and persists into the configured directory',
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const parent = yield* DirectoryFixture.make()
        const directory = path.join(parent, 'configured')
        const configured = JsonlStore.layerConfig({
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
        const reopened = yield* JsonlStore.make({ directory })
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
