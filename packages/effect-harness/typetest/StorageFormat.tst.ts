import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import type * as FileSystem from 'effect/FileSystem'
import type * as SqlClient from 'effect/sql/SqlClient'
import * as Storage from 'effect-harness/Storage'
import * as Format from 'effect-harness/StorageFormat'

test('the guard is a typed deliberate Storage operation; native Layers supply the sibling service', () => {
  expect(Format.requireSupportedFormat({ formatVersion: 2, minimumReaderVersion: 2 })).type.toBe<
    Effect.Effect<Format.Format, Format.Failure, Storage.Storage>
  >()
  expect(Format.inspect).type.toBe<Effect.Effect<Format.Format, Format.Failure, Storage.Storage>>()
  expect(Storage.layerMemory).type.toBe<
    Layer.Layer<Storage.Storage | Format.StorageFormat, Storage.StorageError>
  >()
  expect(Storage.layerSql).type.toBe<
    Layer.Layer<Storage.Storage | Format.StorageFormat, Storage.StorageError, SqlClient.SqlClient>
  >()
  expect(Storage.layerJsonl({ filePath: 'store' })).type.toBe<
    Layer.Layer<Storage.Storage | Format.StorageFormat, Storage.StorageError, FileSystem.FileSystem>
  >()
  const context = (storage: Storage.Storage['Service']) => Context.make(Storage.Storage, storage)
  expect<ReturnType<typeof context>>().type.toBe<Context.Context<Storage.Storage>>()
})

test('structural Storage adapters need no fake format or metadata methods', () => {
  const structural = (storage: Storage.Storage['Service']) => {
    const { documentMetadata: _metadata, ...adapter } = storage
    return adapter
  }
  expect<ReturnType<typeof structural>>().type.toBeAssignableTo<Storage.Storage['Service']>()
  const lookup = (storage: Storage.Storage['Service'], id: Storage.DocumentId) =>
    storage.documentMetadata?.(id)
  expect<ReturnType<typeof lookup>>().type.toBe<
    Effect.Effect<Option.Option<Storage.DocumentMetadata>, Storage.StorageError> | undefined
  >()
})
