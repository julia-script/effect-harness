import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Layer from 'effect/Layer'
import type * as Config from 'effect/Config'
import type * as SqlError from 'effect/sql/SqlError'
import type { StorageError } from '../StorageError.ts'
import type { Store } from '../Store.ts'
import * as Sqlite from './Sqlite.ts'

export const layer = (
  options: SqliteClient.SqliteClientConfig,
): Layer.Layer<Store, StorageError | SqlError.SqlError> =>
  Sqlite.layer.pipe(Layer.provide(SqliteClient.layer(options)))

/** Resolve native database options through the caller's ConfigProvider, retaining acquisition errors. */
export const layerConfig = (
  config: Config.Wrap<SqliteClient.SqliteClientConfig>,
): Layer.Layer<Store, StorageError | Config.ConfigError | SqlError.SqlError> =>
  Sqlite.layer.pipe(Layer.provide(SqliteClient.layerConfig(config)))
