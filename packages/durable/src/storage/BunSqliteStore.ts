/**
 * Bun SQLite platform storage acquisition.
 *
 * @since 0.0.0
 */
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Layer from 'effect/Layer'
import type * as Config from 'effect/Config'
import type * as SqlError from 'effect/sql/SqlError'
import type { StorageError } from '../StorageError.ts'
import type { Store } from '../Store.ts'
import * as SqliteStore from './SqliteStore.ts'

/**
 * layer service Layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (
  options: SqliteClient.SqliteClientConfig,
): Layer.Layer<Store, StorageError | SqlError.SqlError> =>
  SqliteStore.layer.pipe(Layer.provide(SqliteClient.layer(options)))

/**
 * Resolves native database options through the caller's ConfigProvider, retaining acquisition errors.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerConfig = (
  config: Config.Wrap<SqliteClient.SqliteClientConfig>,
): Layer.Layer<Store, StorageError | Config.ConfigError | SqlError.SqlError> =>
  SqliteStore.layer.pipe(Layer.provide(SqliteClient.layerConfig(config)))

/**
 * Canonical SqliteBun Store layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerStoreSqliteBun: typeof layer = layer
/** Scoped memory alternative for the same Store service. */
/**
 * Scoped memory alternatives to this storage backend.
 *
 * @category layers
 * @since 0.0.0
 */
export { makeMemory, layerMemory, layerStoreMemory } from '../Store.ts'
