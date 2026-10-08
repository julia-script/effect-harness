/** Indexed persistence on Bun SQLite, with one scoped database owner. */
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Layer from 'effect/Layer'
import * as Effect from 'effect/Effect'
import { Persistence } from '../Persistence.ts'
import { make } from './internal/sqlite.ts'
import { IoError, isStorageError, rejected, type StorageError } from '../StorageError.ts'

export const layer = (
  options: SqliteClient.SqliteClientConfig,
): Layer.Layer<Persistence, StorageError> =>
  Layer.effect(Persistence, make).pipe(
    Layer.provide(SqliteClient.layer(options)),
    Layer.catch((error) =>
      Layer.effect(
        Persistence,
        Effect.fail(
          isStorageError(error)
            ? error
            : rejected('Cannot acquire SQLite persistence', IoError, error),
        ),
      ),
    ),
  )
