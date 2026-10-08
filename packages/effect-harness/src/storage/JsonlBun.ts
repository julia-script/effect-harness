/** Bun JSONL persistence. commits.jsonl stores records; owner.sqlite holds only the ownership lock. */
import { BunFileSystem } from '@effect/platform-bun'
import * as SqliteClient from '@effect/sql-sqlite-bun/SqliteClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import { Persistence } from '../Persistence.ts'
import { IoError, isStorageError, rejected, type StorageError } from '../StorageError.ts'
import * as Jsonl from './internal/jsonl.ts'

export type Options = Jsonl.Options
export const Options = Jsonl.Options
export const layer = (options: Options): Layer.Layer<Persistence, StorageError> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      yield* fs
        .makeDirectory(options.directory, { recursive: true })
        .pipe(Effect.mapError((cause) => rejected('Cannot create JSONL directory', IoError, cause)))
      return Layer.effect(Persistence, Jsonl.make(options)).pipe(
        Layer.provide(
          SqliteClient.layer({ filename: `${options.directory}/owner.sqlite`, busyTimeout: 0 }),
        ),
      )
    }),
  ).pipe(
    Layer.provide(BunFileSystem.layer),
    Layer.catch((error) =>
      Layer.effect(
        Persistence,
        Effect.fail(
          isStorageError(error)
            ? error
            : rejected('Cannot acquire JSONL persistence', IoError, error),
        ),
      ),
    ),
  )
