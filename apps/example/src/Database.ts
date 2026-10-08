import { SqliteClient } from '@effect/sql-sqlite-bun'
import {
  Config,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Schema,
  type PlatformError,
  type Scope,
} from 'effect'
import type { SqlClient, SqlError } from 'effect/sql'

/** Invalid example configuration prevents database acquisition. */
export class ConfigurationError extends Schema.TaggedError<ConfigurationError>(
  '@effect-harness/example/Database/ConfigurationError',
)('ConfigurationError', { message: Schema.String, cause: Schema.Defect() }) {}

/** Use the configured database, allocating a scoped temporary directory only when absent. */
export const filename: Effect.Effect<
  string,
  ConfigurationError | PlatformError.PlatformError,
  FileSystem.FileSystem | Path.Path | Scope.Scope
> = Effect.gen(function* () {
  const configured = yield* Config.option(Config.String('EXAMPLE_DB')).pipe(
    Effect.mapError(
      (cause) => new ConfigurationError({ message: 'Cannot read EXAMPLE_DB', cause }),
    ),
  )
  return yield* Option.match(configured, {
    onSome: Effect.succeed,
    onNone: () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'effect-harness-example-' })
        return path.join(directory, 'example.sqlite')
      }),
  })
})

/** Acquire SQLite and its optional temporary directory in the same Layer scope. */
export const layer: Layer.Layer<
  SqlClient.SqlClient | SqliteClient.SqliteClient,
  ConfigurationError | PlatformError.PlatformError | SqlError.SqlError,
  FileSystem.FileSystem | Path.Path
> = Layer.unwrap(Effect.map(filename, (filename) => SqliteClient.layer({ filename })))
