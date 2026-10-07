import * as Config from 'effect/Config'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'

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
  if (Option.isSome(configured)) return configured.value
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'effect-harness-example-' })
  return path.join(directory, 'example.sqlite')
})
