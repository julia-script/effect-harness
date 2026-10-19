import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import { CodingFileError } from '../coding/FileError.js'

/** Source tool namespace prefix and screenshot filename Unicode-space normalization. */
export const resolve = Effect.fn('CodingFilePath.resolve')(function* (input: string) {
  const path = yield* Path.Path
  let normalized = input.replace(/[\u00A0\u2000-\u200A\u202F\u205F\u3000]/g, ' ')
  if (normalized.startsWith('@')) normalized = normalized.slice(1)
  if (normalized.length === 0 || normalized.includes('\0'))
    return yield* new CodingFileError({
      reason: 'invalidPath',
      path: input,
      message: 'A file path must be nonempty and contain no NUL',
    })
  return path.resolve(normalized)
})

/** Read-only filename variants; only missing candidates permit fallback. */
export const resolveRead = Effect.fn('CodingFilePath.resolveRead')(function* (input: string) {
  const fs = yield* FileSystem.FileSystem
  const resolved = yield* resolve(input)
  const variants = new Set([
    resolved,
    resolved.replace(/ (AM|PM)\./gi, '\u202F$1.'),
    resolved.normalize('NFD'),
    resolved.replace(/'/g, '\u2019'),
    resolved.normalize('NFD').replace(/'/g, '\u2019'),
  ])
  for (const candidate of variants) {
    const exists = yield* fs.exists(candidate).pipe(
      Effect.mapError(
        (cause) =>
          new CodingFileError({
            reason: 'filesystem',
            path: input,
            message: cause.message,
            cause,
          }),
      ),
    )
    if (exists) return candidate
  }
  return resolved
})
