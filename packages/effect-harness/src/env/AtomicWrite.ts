/**
 * Atomic native file replacement with canonical destinations and settled writes.
 */
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Random from 'effect/Random'
import { NativeFiles } from '../NativeFiles.ts'
import * as Serialization from '../Serialization.ts'
import { fromPlatform } from '../Env.ts'
import { FileError, FileNotSupportedError, fileReason } from '../FileError.ts'

/**
 * Captures the selected file capabilities once and returns an atomic write operation.
 *
 * @category constructors
 */
export const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const native = yield* NativeFiles
  return Effect.fnUntraced(function* (
    resolved: string,
    content: string | Uint8Array,
  ): Effect.fn.Return<undefined, FileError> {
    const io = <A, R>(effect: Effect.Effect<A, PlatformError.PlatformError, R>) =>
      effect.pipe(Effect.mapError((error) => fromPlatform(error, resolved)))
    yield* io(fs.makeDirectory(path.dirname(resolved), { recursive: true }))
    const destination = yield* io(fs.realPath(resolved)).pipe(
      Effect.catchIf(
        (error) => error.code === 'not_found',
        Effect.fnUntraced(function* () {
          const info = yield* native.lstat(resolved).pipe(
            Effect.catchIf(
              (error) => error.code === 'not_found',
              () => Effect.void,
            ),
          )
          if (info?.kind === 'symlink')
            return yield* new FileError({
              reason: new FileNotSupportedError({
                path: resolved,
                message: 'Cannot atomically replace through an unresolved symlink',
              }),
            })
          const parent = yield* io(fs.realPath(path.dirname(resolved)))
          return path.join(parent, path.basename(resolved))
        }),
      ),
    )
    const metadata = yield* io(fs.stat(destination)).pipe(
      Effect.catchIf(
        (error) => error.code === 'not_found',
        () => Effect.void,
      ),
    )
    if (metadata !== undefined && metadata.type !== 'File')
      return yield* new FileError({
        reason: fileReason(metadata.type === 'Directory' ? 'is_directory' : 'not_supported', {
          path: resolved,
          message: 'Atomic replacement requires a regular file',
        }),
      })
    if (metadata !== undefined && Option.exists(metadata.nlink, (count) => count > 1))
      return yield* new FileError({
        reason: new FileNotSupportedError({
          path: resolved,
          message: 'Atomic replacement of multiply linked files is not supported',
        }),
      })
    if (metadata !== undefined) yield* io(fs.access(destination, { writable: true }))
    const directory = path.dirname(destination)
    const nonce = `${yield* Random.nextInt}-${yield* Random.nextInt}`
    const temporary = path.join(directory, `.effect-harness-${nonce}.tmp`)
    const bytes = typeof content === 'string' ? new TextEncoder().encode(content) : content
    yield* Effect.acquireUseRelease(
      // Exclusive creation never truncates a colliding file. Close before acquiring cleanup ownership.
      io(Effect.scoped(fs.open(temporary, { flag: 'wx' }))).pipe(Effect.as(temporary)),
      Effect.fnUntraced(function* () {
        yield* io(
          Effect.scoped(
            Effect.gen(function* () {
              const file = yield* fs.open(temporary, { flag: 'r+' })
              const staged = yield* file.stat
              // Keep admitted contents private while staged, including when replacing a 0600 file.
              yield* fs.chmod(temporary, 0o600)
              yield* file.writeAll(bytes)
              if (metadata !== undefined) {
                const ownership = Option.flatMap(metadata.uid, (uid) =>
                  Option.map(metadata.gid, (gid) => ({ uid, gid })),
                )
                yield* Option.match(ownership, {
                  onNone: () => Effect.void,
                  onSome: ({ uid, gid }) => {
                    const changed = Option.match(staged.uid, {
                      onNone: () => true,
                      onSome: (stagedUid) =>
                        Option.match(staged.gid, {
                          onNone: () => true,
                          onSome: (stagedGid) => stagedUid !== uid || stagedGid !== gid,
                        }),
                    })
                    return changed ? fs.chown(temporary, uid, gid) : Effect.void
                  },
                })
                // chown may clear set-ID bits, so restore mode afterwards.
                yield* fs.chmod(temporary, metadata.mode & 0o7777)
              } else yield* fs.chmod(temporary, staged.mode & 0o7777)
              yield* file.sync
            }),
          ),
        )
        yield* io(fs.rename(temporary, destination))
        yield* io(
          Effect.scoped(
            fs.open(directory, { flag: 'r' }).pipe(Effect.flatMap((file) => file.sync)),
          ).pipe(
            Effect.catchIf(
              // Skip only native errors identifying unavailable directory sync, not bad arguments or I/O faults.
              (error) => {
                const cause = error.reason.cause
                const code = Serialization.stringPropertyOrUndefined(cause, 'code') ?? ''
                return (
                  ['EINVAL', 'EISDIR', 'ENOTSUP', 'ENOSYS'].includes(code) ||
                  (path.sep === '\\' && code === 'EPERM')
                )
              },
              () => Effect.void,
            ),
          ),
        )
      }),
      () => io(fs.remove(temporary, { force: true })).pipe(Effect.orDie),
    )
  })
})
