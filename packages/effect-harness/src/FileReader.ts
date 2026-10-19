import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import type * as PlatformError from 'effect/PlatformError'
import type * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import { FileReaderError } from './FileReaderError.js'

/** Allocation guard per positional read; callers can stream larger ranges in chunks. */
export const maxReadBytes = 1024 * 1024

/** One opened file identity. Cursor movement is serialized, including concurrent readAt calls. */
export interface Reader {
  readonly stat: Effect.Effect<FileSystem.File.Info, FileReaderError>
  readonly readAt: (offset: bigint, length: number) => Effect.Effect<Uint8Array, FileReaderError>
}

export interface OpenOptions {
  /** Native FileSystem.open has no atomic no-follow flag; true is explicitly unsupported. */
  readonly noFollow?: boolean
}

/** Bind the reader to one native filesystem backend; opened readers belong to the caller's Scope. */
export class FileReader extends Context.Service<
  FileReader,
  {
    readonly open: (
      path: string,
      options?: OpenOptions,
    ) => Effect.Effect<Reader, FileReaderError, Scope.Scope>
  }
>()('effect-harness/FileReader') {}

/**
 * Positional/binary seam for text and later image readers. The handle is opened
 * before stat and remains on that file across path replacement or rename.
 * Closing its invocation scope closes the native handle and rejects later use.
 * It follows symlinks; it never pretends stat-then-open is atomic no-follow.
 */
export const layer: Layer.Layer<FileReader, never, FileSystem.FileSystem> = Layer.effect(
  FileReader,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    return {
      open: Effect.fn('FileReader.open')(function* (path: string, options?: OpenOptions) {
        const fail = (reason: FileReaderError['reason'], operation: string, message: string) =>
          new FileReaderError({ reason, operation, path, message })
        if (path.length === 0 || path.includes('\0'))
          return yield* fail('invalid', 'open', 'A file path must be nonempty and contain no NUL')
        if (options?.noFollow)
          return yield* fail(
            'unsupported',
            'open',
            'The native backend cannot open with atomic noFollow',
          )
        const file = yield* fs.open(path, { flag: 'r' }).pipe(
          Effect.mapError(
            (cause) =>
              new FileReaderError({
                reason: 'filesystem',
                operation: 'open',
                path,
                message: cause.message,
                cause,
              }),
          ),
        )
        const mutex = yield* Semaphore.make(1)
        let closed = false
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            closed = true
          }),
        )
        const protect = <A>(
          operation: string,
          effect: Effect.Effect<A, PlatformError.PlatformError>,
        ) =>
          mutex.withPermit(
            Effect.suspend(() => {
              if (closed)
                return Effect.fail(fail('closed', operation, 'The reader scope is closed'))
              return effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new FileReaderError({
                      reason: 'filesystem',
                      operation,
                      path,
                      message: cause.message,
                      cause,
                    }),
                ),
              )
            }),
          )
        const stat = protect('stat', file.stat)
        const info = yield* stat
        if (info.type !== 'File') return yield* fail('notFile', 'open', 'Expected a regular file')
        return {
          stat,
          readAt: Effect.fn('FileReader.readAt')(function* (offset: bigint, length: number) {
            if (offset < 0n || !Number.isSafeInteger(length) || length < 0 || length > maxReadBytes)
              return yield* fail(
                'invalid',
                'readAt',
                `offset must be nonnegative and length between 0 and ${maxReadBytes}`,
              )
            return yield* protect(
              'readAt',
              Effect.gen(function* () {
                yield* file.seek(offset, 'start')
                const buffer = new Uint8Array(length)
                let used = 0
                while (used < length) {
                  const count = yield* file.read(buffer.subarray(used))
                  if (count === 0) break
                  used += count
                }
                return buffer.subarray(0, used)
              }),
            )
          }),
        }
      }),
    }
  }),
)

export { FileReaderError } from './FileReaderError.js'
