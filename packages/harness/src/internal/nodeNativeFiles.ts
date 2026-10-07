import { constant } from 'effect/Function'
import * as Ref from 'effect/Ref'
import * as Time from '../Time.ts'
import * as NativeError from '../env/NativeError.ts'
import * as Serialization from '../Serialization.ts'
/** Node adapter for native capabilities missing from Effect FileSystem. Portable Env never imports this module. */
import * as Fs from 'node:fs'
import * as FsPromises from 'node:fs/promises'
import * as Path from 'node:path'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Stream from 'effect/Stream'
import * as Queue from 'effect/Queue'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import {
  FileError,
  NativeFiles,
  makeBinaryReader,
  makeDirReader,
  type BinaryReader,
  type FileInfo,
  FileInvalid,
  fileReason,
} from '../Env.ts'
import * as LineScan from '../env/LineScan.ts'

/**
 * Maps a caught native filesystem error while preserving its path and cause.
 *
 * @category combinators
 * @since 0.0.0
 */
export function fileError(error: unknown, path: string): FileError {
  if (error instanceof FileError) return error
  const code = NativeError.code(error) ?? ''
  let mapped: FileError['code'] = 'unknown'
  switch (code) {
    case 'ABORT_ERR':
      mapped = 'aborted'
      break
    case 'ENOENT':
      mapped = 'not_found'
      break
    case 'EACCES':
    case 'EPERM':
      mapped = 'permission_denied'
      break
    case 'ENOTDIR':
      mapped = 'not_directory'
      break
    case 'EISDIR':
      mapped = 'is_directory'
      break
    case 'ELOOP':
    case 'EINVAL':
      mapped = 'invalid'
      break
    case 'ENOSYS':
    case 'ENOTSUP':
      mapped = 'not_supported'
      break
  }

  return new FileError({
    reason: fileReason(mapped, { message: Serialization.errorText(error), path, cause: error }),
  })
}
// effect-review-allow P1-throw-only-in-unsafe-orthrow: unsupported native Stats kinds throw through lstat Effect.try; openDirReader filters unsupported kinds before calling infoUnsafe.
function infoUnsafe(path: string, stat: Fs.Stats): FileInfo {
  let kind: FileInfo['kind']
  if (stat.isFile()) kind = 'file'
  else if (stat.isDirectory()) kind = 'directory'
  else if (stat.isSymbolicLink()) kind = 'symlink'
  else
    throw new FileError({
      reason: new FileInvalid({ message: 'Path is not a supported file kind', path }),
    })
  return {
    name: Path.basename(path),
    path,
    kind,
    size: stat.size,
    mtimeMs: Time.fromEpochMillis(stat.mtimeMs),
    identity: JSON.stringify([stat.dev, stat.ino]),
  }
}
const io = <A>(path: string, operation: () => Promise<A>): Effect.Effect<A, FileError> =>
  Effect.tryPromise({
    try: operation,
    catch: (cause) => (cause instanceof FileError ? cause : fileError(cause, path)),
  })
/**
 * Layer for NodeNativeFiles native capabilities.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerNative: Layer.Layer<NativeFiles> = Layer.succeed(
  NativeFiles,
  NativeFiles.of({
    // Each no-follow sample observes this path now; read consistency checks need separate before/after observations.
    lstat: (path) =>
      io(path, () => FsPromises.lstat(path)).pipe(
        Effect.flatMap((stat) =>
          Effect.try({
            try: () => infoUnsafe(path, stat),
            catch: (cause) => fileError(cause, path),
          }),
        ),
      ),
    // Acquiring a cursor owns a distinct native handle and scope; sharing/deduplicating this acquisition is invalid.
    openBinaryReader: Effect.fnUntraced(function* (path, options) {
      const closed = yield* Ref.make(false)
      if (options?.noFollow === true && process.platform === 'win32') {
        const before = yield* io(path, () => FsPromises.lstat(path))
        if (before.isSymbolicLink())
          return yield* new FileError({
            reason: new FileInvalid({ message: 'Final symlink is forbidden', path }),
          })
      }
      const lock = yield* Semaphore.make(1)
      const release = (file: FsPromises.FileHandle) =>
        lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (yield* Ref.get(closed)) return
              yield* Ref.set(closed, true)
              yield* Effect.promise(() => file.close())
            }),
          ),
        )
      const file = yield* Effect.acquireRelease(
        io(path, () =>
          FsPromises.open(
            path,
            Fs.constants.O_RDONLY |
              Fs.constants.O_NONBLOCK |
              (options?.noFollow === true ? (Fs.constants.O_NOFOLLOW ?? 0) : 0),
          ),
        ),
        release,
      )
      const close = release(file)
      const stat = yield* (
        // Handle metadata is freshly sampled on every use, including after a read or replacement.
        io(path, () => file.stat()).pipe(Effect.onError(constant(close)))
      )
      if (!stat.isFile()) {
        yield* close
        return yield* new FileError({
          reason: fileReason(stat.isDirectory() ? 'is_directory' : 'invalid', {
            message: 'Reader requires a regular file',
            path,
          }),
        })
      }
      const live = <A>(effect: Effect.Effect<A, FileError>): Effect.Effect<A, FileError> =>
        Effect.suspend(() =>
          Ref.getUnsafe(closed)
            ? Effect.fail(
                new FileError({ reason: new FileInvalid({ message: 'Reader is closed', path }) }),
              )
            : effect,
        )
      const read = Effect.fnUntraced(
        function* (offset: number, length: number) {
          if (
            !Number.isSafeInteger(offset) ||
            offset < 0 ||
            !Number.isSafeInteger(length) ||
            length < 0
          )
            return yield* new FileError({
              reason: new FileInvalid({ message: 'Invalid byte range', path }),
            })
          const chunks: Array<Uint8Array> = []
          let count = 0
          while (count < length) {
            const chunk = new Uint8Array(Math.min(length - count, 1024 * 1024))
            const result = yield* Effect.uninterruptible(
              io(path, () => file.read(chunk, 0, chunk.length, offset + count)),
            )
            if (result.bytesRead === 0) break
            chunks.push(chunk.subarray(0, result.bytesRead))
            count += result.bytesRead
          }
          if (chunks.length === 1 && chunks[0] !== undefined) return chunks[0]
          return yield* Effect.try({
            try: () => {
              const bytes = new Uint8Array(count)
              let position = 0
              for (const chunk of chunks) {
                bytes.set(chunk, position)
                position += chunk.length
              }
              return bytes
            },
            catch: (cause) =>
              new FileError({
                reason: new FileInvalid({
                  message: 'Read result cannot be allocated',
                  path,
                  cause,
                }),
              }),
          })
        },
        (effect) => lock.withPermit(live(effect)),
      )
      const reader: BinaryReader = makeBinaryReader({
        info: lock.withPermit(
          live(
            // Handle metadata is freshly sampled on every use, including after a read or replacement.
            io(path, () => file.stat()).pipe(
              Effect.map((stat) => ({
                name: Path.basename(path),
                path,
                kind: 'file' as const,
                size: stat.size,
                mtimeMs: Time.fromEpochMillis(stat.mtimeMs),
              })),
            ),
          ),
        ),
        read,
        scanLines: Effect.fnUntraced(function* (options) {
          const scanner = yield* Effect.fromResult(
            LineScan.make(options.startLine, { endLine: options.endLine }),
          ).pipe(
            Effect.mapError(
              (error) =>
                new FileError({ reason: new FileInvalid({ message: error.message, path }) }),
            ),
          )
          let offset = 0
          while (true) {
            const bytes = yield* read(offset, 65536)
            if (bytes.length === 0) break
            offset += bytes.length
            yield* LineScan.push(scanner, bytes)
          }
          return yield* LineScan.finish(scanner)
        }),
      })
      return reader
    }),
    // Each directory acquisition/pages owns its cursor; there is no bulk atomic native metadata snapshot.
    openDirReader: Effect.fnUntraced(function* (path) {
      const closed = yield* Ref.make(false)
      const lock = yield* Semaphore.make(1)
      const directory = yield* Effect.acquireRelease(
        io(path, () => FsPromises.opendir(path)),
        (directory) =>
          lock.withPermit(
            Effect.gen(function* () {
              if (yield* Ref.get(closed)) return
              yield* Ref.set(closed, true)
              yield* Effect.promise(() =>
                directory.close().catch((error: unknown) => {
                  if (Serialization.stringProperty(error, 'code') !== 'ERR_DIR_CLOSED') throw error
                }),
              )
            }),
          ),
      )
      const done = yield* Ref.make(false)
      return makeDirReader({
        next: Effect.fnUntraced(
          function* (maxEntries) {
            const isClosed = yield* Ref.get(closed)
            if (isClosed || !Number.isSafeInteger(maxEntries) || maxEntries <= 0)
              return yield* new FileError({
                reason: new FileInvalid({
                  message: isClosed ? 'Directory reader is closed' : 'Invalid page size',
                  path,
                }),
              })
            const entries: Array<FileInfo> = []
            while (entries.length < maxEntries && !(yield* Ref.get(done))) {
              const entry = yield* Effect.uninterruptible(io(path, () => directory.read()))
              if (entry === null) {
                yield* Ref.set(done, true)
                break
              }
              const resolved = Path.join(path, entry.name)
              // Per-entry fresh no-follow metadata tolerates disappearance since the cursor returned its name.
              const stat = yield* io(resolved, () => FsPromises.lstat(resolved)).pipe(
                Effect.catchIf(
                  (error) => error.code === 'not_found',
                  () => Effect.void,
                ),
              )
              if (stat === undefined) continue
              if (!stat.isFile() && !stat.isDirectory() && !stat.isSymbolicLink()) continue
              entries.push(infoUnsafe(resolved, stat))
            }
            return { entries, done: yield* Ref.get(done) }
          },
          (effect) => lock.withPermit(effect),
        ),
      })
    }),
    // Each subscription installs a scoped native producer with its own startup acknowledgement; it cannot be batched.
    watchDirectory: Effect.fnUntraced(function* (path) {
      const installed = yield* Deferred.make<void, FileError>()
      const consumed = yield* Ref.make(false)
      const changes = Stream.callback<string | undefined, FileError>(
        Effect.fnUntraced(
          function* (queue) {
            if (yield* Ref.getAndSet(consumed, true))
              return yield* new FileError({
                reason: new FileInvalid({
                  message: 'Directory notifications already consumed',
                  path,
                }),
              })
            const nativeState = yield* Ref.make({ active: true, closed: false })
            const nativeContext = yield* Effect.context<never>()
            return yield* Effect.acquireRelease(
              Effect.try({
                try: () => {
                  // These Ref operations bridge the actual synchronous native callbacks/release promise.
                  const onChange = (
                    _event: string,
                    filename: string | Buffer | null | undefined,
                  ) => {
                    if (Ref.getUnsafe(nativeState).active)
                      Queue.offerUnsafe(
                        queue,
                        filename === null || filename === undefined
                          ? undefined
                          : Path.join(path, filename.toString()),
                      )
                  }
                  const onError = (error: Error) => {
                    if (Ref.getUnsafe(nativeState).active)
                      Queue.failCauseUnsafe(queue, Cause.fail(fileError(error, path)))
                  }
                  const onClose = () => {
                    Effect.runSyncWith(nativeContext)(
                      Ref.update(nativeState, (value) => ({ ...value, closed: true })),
                    )
                    if (Ref.getUnsafe(nativeState).active) Queue.endUnsafe(queue)
                  }
                  const watcher = Fs.watch(path, { persistent: false }, onChange)
                  watcher.on('error', onError)
                  watcher.on('close', onClose)
                  return {
                    release: () =>
                      new Promise<void>((resolve) => {
                        Effect.runSyncWith(nativeContext)(
                          Ref.update(nativeState, (value) => ({ ...value, active: false })),
                        )
                        watcher.off('change', onChange)
                        if (Ref.getUnsafe(nativeState).closed) {
                          watcher.off('error', onError)
                          watcher.off('close', onClose)
                          resolve()
                        } else {
                          watcher.once('close', () => {
                            watcher.off('error', onError)
                            watcher.off('close', onClose)
                            resolve()
                          })
                          watcher.close()
                        }
                      }),
                  }
                },
                catch: (cause) => fileError(cause, path),
              }),
              (watcher) => Effect.promise(watcher.release),
            )
          },
          (effect, queue) =>
            effect.pipe(
              Effect.onExit((exit) =>
                Effect.gen(function* () {
                  yield* Deferred.done(installed, Exit.asVoid(exit))
                  // Stream.callback forks its producer; its failed Exit must also settle the queue.
                  if (exit._tag === 'Failure') yield* Queue.failCause(queue, exit.cause)
                }),
              ),
            ),
        ),
      ).pipe(Stream.onExit((exit) => Deferred.done(installed, Exit.asVoid(exit))))
      return { changes, started: Deferred.await(installed) }
    }),
  }),
)
/** Host metadata only: cwd/home and path-list syntax are not supplied by Effect Path. */
