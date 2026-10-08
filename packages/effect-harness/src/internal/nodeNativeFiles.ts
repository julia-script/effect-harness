import * as NodeResourceAdapter from './NodeResourceAdapter.ts'
import { dual } from 'effect/Function'
import { constant } from 'effect/Function'
import * as Ref from 'effect/Ref'
import * as Time from '../Time.ts'
import * as NativeError from '../env/NativeError.ts'
import * as Serialization from '../Serialization.ts'
/** Node adapter for native capabilities missing from Effect FileSystem. Portable Env never imports this module. */
import * as fs from 'node:fs'
import type * as promises from 'node:fs/promises'
import * as path from 'node:path'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Stream from 'effect/Stream'
import * as Queue from 'effect/Queue'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import {
  NativeFiles,
  makeBinaryReader,
  makeDirReader,
  type BinaryReader,
  type FileInfo,
} from 'effect-harness/NativeFiles'
import { FileError, FileInvalidError, fileReason } from '../FileError.ts'

import * as LineScan from '../env/LineScan.ts'

/**
 * Maps a caught native filesystem error while preserving its path and cause.
 *
 * @category combinators
 */
function fileErrorImpl(error: unknown, filePath: string): FileError {
  if (error instanceof FileError) return error
  const code = NativeError.codeOrUndefined(error) ?? ''
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
    reason: fileReason(mapped, {
      message: Serialization.errorText(error),
      path: filePath,
      cause: error,
    }),
  })
}
// effect-nit-allow P1-throw-only-in-unsafe-orthrow: unsupported native Stats kinds throw through lstat Effect.try; openDirReader filters unsupported kinds before calling infoUnsafe.
function infoUnsafe(filePath: string, stat: fs.Stats): FileInfo {
  let kind: FileInfo['kind']
  if (stat.isFile()) kind = 'file'
  else if (stat.isDirectory()) kind = 'directory'
  else if (stat.isSymbolicLink()) kind = 'symlink'
  else
    throw new FileError({
      reason: new FileInvalidError({
        message: 'Path is not a supported file kind',
        path: filePath,
      }),
    })
  return {
    name: path.basename(filePath),
    path: filePath,
    kind,
    size: stat.size,
    mtimeMs: Time.fromEpochMillis(stat.mtimeMs),
    identity: JSON.stringify([stat.dev, stat.ino]),
  }
}
const io = <A>(filePath: string, operation: () => Promise<A>): Effect.Effect<A, FileError> =>
  Effect.tryPromise({
    try: operation,
    catch: (cause) => (cause instanceof FileError ? cause : fileError(cause, filePath)),
  })
/**
 * Layer for NodeNativeFiles native capabilities.
 *
 * @category layers
 */
export const layerNative: Layer.Layer<NativeFiles> = Layer.effect(NativeFiles)(
  Effect.gen(function* () {
    const adapter = yield* NodeResourceAdapter.adapter
    return NativeFiles.of({
      // Each no-follow sample observes this path now; read consistency checks need separate before/after observations.
      lstat: (filePath) =>
        io(filePath, () => adapter.lstat(filePath)).pipe(
          Effect.flatMap((stat) =>
            Effect.try({
              try: () => infoUnsafe(filePath, stat),
              catch: (cause) => fileError(cause, filePath),
            }),
          ),
        ),
      // Acquiring a cursor owns a distinct native handle and scope; sharing/deduplicating this acquisition is invalid.
      openBinaryReader: Effect.fnUntraced(function* (filePath, options) {
        const closed = yield* Ref.make(false)
        if (options?.noFollow === true && process.platform === 'win32') {
          const before = yield* io(filePath, () => adapter.lstat(filePath))
          if (before.isSymbolicLink())
            return yield* new FileError({
              reason: new FileInvalidError({
                message: 'Final symlink is forbidden',
                path: filePath,
              }),
            })
        }
        const lock = yield* Semaphore.make(1)
        const release = (file: promises.FileHandle) =>
          lock.withPermit(
            Effect.uninterruptible(
              Effect.gen(function* () {
                if (yield* Ref.get(closed)) return
                yield* Ref.set(closed, true)
                // effect-nit-allow P2-sync-promise-must-not-fail: acquireRelease finalizers have no recoverable error channel; a native close rejection terminates scope closure as a defect.
                yield* Effect.promise(() => file.close())
              }),
            ),
          )
        const file = yield* Effect.acquireRelease(
          io(filePath, () =>
            adapter.open(
              filePath,
              fs.constants.O_RDONLY |
                fs.constants.O_NONBLOCK |
                (options?.noFollow === true ? (fs.constants.O_NOFOLLOW ?? 0) : 0),
            ),
          ),
          release,
        )
        const close = release(file)
        const stat = yield* (
          // Handle metadata is freshly sampled on every use, including after a read or replacement.
          io(filePath, () => file.stat()).pipe(Effect.onError(constant(close)))
        )
        if (!stat.isFile()) {
          yield* close
          return yield* new FileError({
            reason: fileReason(stat.isDirectory() ? 'is_directory' : 'invalid', {
              message: 'Reader requires a regular file',
              path: filePath,
            }),
          })
        }
        const live = <A>(effect: Effect.Effect<A, FileError>): Effect.Effect<A, FileError> =>
          Effect.suspend(() =>
            Ref.getUnsafe(closed)
              ? Effect.fail(
                  new FileError({
                    reason: new FileInvalidError({ message: 'Reader is closed', path: filePath }),
                  }),
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
                reason: new FileInvalidError({ message: 'Invalid byte range', path: filePath }),
              })
            const chunks: Array<Uint8Array> = []
            let count = 0
            while (count < length) {
              const chunk = new Uint8Array(Math.min(length - count, 1024 * 1024))
              const result = yield* Effect.uninterruptible(
                io(filePath, () => file.read(chunk, 0, chunk.length, offset + count)),
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
                  reason: new FileInvalidError({
                    message: 'Read result cannot be allocated',
                    path: filePath,
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
              io(filePath, () => file.stat()).pipe(
                Effect.map((stat) => ({
                  name: path.basename(filePath),
                  path: filePath,
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
                  new FileError({
                    reason: new FileInvalidError({ message: error.message, path: filePath }),
                  }),
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
      openDirReader: Effect.fnUntraced(function* (filePath) {
        const closed = yield* Ref.make(false)
        const lock = yield* Semaphore.make(1)
        const directory = yield* Effect.acquireRelease(
          io(filePath, () => adapter.opendir(filePath)),
          (directory) =>
            lock.withPermit(
              Effect.gen(function* () {
                if (yield* Ref.get(closed)) return
                yield* Ref.set(closed, true)
                // effect-nit-allow P2-sync-promise-must-not-fail: scope cleanup defects on native rejection; ERR_DIR_CLOSED is an idempotent successful close.
                yield* Effect.promise(() =>
                  directory.close().catch((error: unknown) => {
                    if (Serialization.stringPropertyOrUndefined(error, 'code') !== 'ERR_DIR_CLOSED')
                      throw error
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
                  reason: new FileInvalidError({
                    message: isClosed ? 'Directory reader is closed' : 'Invalid page size',
                    path: filePath,
                  }),
                })
              const entries: Array<FileInfo> = []
              while (entries.length < maxEntries && !(yield* Ref.get(done))) {
                const entry = yield* Effect.uninterruptible(io(filePath, () => directory.read()))
                if (entry === null) {
                  yield* Ref.set(done, true)
                  break
                }
                const resolved = path.join(filePath, entry.name)
                // Per-entry fresh no-follow metadata tolerates disappearance since the cursor returned its name.
                const stat = yield* io(resolved, () => adapter.lstat(resolved)).pipe(
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
      watchDirectory: Effect.fnUntraced(function* (filePath) {
        const installed = yield* Deferred.make<void, FileError>()
        const consumed = yield* Ref.make(false)
        const changes = Stream.callback<string | undefined, FileError>(
          Effect.fnUntraced(
            function* (queue) {
              if (yield* Ref.getAndSet(consumed, true))
                return yield* new FileError({
                  reason: new FileInvalidError({
                    message: 'Directory notifications already consumed',
                    path: filePath,
                  }),
                })
              const nativeState = yield* Ref.make({ active: true, closed: false })
              const nativeContext = yield* Effect.context<never>()
              return yield* Effect.acquireRelease(
                Effect.try({
                  try: () => {
                    // These Ref operations bridge the actual synchronous native callbacks/release promise.
                    const onChangeUnsafe = (
                      _event: string,
                      filename: string | Buffer | null | undefined,
                    ) => {
                      if (Ref.getUnsafe(nativeState).active)
                        Queue.offerUnsafe(
                          queue,
                          filename === null || filename === undefined
                            ? undefined
                            : path.join(filePath, filename.toString()),
                        )
                    }
                    const onErrorUnsafe = (error: Error) => {
                      if (Ref.getUnsafe(nativeState).active)
                        Queue.failCauseUnsafe(queue, Cause.fail(fileError(error, filePath)))
                    }
                    const onCloseUnsafe = () => {
                      // effect-nit-allow P3-run-only-at-edges: Node's synchronous close callback must update the captured Ref before inspecting active; no asynchronous work is detached.
                      Effect.runSyncWith(nativeContext)(
                        Ref.update(nativeState, (value) => ({ ...value, closed: true })),
                      )
                      if (Ref.getUnsafe(nativeState).active) Queue.endUnsafe(queue)
                    }
                    const watcher = adapter.watch(filePath, { persistent: false }, onChangeUnsafe)
                    watcher.on('error', onErrorUnsafe)
                    watcher.on('close', onCloseUnsafe)
                    return {
                      release: () =>
                        Effect.callback<void>((resume) => {
                          // effect-nit-allow P3-run-only-at-edges: Native release must synchronously mark the captured Ref inactive before detaching listeners; no asynchronous work is detached.
                          Effect.runSyncWith(nativeContext)(
                            Ref.update(nativeState, (value) => ({ ...value, active: false })),
                          )
                          watcher.off('change', onChangeUnsafe)
                          if (Ref.getUnsafe(nativeState).closed) {
                            watcher.off('error', onErrorUnsafe)
                            watcher.off('close', onCloseUnsafe)
                            resume(Effect.void)
                          } else {
                            watcher.once('close', () => {
                              watcher.off('error', onErrorUnsafe)
                              watcher.off('close', onCloseUnsafe)
                              resume(Effect.void)
                            })
                            watcher.close()
                          }
                        }),
                    }
                  },
                  catch: (cause) => fileError(cause, filePath),
                }),
                (watcher) => watcher.release(),
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
    })
  }),
)
/** Host metadata only: cwd/home and path-list syntax are not supplied by Effect Path. */

/** Maps native filesystem errors while retaining the original caught cause.
 * @category combinators
 */
export const fileError: {
  (filePath: string): (self: unknown) => FileError
  (self: unknown, filePath: string): FileError
} = dual(2, fileErrorImpl)
