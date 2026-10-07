import * as Serialization from '../Serialization.ts'
/** Node adapter for native capabilities missing from Effect FileSystem. Portable Env never imports this module. */
import * as Fs from 'node:fs'
import * as Fsp from 'node:fs/promises'
import * as Os from 'node:os'
import * as NodePath from 'node:path'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Stream from 'effect/Stream'
import * as Queue from 'effect/Queue'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import * as Config from 'effect/Config'
import * as Option from 'effect/Option'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import type { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import {
  FileError,
  ExecutionError,
  type ShellConfiguration,
  type WatchTarget,
  NativeFiles,
  type BinaryReader,
  type FileInfo,
  type Options as EnvOptions,
  layer as environmentLayer,
  ExecutionShellUnavailable,
  FileInvalid,
  fileReason,
} from '../Env.ts'
import * as LineScan from './LineScan.ts'

export function fileError(error: unknown, path: string): FileError {
  if (error instanceof FileError) return error
  const code = Serialization.stringProperty(error, 'code') ?? ''
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
function info(path: string, stat: Fs.Stats): FileInfo {
  let kind: FileInfo['kind']
  if (stat.isFile()) kind = 'file'
  else if (stat.isDirectory()) kind = 'directory'
  else if (stat.isSymbolicLink()) kind = 'symlink'
  else
    throw new FileError({
      reason: new FileInvalid({ message: 'Path is not a supported file kind', path }),
    })
  return {
    name: NodePath.basename(path),
    path,
    kind,
    size: stat.size,
    mtimeMs: stat.mtimeMs,
    identity: JSON.stringify([stat.dev, stat.ino]),
  }
}
const io = <A>(path: string, operation: () => Promise<A>): Effect.Effect<A, FileError> =>
  Effect.tryPromise({
    try: operation,
    catch: (cause) => (cause instanceof FileError ? cause : fileError(cause, path)),
  })
export const nativeLayer: Layer.Layer<NativeFiles> = Layer.succeed(
  NativeFiles,
  NativeFiles.of({
    lstat: (path) =>
      io(path, () => Fsp.lstat(path)).pipe(
        Effect.flatMap((stat) =>
          Effect.try({ try: () => info(path, stat), catch: (cause) => fileError(cause, path) }),
        ),
      ),
    openBinaryReader: Effect.fnUntraced(function* (path, options) {
      let closed = false
      if (options?.noFollow === true && process.platform === 'win32') {
        const before = yield* io(path, () => Fsp.lstat(path))
        if (before.isSymbolicLink())
          return yield* new FileError({
            reason: new FileInvalid({ message: 'Final symlink is forbidden', path }),
          })
      }
      const lock = yield* Semaphore.make(1)
      const release = (file: Fsp.FileHandle) =>
        lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (closed) return
              closed = true
              yield* Effect.promise(() => file.close())
            }),
          ),
        )
      const file = yield* Effect.acquireRelease(
        io(path, () =>
          Fsp.open(
            path,
            Fs.constants.O_RDONLY |
              Fs.constants.O_NONBLOCK |
              (options?.noFollow === true ? (Fs.constants.O_NOFOLLOW ?? 0) : 0),
          ),
        ),
        release,
      )
      const close = release(file)
      const stat = yield* io(path, () => file.stat()).pipe(Effect.onError(() => close))
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
          closed
            ? Effect.fail(
                new FileError({ reason: new FileInvalid({ message: 'Reader is closed', path }) }),
              )
            : effect,
        )
      const read = (offset: number, length: number) =>
        lock.withPermit(
          live(
            Effect.gen(function* () {
              if (
                !Number.isSafeInteger(offset) ||
                offset < 0 ||
                !Number.isSafeInteger(length) ||
                length < 0
              )
                return yield* new FileError({
                  reason: new FileInvalid({ message: 'Invalid byte range', path }),
                })
              const chunks: Uint8Array[] = []
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
            }),
          ),
        )
      const reader: BinaryReader = {
        info: lock.withPermit(
          live(
            io(path, () => file.stat()).pipe(
              Effect.map((stat) => ({
                name: NodePath.basename(path),
                path,
                kind: 'file' as const,
                size: stat.size,
                mtimeMs: stat.mtimeMs,
              })),
            ),
          ),
        ),
        read,
        scanLines: (options) =>
          Effect.gen(function* () {
            const scanner = yield* Effect.fromResult(
              LineScan.make(options.startLine, options.endLine),
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
              LineScan.push(scanner, bytes)
            }
            return LineScan.finish(scanner)
          }),
      }
      return reader
    }),
    openDirReader: Effect.fnUntraced(function* (path) {
      let closed = false
      const lock = yield* Semaphore.make(1)
      const directory = yield* Effect.acquireRelease(
        io(path, () => Fsp.opendir(path)),
        (directory) =>
          lock.withPermit(
            Effect.gen(function* () {
              if (closed) return
              closed = true
              yield* Effect.promise(() =>
                directory.close().catch((error: unknown) => {
                  if (Serialization.stringProperty(error, 'code') !== 'ERR_DIR_CLOSED') throw error
                }),
              )
            }),
          ),
      )
      let done = false
      return {
        next: (maxEntries) =>
          lock.withPermit(
            Effect.gen(function* () {
              if (closed || !Number.isSafeInteger(maxEntries) || maxEntries <= 0)
                return yield* new FileError({
                  reason: new FileInvalid({
                    message: closed ? 'Directory reader is closed' : 'Invalid page size',
                    path,
                  }),
                })
              const entries: FileInfo[] = []
              while (entries.length < maxEntries && !done) {
                const entry = yield* Effect.uninterruptible(io(path, () => directory.read()))
                if (entry === null) {
                  done = true
                  break
                }
                const resolved = NodePath.join(path, entry.name)
                const stat = yield* io(resolved, () => Fsp.lstat(resolved)).pipe(
                  Effect.catchIf(
                    (error) => error.code === 'not_found',
                    () => Effect.void,
                  ),
                )
                if (stat === undefined) continue
                if (!stat.isFile() && !stat.isDirectory() && !stat.isSymbolicLink()) continue
                entries.push(info(resolved, stat))
              }
              return { entries, done }
            }),
          ),
      }
    }),
    watchDirectory: Effect.fnUntraced(function* (path) {
      const installed = yield* Deferred.make<void, FileError>()
      let consumed = false
      const changes = Stream.callback<string | undefined, FileError>((queue) =>
        Effect.gen(function* () {
          if (consumed)
            return yield* new FileError({
              reason: new FileInvalid({
                message: 'Directory notifications already consumed',
                path,
              }),
            })
          consumed = true
          return yield* Effect.acquireRelease(
            Effect.try({
              try: () => {
                let active = true
                let nativeClosed = false
                const onChange = (_event: string, filename: string | Buffer | null | undefined) => {
                  if (active)
                    Queue.offerUnsafe(
                      queue,
                      filename === null || filename === undefined
                        ? undefined
                        : NodePath.join(path, filename.toString()),
                    )
                }
                const onError = (error: Error) => {
                  if (active) Queue.failCauseUnsafe(queue, Cause.fail(fileError(error, path)))
                }
                const onClose = () => {
                  nativeClosed = true
                  if (active) Queue.endUnsafe(queue)
                }
                const watcher = Fs.watch(path, { persistent: false }, onChange)
                watcher.on('error', onError)
                watcher.on('close', onClose)
                return {
                  release: () =>
                    new Promise<void>((resolve) => {
                      active = false
                      watcher.off('change', onChange)
                      if (nativeClosed) {
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
        }).pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              yield* Deferred.done(installed, Exit.asVoid(exit))
              // Stream.callback forks its producer; its failed Exit must also settle the queue.
              if (exit._tag === 'Failure') yield* Queue.failCause(queue, exit.cause)
            }),
          ),
        ),
      ).pipe(Stream.onExit((exit) => Deferred.done(installed, Exit.asVoid(exit))))
      return { changes, started: Deferred.await(installed) }
    }),
  }),
)
/** Host metadata only: cwd/home and path-list syntax are not supplied by Effect Path. */
export interface Host {
  readonly platform: string
  readonly cwd: string
  readonly home: string
  readonly searchPathDelimiter: string
}
export interface Options extends Partial<EnvOptions> {
  readonly host?: Host | undefined
}
/** Narrow Node host adapter, evaluated at Layer construction rather than factory declaration.
 * Shell environment is supplied by ConfigProvider; this adapter reads only runtime identity,
 * cwd/home and the host search-path delimiter absent from stable Effect Path.
 */
export const hostDefaults: Effect.Effect<Host> = Effect.sync(() => ({
  platform: process.platform,
  cwd: process.cwd(),
  home: Os.homedir(),
  searchPathDelimiter: NodePath.delimiter,
}))
const resolveShell = (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  host: Host,
  custom?: string,
): Effect.Effect<ShellConfiguration, ExecutionError> =>
  Effect.gen(function* () {
    const exists = (value: string) =>
      fs.access(value).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      )
    const configuration = (program: string): ShellConfiguration => {
      const normalized = program.replace(/\//g, '\\').toLowerCase()
      if (/^[a-z]:\\windows\\(?:system32|sysnative)\\bash\.exe$/.test(normalized))
        return { program, args: ['-s'], commandOnStdin: true }
      return { program, args: ['-c'] }
    }
    if (custom !== undefined) {
      if (yield* exists(custom)) return configuration(custom)
      return yield* new ExecutionError({
        reason: new ExecutionShellUnavailable({
          message: `Custom shell path not found: ${custom}`,
        }),
      })
    }
    const configFailure = (cause: Config.ConfigError) =>
      new ExecutionError({
        reason: new ExecutionShellUnavailable({
          message: `Shell discovery configuration failed: ${cause.message}`,
          cause,
        }),
      })
    const candidates: string[] = []
    if (host.platform === 'win32') {
      for (const key of ['ProgramFiles', 'ProgramFiles(x86)']) {
        const base = yield* Config.option(Config.String(key)).pipe(Effect.mapError(configFailure))
        if (Option.isSome(base)) candidates.push(path.join(base.value, 'Git', 'bin', 'bash.exe'))
      }
    } else candidates.push('/bin/bash')
    const searchPath = yield* Config.String('PATH').pipe(
      Config.withDefault(''),
      Effect.mapError(configFailure),
    )
    for (const directory of searchPath.split(host.searchPathDelimiter))
      candidates.push(path.join(directory, host.platform === 'win32' ? 'bash.exe' : 'bash'))
    for (const candidate of candidates)
      if (yield* exists(candidate)) return configuration(candidate)
    if (host.platform === 'win32')
      return yield* new ExecutionError({
        reason: new ExecutionShellUnavailable({
          message: 'No Bash shell is available; install Git Bash or configure a shell path',
        }),
      })
    return { program: 'sh', args: ['-c'] }
  })
const unreliableFileSystems = new Set([
  0x6969, 0x517b, 0xff534d42, 0xfe534d42, 0x65735546, 0x01021997, 0x0bd00bd0, 0x47504653,
  0x00c36400, 0x5346414f, 0x6b414653, 0x5dca2df5,
])
/** Windows and Linux network/FUSE mounts require polling to see remote changes. */
const watchMode = (
  path: Path.Path,
  platform: string,
  targets: ReadonlyArray<WatchTarget>,
): Effect.Effect<'native' | 'polling'> =>
  Effect.gen(function* () {
    if (platform === 'win32') return 'polling'
    if (platform !== 'linux' && platform !== 'android') return 'native'
    for (const target of targets) {
      let candidate = target.path
      while (true) {
        const stat = yield* Effect.tryPromise({
          try: () => Fsp.statfs(candidate),
          catch: () => undefined,
        }).pipe(Effect.orElseSucceed(() => undefined))
        if (stat !== undefined) {
          if (unreliableFileSystems.has(stat.type)) return 'polling'
          break
        }
        const parent = path.dirname(candidate)
        if (parent === candidate) break
        candidate = parent
      }
    }
    return 'native'
  })
/** Native statfs policy accessor; normal parent traversal requires the supplied Path service. */
export const resolveWatchMode = (
  targets: ReadonlyArray<WatchTarget>,
): Effect.Effect<'native' | 'polling', never, Path.Path> =>
  Effect.gen(function* () {
    const path = yield* Path.Path
    const host = yield* hostDefaults
    return yield* watchMode(path, host.platform, targets)
  })
/** Constructs Env with caller-supplied FileSystem, Path and ChildProcessSpawner.
 * Only the narrow NativeFiles adapter is provided here; platform services belong at the application edge.
 */
export const layer = (
  options: Options = {},
): Layer.Layer<
  import('../Env.ts').Env,
  never,
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const host = options.host ?? (yield* hostDefaults)
      return environmentLayer({
        id: options.id ?? 'node:local',
        cwd: options.cwd ?? host.cwd,
        home: options.home ?? host.home,
        resolveShell: options.resolveShell ?? resolveShell(fs, path, host, options.shell),
        resolveWatchMode:
          options.resolveWatchMode ?? ((targets) => watchMode(path, host.platform, targets)),
        ...(options.watch === undefined ? {} : { watch: options.watch }),
        ...(options.env === undefined ? {} : { env: options.env }),
      })
    }),
  ).pipe(Layer.provide(nativeLayer))
export const layerConfig = (
  config: Config.Wrap<Options>,
): Layer.Layer<
  import('../Env.ts').Env,
  Config.ConfigError,
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner
> => Layer.unwrap(Config.unwrap(config).pipe(Effect.map(layer)))
