import * as Serialization from '../Serialization.ts'
/** Node adapter for native capabilities missing from Effect FileSystem. Portable Env never imports this module. */
import * as Fs from 'node:fs'
import * as Fsp from 'node:fs/promises'
import * as Os from 'node:os'
import * as NodePath from 'node:path'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import * as NodeServices from '@effect/platform-node/NodeServices'
import {
  FileError,
  ExecutionError,
  type ShellConfiguration,
  type WatchTarget,
  NativeFiles,
  type BinaryReader,
  type FileInfo,
  type Options,
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
      const file = yield* Effect.acquireRelease(
        io(path, () =>
          Fsp.open(
            path,
            Fs.constants.O_RDONLY |
              Fs.constants.O_NONBLOCK |
              (options?.noFollow === true ? (Fs.constants.O_NOFOLLOW ?? 0) : 0),
          ),
        ),
        (file) =>
          Effect.sync(() => {
            closed = true
          }).pipe(Effect.andThen(io(path, () => file.close())), Effect.orDie),
      )
      const lock = yield* Semaphore.make(1)
      const close = lock.withPermit(
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (closed) return
            closed = true
            yield* io(path, () => file.close()).pipe(Effect.orDie)
          }),
        ),
      )
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
        close,
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
      const directory = yield* Effect.acquireRelease(
        io(path, () => Fsp.opendir(path)),
        (directory) =>
          Effect.sync(() => {
            closed = true
          }).pipe(
            Effect.andThen(
              io(path, () => directory.close()).pipe(
                Effect.catchIf(
                  (error) => error.message.includes('Directory handle was closed'),
                  () => Effect.void,
                ),
              ),
            ),
            Effect.orDie,
          ),
      )
      let done = false
      const lock = yield* Semaphore.make(1)
      const close = lock.withPermit(
        Effect.uninterruptible(
          Effect.gen(function* () {
            if (closed) return
            closed = true
            yield* io(path, () => directory.close()).pipe(Effect.orDie)
          }),
        ),
      )
      return {
        close,
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
    watchDirectory: (path, onChange) =>
      Effect.acquireRelease(
        Effect.try({
          try: () => {
            const watcher = Fs.watch(path, { persistent: false }, (_event, filename) =>
              onChange(filename === null ? undefined : NodePath.join(path, filename.toString())),
            )
            watcher.on('error', (error) => onChange(undefined, fileError(error, path)))
            return watcher
          },
          catch: (cause) => fileError(cause, path),
        }),
        (watcher) => Effect.sync(() => watcher.close()),
      ).pipe(Effect.asVoid),
  }),
)
const resolveShell = (custom?: string): Effect.Effect<ShellConfiguration, ExecutionError> =>
  Effect.gen(function* () {
    const exists = (value: string) =>
      Effect.tryPromise({
        try: () => Fsp.access(value).then(() => true),
        catch: (cause) =>
          new ExecutionError({
            reason: new ExecutionShellUnavailable({ message: `Shell not found: ${value}`, cause }),
          }),
      }).pipe(Effect.orElseSucceed(() => false))
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
    const candidates: string[] = []
    if (process.platform === 'win32') {
      for (const key of ['ProgramFiles', 'ProgramFiles(x86)']) {
        const base = process.env[key]
        if (base !== undefined) candidates.push(NodePath.join(base, 'Git', 'bin', 'bash.exe'))
      }
    } else candidates.push('/bin/bash')
    for (const directory of (process.env['PATH'] ?? '').split(NodePath.delimiter))
      candidates.push(NodePath.join(directory, process.platform === 'win32' ? 'bash.exe' : 'bash'))
    for (const candidate of candidates)
      if (yield* exists(candidate)) return configuration(candidate)
    if (process.platform === 'win32')
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
export const resolveWatchMode = (
  targets: ReadonlyArray<WatchTarget>,
): Effect.Effect<'native' | 'polling'> =>
  Effect.gen(function* () {
    if (process.platform === 'win32') return 'polling'
    if (process.platform !== 'linux' && process.platform !== 'android') return 'native'
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
        const parent = NodePath.dirname(candidate)
        if (parent === candidate) break
        candidate = parent
      }
    }
    return 'native'
  })
/** Own Env plus native services in a Layer; command resources are separately scoped per execution. */
export const layer = (options: Partial<Options> = {}): Layer.Layer<import('../Env.ts').Env> =>
  environmentLayer({
    id: options.id ?? 'node:local',
    cwd: options.cwd ?? process.cwd(),
    home: options.home ?? Os.homedir(),
    resolveShell: options.resolveShell ?? resolveShell(options.shell),
    resolveWatchMode: options.resolveWatchMode ?? resolveWatchMode,
    ...(options.watch === undefined ? {} : { watch: options.watch }),
    ...(options.env === undefined ? {} : { env: options.env }),
  }).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, nativeLayer)))
