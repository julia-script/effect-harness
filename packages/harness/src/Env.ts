import * as Serialization from './Serialization.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import type * as Stream from 'effect/Stream'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import * as Decode from './env/Decode.ts'
import * as AtomicWrite from './env/AtomicWrite.ts'
import * as Exec from './env/Exec.ts'
import * as Watch from './env/Watch.ts'

import { FileError, ExecutionError, FileInvalid, fileReason } from './env/Error.ts'
export * from './env/Error.ts'

export interface FileInfo {
  readonly name: string
  readonly path: string
  readonly kind: 'file' | 'directory' | 'symlink'
  readonly size: number
  readonly mtimeMs: number
  /** Native identity used by watch snapshots; adapters without stable identities may omit it. */
  readonly identity?: string | undefined
}
export interface LineScan {
  readonly newlines: number
  readonly start: number
  readonly end: number
  readonly firstLineEnd: number
  readonly lastLineStart: number
  readonly selectedBytes: number
  readonly firstLineBytes: number
}
export interface BinaryReader {
  readonly info: Effect.Effect<FileInfo, FileError>
  readonly read: (offset: number, length: number) => Effect.Effect<Uint8Array, FileError>
  readonly scanLines: (options: {
    readonly startLine: number
    readonly endLine?: number | undefined
  }) => Effect.Effect<LineScan, FileError>
}
export interface TextLine {
  readonly text: string
  readonly terminated: boolean
}
export interface TextLineReader {
  readonly readLine: Effect.Effect<TextLine | undefined, FileError>
}
export interface DirReader {
  readonly next: (
    maxEntries: number,
  ) => Effect.Effect<
    { readonly entries: ReadonlyArray<FileInfo>; readonly done: boolean },
    FileError
  >
}
export interface WatchTarget {
  readonly path: string
  readonly recursive?: boolean | undefined
  readonly exclude?:
    | { readonly hidden?: boolean | undefined; readonly names?: ReadonlyArray<string> | undefined }
    | undefined
}
export type WatchChange =
  | { readonly paths: ReadonlyArray<string> }
  | { readonly overflow: true }
  | { readonly error: FileError }
export interface Watcher {
  readonly mode: 'native' | 'polling'
  readonly changes: Stream.Stream<WatchChange>
}
export interface WatchOptions {
  readonly mode?: 'native' | 'polling' | undefined
  readonly pollIntervalMs?: number | undefined
  readonly directoryBudget?: number | undefined
}
/** Missing native FileSystem capabilities; a remote or platform Layer implements this narrow boundary. */
export class NativeFiles extends Context.Service<
  NativeFiles,
  {
    readonly lstat: (path: string) => Effect.Effect<FileInfo, FileError>
    readonly openBinaryReader: (
      path: string,
      options?: { readonly noFollow?: boolean | undefined },
    ) => Effect.Effect<BinaryReader, FileError, Scope.Scope>
    readonly openDirReader: (path: string) => Effect.Effect<DirReader, FileError, Scope.Scope>
    readonly watchDirectory: (
      path: string,
      onChange: (path: string | undefined, error?: FileError) => void,
    ) => Effect.Effect<void, FileError, Scope.Scope>
  }
>()('@effect-harness/harness/Env/NativeFiles') {}
export interface ShellOutputInfo {
  readonly stream: 'stdout' | 'stderr'
  readonly skipped?:
    | { readonly bytes: number; readonly newlines: number; readonly endsWithNewline: boolean }
    | undefined
}
export interface ShellOutputWindow {
  readonly maxBytes: number
  readonly maxLines: number
  readonly minIntervalMs: number
  readonly bytesPerSecond: number
}
export interface ShellExecOptions {
  readonly cwd?: string | undefined
  readonly env?: Readonly<Record<string, string>> | undefined
  readonly inheritEnv?: boolean | undefined
  readonly timeout?: number | undefined
  readonly onSpill?: ((path: string) => Effect.Effect<void, ExecutionError>) | undefined
  readonly onOutput?:
    | ((text: string, info: ShellOutputInfo) => Effect.Effect<void, ExecutionError>)
    | undefined
  readonly spill?: { readonly afterBytes: number; readonly afterLines: number } | undefined
  readonly window?: ShellOutputWindow | undefined
}
export interface ShellExecResult {
  readonly exitCode: number
  readonly spillPath?: string | undefined
}
export interface ShellConfiguration {
  readonly program: string
  readonly args: ReadonlyArray<string>
  readonly commandOnStdin?: boolean | undefined
}
export interface Options {
  readonly id: string
  readonly cwd: string
  readonly home?: string | undefined
  readonly resolveShell?: Effect.Effect<ShellConfiguration, ExecutionError> | undefined
  readonly shell?: string | undefined
  readonly watch?: WatchOptions | undefined
  readonly resolveWatchMode?:
    | ((targets: ReadonlyArray<WatchTarget>) => Effect.Effect<'native' | 'polling', FileError>)
    | undefined
  readonly env?: Readonly<Record<string, string>> | undefined
}
export class Env extends Context.Service<
  Env,
  {
    readonly id: string
    readonly cwd: string
    readonly path: Path.Path
    readonly absolutePath: (path: string, cwd?: string) => Effect.Effect<string, FileError>
    readonly joinPath: (parts: ReadonlyArray<string>) => Effect.Effect<string, FileError>
    readonly readTextFile: (path: string) => Effect.Effect<string, FileError>
    readonly readBinaryFile: (path: string) => Effect.Effect<Uint8Array, FileError>
    readonly openBinaryReader: NativeFiles['Service']['openBinaryReader']
    readonly openTextLineReader: (
      path: string,
    ) => Effect.Effect<TextLineReader, FileError, Scope.Scope>
    readonly readTextLines: (
      path: string,
      options?: { readonly maxLines?: number | undefined },
    ) => Effect.Effect<ReadonlyArray<string>, FileError>
    readonly writeFile: (
      path: string,
      content: string | Uint8Array,
    ) => Effect.Effect<void, FileError>
    readonly appendFile: (
      path: string,
      content: string | Uint8Array,
    ) => Effect.Effect<void, FileError>
    readonly truncateFile: (path: string, size: number) => Effect.Effect<void, FileError>
    readonly flushFile: (path: string) => Effect.Effect<void, FileError>
    readonly renameFile: (source: string, destination: string) => Effect.Effect<void, FileError>
    readonly fileInfo: (path: string) => Effect.Effect<FileInfo, FileError>
    readonly listDir: (path: string) => Effect.Effect<ReadonlyArray<FileInfo>, FileError>
    readonly openDirReader: NativeFiles['Service']['openDirReader']
    readonly watch: (
      targets: ReadonlyArray<WatchTarget>,
      options?: WatchOptions,
    ) => Effect.Effect<Watcher, FileError, Scope.Scope>
    readonly canonicalPath: (path: string) => Effect.Effect<string, FileError>
    readonly exists: (path: string) => Effect.Effect<boolean, FileError>
    readonly createDir: (
      path: string,
      options?: { readonly recursive?: boolean | undefined },
    ) => Effect.Effect<void, FileError>
    readonly remove: (
      path: string,
      options?: { readonly recursive?: boolean | undefined; readonly force?: boolean | undefined },
    ) => Effect.Effect<void, FileError>
    readonly createTempDir: (prefix?: string) => Effect.Effect<string, FileError>
    readonly createTempFile: (options?: {
      readonly prefix?: string | undefined
      readonly suffix?: string | undefined
    }) => Effect.Effect<string, FileError>
    readonly exec: (
      command: string | ReadonlyArray<string>,
      options?: ShellExecOptions,
    ) => Effect.Effect<ShellExecResult, ExecutionError>
  }
>()('@effect-harness/harness/Env') {}
export const fromPlatform = (error: PlatformError.PlatformError, path?: string): FileError => {
  let code: FileError['code'] = 'unknown'
  const cause = error.reason.cause
  const nativeCode = Serialization.stringProperty(cause, 'code') ?? ''
  if (nativeCode === 'ABORT_ERR') code = 'aborted'
  else if (error.reason._tag === 'NotFound' || nativeCode === 'ENOENT') code = 'not_found'
  else if (
    error.reason._tag === 'PermissionDenied' ||
    nativeCode === 'EACCES' ||
    nativeCode === 'EPERM'
  )
    code = 'permission_denied'
  else if (nativeCode === 'ENOTDIR') code = 'not_directory'
  else if (nativeCode === 'EISDIR') code = 'is_directory'
  else if (error.reason._tag === 'BadArgument' || nativeCode === 'EINVAL' || nativeCode === 'ELOOP')
    code = 'invalid'
  else if (nativeCode === 'ENOTSUP' || nativeCode === 'ENOSYS') code = 'not_supported'

  return new FileError({
    reason: fileReason(code, {
      message: error.message,
      cause: error,
      ...(path === undefined ? {} : { path }),
    }),
  })
}
export const make = Effect.fnUntraced(function* (options: Options) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const native = yield* NativeFiles
  const spawner = yield* ChildProcessSpawner
  const shell = yield* Exec.make(fs, path, spawner, options)
  const absolutePath = Effect.fnUntraced(function* (value: string, cwd = options.cwd) {
    let input = value
    if (input.startsWith('file:')) {
      const url = yield* Effect.try({
        try: () => new URL(input),
        catch: (cause) =>
          new FileError({ reason: new FileInvalid({ message: 'Invalid URL', cause }) }),
      }).pipe(Effect.option)
      if (Option.isSome(url))
        input = yield* path.fromFileUrl(url.value).pipe(Effect.orElseSucceed(() => input))
    }
    if (
      options.home !== undefined &&
      (input === '~' || input.startsWith('~/') || (path.sep === '\\' && input.startsWith('~\\')))
    )
      input = options.home + input.slice(1)
    return path.resolve(cwd, input)
  })
  const at = <A, E, R>(
    value: string,
    operation: (resolved: string) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | FileError, R> => Effect.flatMap(absolutePath(value), operation)
  const io = <A, R>(
    value: string,
    operation: (resolved: string) => Effect.Effect<A, PlatformError.PlatformError, R>,
  ) =>
    at(value, (resolved) =>
      operation(resolved).pipe(Effect.mapError((error) => fromPlatform(error, resolved))),
    )
  const readBinaryFile = (value: string) => io(value, fs.readFile)
  const openBinaryReader = (
    value: string,
    readerOptions?: { readonly noFollow?: boolean | undefined },
  ) => at(value, (resolved) => native.openBinaryReader(resolved, readerOptions))
  const openDirReader = (value: string) => at(value, native.openDirReader)
  const openTextLineReader = Effect.fnUntraced(function* (value: string) {
    const reader = yield* openBinaryReader(value)
    const lock = yield* Semaphore.make(1)
    const decoder = Decode.make()
    let position = 0
    let pending = ''
    let eof = false
    let closed = false
    const readLine = lock.withPermit(
      Effect.gen(function* () {
        if (closed)
          return yield* new FileError({
            reason: new FileInvalid({ message: 'Reader is closed', path: value }),
          })
        while (true) {
          const index = pending.indexOf('\n')
          if (index >= 0) {
            const text = pending.slice(0, index)
            pending = pending.slice(index + 1)
            return { text, terminated: true }
          }
          if (eof) {
            if (pending === '') return undefined
            const text = pending
            pending = ''
            return { text, terminated: false }
          }
          const bytes = yield* reader.read(position, 65536)
          position += bytes.length
          if (bytes.length === 0) {
            eof = true
            pending += Decode.decode(decoder)
          } else pending += Decode.decode(decoder, bytes)
        }
      }),
    )
    const close = lock.withPermit(
      Effect.sync(() => {
        closed = true
      }),
    )
    yield* Effect.addFinalizer(() => close)
    return { readLine }
  })
  const append = (value: string, content: string | Uint8Array) =>
    io(value, (resolved) =>
      Effect.uninterruptible(
        fs
          .makeDirectory(path.dirname(resolved), { recursive: true })
          .pipe(
            Effect.andThen(
              typeof content === 'string'
                ? fs.writeFileString(resolved, content, { flag: 'a' })
                : fs.writeFile(resolved, content, { flag: 'a' }),
            ),
          ),
      ),
    )
  return Env.of({
    id: options.id,
    cwd: options.cwd,
    path,
    absolutePath,
    joinPath: (parts) => Effect.succeed(path.join(...parts)),
    readBinaryFile,
    readTextFile: (value) =>
      readBinaryFile(value).pipe(
        Effect.map((bytes) => new TextDecoder('utf-8', { ignoreBOM: true }).decode(bytes)),
      ),
    openBinaryReader,
    openDirReader,
    openTextLineReader,
    readTextLines: (value, lineOptions) =>
      Effect.scoped(
        Effect.gen(function* () {
          const max = lineOptions?.maxLines ?? Infinity
          if (max !== Infinity && (!Number.isSafeInteger(max) || max < 0))
            return yield* new FileError({
              reason: new FileInvalid({ message: 'Invalid maxLines', path: value }),
            })
          const reader = yield* openTextLineReader(value)
          const lines: string[] = []
          while (lines.length < max) {
            const line = yield* reader.readLine
            if (line === undefined) break
            lines.push(line.text)
          }
          return lines
        }),
      ),
    writeFile: (value, content) =>
      at(value, (resolved) =>
        Effect.uninterruptible(AtomicWrite.write(fs, path, native, resolved, content)),
      ),
    appendFile: append,
    truncateFile: (value, size) =>
      !Number.isSafeInteger(size) || size < 0
        ? Effect.fail(
            new FileError({
              reason: new FileInvalid({ message: 'Invalid truncate size', path: value }),
            }),
          )
        : io(value, (resolved) =>
            Effect.scoped(
              fs
                .open(resolved, { flag: 'r+' })
                .pipe(Effect.flatMap((file) => Effect.uninterruptible(file.truncate(size)))),
            ),
          ),
    flushFile: (value) =>
      io(value, (resolved) =>
        Effect.scoped(fs.open(resolved, { flag: 'r' }).pipe(Effect.flatMap((file) => file.sync))),
      ),
    renameFile: (source, destination) =>
      Effect.gen(function* () {
        const target = yield* absolutePath(destination)
        yield* io(source, (resolved) => fs.rename(resolved, target))
      }),
    fileInfo: (value) => at(value, native.lstat),
    listDir: (value) =>
      Effect.scoped(
        Effect.gen(function* () {
          const reader = yield* openDirReader(value)
          const entries: FileInfo[] = []
          while (true) {
            const page = yield* reader.next(256)
            entries.push(...page.entries)
            if (page.done) return entries
          }
        }),
      ),
    watch: (targets, watchOptions) =>
      Effect.forEach(targets, (target) =>
        absolutePath(target.path).pipe(Effect.map((resolved) => ({ ...target, path: resolved }))),
      ).pipe(
        Effect.flatMap((resolved) =>
          Effect.gen(function* () {
            const settings = { ...options.watch, ...watchOptions }
            const mode =
              settings.mode ??
              (options.resolveWatchMode === undefined
                ? 'native'
                : yield* options.resolveWatchMode(resolved))
            return yield* Watch.make(fs, path, native, resolved, { ...settings, mode })
          }),
        ),
      ),
    canonicalPath: (value) => io(value, fs.realPath),
    exists: (value) => io(value, fs.exists),
    createDir: (value, dirOptions) =>
      io(value, (resolved) => fs.makeDirectory(resolved, dirOptions)),
    remove: (value, removeOptions) => io(value, (resolved) => fs.remove(resolved, removeOptions)),
    createTempDir: (prefix) =>
      fs
        .makeTempDirectory({ prefix: prefix ?? 'tmp-' })
        .pipe(Effect.mapError((error) => fromPlatform(error))),
    createTempFile: (tempOptions) =>
      Effect.gen(function* () {
        const original = yield* fs
          .makeTempFile({ prefix: 'tmp-', suffix: tempOptions?.suffix })
          .pipe(Effect.mapError((error) => fromPlatform(error)))
        if (tempOptions?.prefix === undefined || tempOptions.prefix === '') return original
        const target = path.join(
          path.dirname(original),
          tempOptions.prefix + path.basename(original),
        )
        yield* fs
          .rename(original, target)
          .pipe(Effect.mapError((error) => fromPlatform(error, target)))
        return target
      }),
    exec: shell.exec,
  })
})
export const layer = (
  options: Options,
): Layer.Layer<Env, never, FileSystem.FileSystem | Path.Path | NativeFiles | ChildProcessSpawner> =>
  Layer.effect(Env, make(options))
