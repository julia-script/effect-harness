/**
 * Portable scoped file, directory, watch and process capabilities.
 */
import { NativeFiles, type FileInfo } from './NativeFiles.ts'
import { constant } from 'effect/Function'
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import { dual, constFalse } from 'effect/Function'
import * as Data from 'effect/Data'
import * as Predicate from 'effect/Predicate'
import * as Ref from 'effect/Ref'
import type * as Duration from 'effect/Duration'
import * as NativeError from './env/NativeError.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import type * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Decode from './env/Decode.ts'
import * as AtomicWrite from './env/AtomicWrite.ts'
import * as exec from './env/internal/exec.ts'
import * as watch from './env/internal/watch.ts'

import { FileError, FileInvalidError, fileReason } from './FileError.ts'
import type { ExecutionError } from './ExecutionError.ts'

/**
 * Decoded line and whether it ended with a line terminator.
 *
 * @category models
 */
export interface TextLine {
  readonly text: string
  readonly terminated: boolean
}
const TextLineReaderTypeId = '~effect-harness/Env/TextLineReader'
/**
 * Scoped sequential reader returning decoded lines until Option.none.
 *
 * **Details**
 *
 * Each line records whether it was terminated. End-of-input is a successful None; read
 * failures remain FileError.
 *
 * **Gotchas**
 *
 * Acquire and consume in the same owning Scope. Do not treat a read failure as end-of-input.
 *
 * @category models
 */
export interface TextLineReader extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TextLineReaderTypeId]: typeof TextLineReaderTypeId
  /**
   * Reads the next decoded line; successful None marks end-of-input, while FileError remains a
   * failure.
   */
  readonly readLine: Effect.Effect<Option.Option<TextLine>, FileError>
}
/**
 * Attaches handle identity without evaluating or changing capability getters.
 *
 * @category constructors
 */
export const makeTextLineReader = (
  input: Omit<
    TextLineReader,
    typeof TextLineReaderTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): TextLineReader => {
  const handle: TextLineReader = Object.create(TextLineReaderProto)
  Object.defineProperties(handle, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(handle, TextLineReaderTypeId, {
    value: TextLineReaderTypeId,
    enumerable: false,
  })
  return handle
}
/**
 * Checks whether a value carries the nominal `TextLineReader` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isTextLineReader = (u: unknown): u is TextLineReader =>
  Predicate.hasProperty(u, TextLineReaderTypeId)

/**
 * Directory path, recursion and exclusions for environment watching.
 *
 * @category models
 */
export interface WatchTarget {
  readonly path: string
  readonly recursive?: boolean | undefined
  readonly exclude?:
    | { readonly hidden?: boolean | undefined; readonly names?: ReadonlyArray<string> | undefined }
    | undefined
}
/**
 * Changed paths, overflow or watcher error delivered by a watch stream.
 *
 * @category models
 */
export type WatchChange = Data.TaggedEnum<{
  Paths: { readonly paths: ReadonlyArray<string> }
  Overflow: {}
  Error: { readonly error: FileError }
}>
/**
 * Constructors and matchers for path changes, coverage overflow and terminal watch failures.
 *
 * @category constants
 */
export const WatchChange = Data.taggedEnum<WatchChange>()
const WatcherTypeId = '~effect-harness/Env/Watcher'
/**
 * Scoped native or polling stream of environment changes.
 *
 * @category models
 */
export interface Watcher extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [WatcherTypeId]: typeof WatcherTypeId
  readonly mode: 'native' | 'polling'
  /**
   * Single-consumer stream of watcher changes owned by the consumption Scope.
   */
  readonly changes: Stream.Stream<WatchChange>
}
/**
 * Attaches handle identity without evaluating or changing capability getters.
 *
 * @category constructors
 */
export const makeWatcher = (
  input: Omit<
    Watcher,
    typeof WatcherTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): Watcher => {
  const handle: Watcher = Object.create(WatcherProto)
  Object.defineProperties(handle, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(handle, WatcherTypeId, { value: WatcherTypeId, enumerable: false })
  return handle
}
/**
 * Checks whether a value carries the nominal `Watcher` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isWatcher = (u: unknown): u is Watcher => Predicate.hasProperty(u, WatcherTypeId)
/**
 * Output stream identity and metadata for skipped bytes.
 *
 * @category models
 */
export interface ShellOutputInfo {
  readonly stream: 'stdout' | 'stderr'
  readonly skipped?:
    | { readonly bytes: number; readonly newlines: number; readonly endsWithNewline: boolean }
    | undefined
}
/**
 * Executable and argument prefix used to launch shell commands.
 *
 * @category models
 */
export interface ShellConfiguration {
  readonly program: string
  readonly args: ReadonlyArray<string>
  readonly commandOnStdin?: boolean | undefined
}
/**
 * Service for bounded file access, shell execution and scoped environment watching.
 *
 * **Details**
 *
 * Paths resolve against configured cwd/home through the supplied Path service. FileSystem,
 * ChildProcessSpawner and NativeFiles remain application-owned capabilities.
 *
 * **Gotchas**
 *
 * An Env is not a filesystem sandbox. The host owns access policy, tool selection and the
 * lifetime of open readers and watchers.
 *
 * @category services
 */
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
      options?: Env.WatchOptions,
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
      options?: Env.ShellExecOptions,
    ) => Effect.Effect<Env.ShellExecResult, ExecutionError>
  }
>()('effect-harness/Env') {}
const fromPlatformImpl = (self: PlatformError.PlatformError, path?: string): FileError => {
  let code: FileError['code'] = 'unknown'
  const cause = self.reason.cause
  const nativeCode = NativeError.codeOrUndefined(cause) ?? ''
  let reasonCode: FileError['code']
  switch (self.reason._tag) {
    case 'NotFound':
      reasonCode = 'not_found'
      break
    case 'PermissionDenied':
      reasonCode = 'permission_denied'
      break
    case 'BadArgument':
      reasonCode = 'invalid'
      break
    case 'AlreadyExists':
    case 'BadResource':
    case 'Busy':
    case 'InvalidData':
    case 'TimedOut':
    case 'UnexpectedEof':
    case 'Unknown':
    case 'WouldBlock':
    case 'WriteZero':
      reasonCode = 'unknown'
      break
  }
  if (nativeCode === 'ABORT_ERR') code = 'aborted'
  else if (reasonCode === 'not_found' || nativeCode === 'ENOENT') code = 'not_found'
  else if (reasonCode === 'permission_denied' || nativeCode === 'EACCES' || nativeCode === 'EPERM')
    code = 'permission_denied'
  else if (nativeCode === 'ENOTDIR') code = 'not_directory'
  else if (nativeCode === 'EISDIR') code = 'is_directory'
  else if (reasonCode === 'invalid' || nativeCode === 'EINVAL' || nativeCode === 'ELOOP')
    code = 'invalid'
  else if (nativeCode === 'ENOTSUP' || nativeCode === 'ENOSYS') code = 'not_supported'

  return new FileError({
    reason: fileReason(code, {
      message: self.message,
      cause: self,
      ...(path === undefined ? {} : { path }),
    }),
  })
}
/**
 * Maps platform reasons and ordered native errno fallbacks to a semantic file failure.
 *
 * @category combinators
 */
export const fromPlatform: {
  (path?: string): (self: PlatformError.PlatformError) => FileError
  (self: PlatformError.PlatformError, path?: string): FileError
} = dual((args) => PlatformError.isPlatformError(args[0]), fromPlatformImpl)
/**
 * Acquires scoped file, watcher and process capabilities from the injected platform services.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  options: Env.Options,
): Effect.fn.Return<
  Env['Service'],
  never,
  | FileSystem.FileSystem
  | Path.Path
  | NativeFiles
  | ChildProcessSpawner.ChildProcessSpawner
  | Scope.Scope
> {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const native = yield* NativeFiles
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const cwd = path.resolve(options.cwd)
  const atomicWrite = yield* AtomicWrite.make
  const openWatcher = yield* watch.acquire
  const shell = yield* exec.make({ fs, path, spawner, defaults: { ...options, cwd } })
  const absolutePath = Effect.fnUntraced(function* (value: string, base = cwd) {
    let input = value
    if (input.startsWith('file:')) {
      const url = yield* Effect.try({
        try: () => new URL(input),
        catch: (cause) =>
          new FileError({ reason: new FileInvalidError({ message: 'Invalid URL', cause }) }),
      }).pipe(Effect.option)
      input = yield* Option.match(url, {
        onNone: () => Effect.succeed(input),
        onSome: (value) => path.fromFileUrl(value).pipe(Effect.orElseSucceed(() => input)),
      })
    }
    if (
      options.home !== undefined &&
      (input === '~' || input.startsWith('~/') || (path.sep === '\\' && input.startsWith('~\\')))
    )
      input = options.home + input.slice(1)
    return path.resolve(cwd, base, input)
  })
  const at = <A, E, R>(
    self: string,
    operation: (resolved: string) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | FileError, R> => Effect.flatMap(absolutePath(self), operation)
  const io = <A, R>(
    self: string,
    operation: (resolved: string) => Effect.Effect<A, PlatformError.PlatformError, R>,
  ) =>
    at(self, (resolved) =>
      operation(resolved).pipe(Effect.mapError((error) => fromPlatform(error, resolved))),
    )
  // P5-request-resolver-batching: each content read is a new observation (including reads before/after a write).
  // The platform has no bulk snapshot API; deduplication would suppress the caller's required fresh sample.
  const readBinaryFile = (self: string) => io(self, fs.readFile)
  // P5-request-resolver-batching: opening returns a scope-owned cursor/handle, never a reusable keyed value.
  // Sharing a resolver result would merge unrelated reader lifetimes and independent metadata samples.
  const openBinaryReader = (
    self: string,
    readerOptions?: { readonly noFollow?: boolean | undefined },
  ) => at(self, (resolved) => native.openBinaryReader(resolved, readerOptions))
  const openDirReader = (self: string) => at(self, native.openDirReader)
  const openTextLineReader = Effect.fnUntraced(function* (value: string) {
    const reader = yield* openBinaryReader(value)
    const lock = yield* Semaphore.make(1)
    const decoder = Decode.make()
    const state = yield* Ref.make({ position: 0, pending: '', eof: false, closed: false })
    const readLine = lock.withPermit(
      Effect.gen(function* () {
        if ((yield* Ref.get(state)).closed)
          return yield* new FileError({
            reason: new FileInvalidError({ message: 'Reader is closed', path: value }),
          })
        while (true) {
          const current = yield* Ref.get(state)
          const index = current.pending.indexOf('\n')
          if (index >= 0) {
            const text = current.pending.slice(0, index)
            yield* Ref.update(state, (value) => ({
              ...value,
              pending: current.pending.slice(index + 1),
            }))
            return Option.some({ text, terminated: true })
          }
          if (current.eof) {
            if (current.pending === '') return Option.none()
            yield* Ref.update(state, (value) => ({ ...value, pending: '' }))
            return Option.some({ text: current.pending, terminated: false })
          }
          const bytes = yield* reader.read(current.position, 65536)
          const decoded = yield* bytes.length === 0
            ? Decode.decode(decoder)
            : Decode.decode(decoder, bytes)
          yield* Ref.update(state, (value) => ({
            ...value,
            position: current.position + bytes.length,
            eof: bytes.length === 0,
            pending: current.pending + decoded,
          }))
        }
      }),
    )
    const close = lock.withPermit(Ref.update(state, (value) => ({ ...value, closed: true })))
    yield* Effect.addFinalizer(constant(close))
    return makeTextLineReader({ readLine })
  })
  const append = (self: string, content: string | Uint8Array) =>
    io(self, (resolved) =>
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
    cwd,
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
    readTextLines: Effect.fnUntraced(function* (value, lineOptions) {
      const max = lineOptions?.maxLines ?? Infinity
      if (max !== Infinity && (!Number.isSafeInteger(max) || max < 0))
        return yield* new FileError({
          reason: new FileInvalidError({ message: 'Invalid maxLines', path: value }),
        })
      const reader = yield* openTextLineReader(value)
      const lines: Array<string> = []
      while (lines.length < max) {
        const line = yield* reader.readLine
        const more = Option.match(line, {
          onNone: constFalse,
          onSome: (self) => {
            lines.push(self.text)
            return true
          },
        })
        if (!more) break
      }
      return lines
    }, Effect.scoped),
    writeFile: (value, content) =>
      at(value, (resolved) => Effect.uninterruptible(atomicWrite(resolved, content))),
    appendFile: append,
    truncateFile: (value, size) =>
      !Number.isSafeInteger(size) || size < 0
        ? Effect.fail(
            new FileError({
              reason: new FileInvalidError({ message: 'Invalid truncate size', path: value }),
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
    renameFile: Effect.fnUntraced(function* (source, destination) {
      const target = yield* absolutePath(destination)
      yield* io(source, (resolved) => fs.rename(resolved, target))
    }),
    // P5-request-resolver-batching: lstat is a fresh no-follow observation; following platform stat is a
    // separate sample with different link semantics, so before/after callers must never be deduplicated.
    fileInfo: (value) => at(value, native.lstat),
    // P5-request-resolver-batching: directory pages consume a specific acquired cursor; no bulk atomic
    // metadata snapshot exists, and deduplication would mix cursor positions or conceal newly added files.
    listDir: Effect.fnUntraced(function* (value) {
      const reader = yield* openDirReader(value)
      const entries: Array<FileInfo> = []
      while (true) {
        const page = yield* reader.next(256)
        entries.push(...page.entries)
        if (page.done) return entries
      }
    }, Effect.scoped),
    watch: (targets, watchOptions) =>
      Effect.forEach(targets, (target) =>
        absolutePath(target.path).pipe(Effect.map((resolved) => ({ ...target, path: resolved }))),
      ).pipe(
        Effect.flatMap(
          Effect.fnUntraced(function* (resolved) {
            const settings = { ...options.watch, ...watchOptions }
            const mode =
              settings.mode ??
              (options.resolveWatchMode === undefined
                ? 'native'
                : yield* options.resolveWatchMode(resolved))
            return yield* openWatcher(resolved, { ...settings, mode })
          }),
        ),
      ),
    // P5-request-resolver-batching: mutation admission resolves current aliases after previous replacements.
    // A cached/deduplicated realPath could lock a stale destination; the platform offers no atomic bulk resolve.
    canonicalPath: (value) => io(value, fs.realPath),
    // Existence is sampled at this operation; sharing a keyed request across mutations would conceal creation/removal.
    exists: (value) => io(value, fs.exists),
    createDir: (value, dirOptions) =>
      io(value, (resolved) => fs.makeDirectory(resolved, dirOptions)),
    remove: (value, removeOptions) => io(value, (resolved) => fs.remove(resolved, removeOptions)),
    createTempDir: (prefix) =>
      fs
        .makeTempDirectory({ prefix: prefix ?? 'tmp-' })
        .pipe(Effect.mapError((error) => fromPlatform(error))),
    createTempFile: Effect.fnUntraced(function* (tempOptions) {
      const original = yield* fs
        .makeTempFile({ prefix: 'tmp-', suffix: tempOptions?.suffix })
        .pipe(Effect.mapError((error) => fromPlatform(error)))
      if (tempOptions?.prefix === undefined || tempOptions.prefix === '') return original
      const target = path.join(path.dirname(original), tempOptions.prefix + path.basename(original))
      yield* fs
        .rename(original, target)
        .pipe(Effect.mapError((error) => fromPlatform(error, target)))
      return target
    }),
    exec: shell.exec,
  })
})
/**
 * Provides an Env from the caller’s native filesystem, path, process and file capabilities.
 *
 * **Details**
 *
 * Normalizes cwd/home and shell/watch configuration for bounded operations. Readers,
 * watchers and subprocesses remain scoped.
 *
 * @category layers
 */
export const layer = (
  options: Env.Options,
): Layer.Layer<
  Env,
  never,
  FileSystem.FileSystem | Path.Path | NativeFiles | ChildProcessSpawner.ChildProcessSpawner
> => Layer.effect(Env, make(options))

const TextLineReaderProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/Env/TextLineReader' }
  },
}

const WatcherProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/Env/Watcher' }
  },
}

/**
 * Type-level contracts for `Env`.
 *
 */
export declare namespace Env {
  /**
   * Backend selection and timing policy for environment watching.
   *
   * @category models
   */
  interface WatchOptions {
    readonly mode?: 'native' | 'polling' | undefined
    readonly pollInterval?: Duration.Input | undefined
    readonly directoryBudget?: number | undefined
  }
  /**
   * Retained stdout/stderr window with bounds and omission metadata.
   *
   * @category models
   */
  type ShellOutputWindow = {
    readonly maxBytes: number
    readonly maxLines: number
    readonly bytesPerSecond: number
    readonly minInterval: Duration.Input
  }
  /**
   * Command environment, timeout and output-reporting options.
   *
   * @category models
   */
  interface ShellExecOptions {
    readonly cwd?: string | undefined
    readonly env?: Readonly<Record<string, string>> | undefined
    readonly inheritEnv?: boolean | undefined
    readonly timeout?: Duration.Input | undefined
    readonly onSpill?: ((path: string) => Effect.Effect<void, ExecutionError>) | undefined
    readonly onOutput?:
      | ((text: string, info: ShellOutputInfo) => Effect.Effect<void, ExecutionError>)
      | undefined
    readonly spill?: { readonly afterBytes: number; readonly afterLines: number } | undefined
    readonly window?: ShellOutputWindow | undefined
  }
  /**
   * Exit status and bounded output from a completed shell command.
   *
   * @category models
   */
  interface ShellExecResult {
    readonly exitCode: number
    readonly spillPath?: string | undefined
  }
  /**
   * Configuration accepted by Env.
   *
   * @category models
   */
  interface Options {
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
}

/** Streams decoded text lines with a fresh scoped reader for every subscription.
 * @category combinators
 */
export const streamTextLines = (self: string): Stream.Stream<TextLine, FileError, Env> =>
  Stream.unwrap(
    Effect.flatMap(Env, (env) =>
      Effect.map(env.openTextLineReader(self), (reader) =>
        Stream.paginate(undefined, () =>
          reader.readLine.pipe(
            Effect.map((line) =>
              Option.match(line, {
                onNone: () => [[], Option.none<void>()] as const,
                onSome: (value) => [[value], Option.some(undefined)] as const,
              }),
            ),
          ),
        ),
      ),
    ),
  )
/** Streams directory metadata in bounded pages with a fresh reader per subscription.
 * @category combinators
 */
export const streamDirEntries = (
  self: string,
  options?: { readonly pageSize?: number | undefined },
): Stream.Stream<FileInfo, FileError, Env> =>
  Stream.unwrap(
    Effect.flatMap(Env, (env) =>
      Effect.map(env.openDirReader(self), (reader) =>
        Stream.paginate(undefined, () =>
          reader
            .next(options?.pageSize ?? 64)
            .pipe(
              Effect.map(
                (page) =>
                  [page.entries, page.done ? Option.none<void>() : Option.some(undefined)] as const,
              ),
            ),
        ),
      ),
    ),
  )
