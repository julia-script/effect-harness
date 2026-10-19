import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import type * as ToolkitFileSystem from './ToolkitFileSystem.js'
import { binding, validPath } from './internal/DirectoryWatchBinding.js'

export class DirectoryReaderError extends Schema.TaggedError<DirectoryReaderError>()(
  'DirectoryReaderError',
  {
    reason: Schema.Literals([
      'invalid',
      'permission',
      'notFound',
      'notDirectory',
      'closed',
      'unsupported',
      'backend',
    ]),
    operation: Schema.String,
    path: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Maximum returned entries per page, independent of directory cardinality. */
export const maxPageEntries = 1024

export interface Entry {
  readonly name: string
  /** Enumeration metadata, not a later stat through a possibly replaced pathname. */
  readonly kind: 'file' | 'directory' | 'symlink' | 'other' | 'unknown'
}

export interface Capabilities {
  readonly identity: 'opened-directory' | 'names-snapshot'
  readonly allocation: 'native-buffer' | 'whole-directory-array'
  /** Native buffers/OS allocations are opaque; this is not a byte-memory guarantee. */
  readonly bufferEntries?: number
  /** Concurrent mutation can return buffered entries that have since been removed. */
  readonly membership: 'enumeration-time'
}

/** One adapter-owned handle. Its acquisition Scope releases it even on failed opens. */
export interface Handle {
  readonly capabilities: Capabilities
  readonly read: Effect.Effect<Option.Option<Entry>, DirectoryReaderError>
  /** Idempotent; use the same guarded release in the acquisition Scope. */
  readonly close: Effect.Effect<void, DirectoryReaderError>
}

/** Custom/remote adapters accept absolute paths in the supplied Path service's namespace. */
export class Adapter extends Context.Service<
  Adapter,
  {
    readonly capabilities: Capabilities
    readonly open: (path: string) => Effect.Effect<Handle, DirectoryReaderError, Scope.Scope>
  }
>()('effect-harness/DirectoryReader/Adapter') {}

export interface Reader {
  readonly path: string
  readonly capabilities: Capabilities
  readonly next: (
    maxEntries: number,
  ) => Effect.Effect<
    { readonly entries: ReadonlyArray<Entry>; readonly done: boolean },
    DirectoryReaderError
  >
  readonly close: Effect.Effect<void, DirectoryReaderError>
}

export interface OpenOptions {
  /** Refuse snapshot identity before enumeration. */
  readonly requireOpenedIdentity?: boolean
  /** Refuse whole-array enumeration; native/OS buffers remain opaque, without a byte-memory guarantee. */
  readonly requireBoundedEnumeration?: boolean
}

export class DirectoryReader extends Context.Service<
  DirectoryReader,
  {
    readonly open: (
      path: string,
      options?: OpenOptions,
    ) => Effect.Effect<Reader, DirectoryReaderError, Scope.Scope>
  }
>()('effect-harness/DirectoryReader') {}

/** Bind this narrow service alongside ToolkitFileSystem using the same root/backend. */
export const layer = (
  options: ToolkitFileSystem.Options,
): Layer.Layer<DirectoryReader, DirectoryReaderError, Adapter | Path.Path> => {
  const config = { ...options }
  return Layer.effect(
    DirectoryReader,
    Effect.gen(function* () {
      const path = yield* Path.Path
      const adapter = yield* Adapter
      if (!validPath(config.root) || !path.isAbsolute(config.root))
        return yield* new DirectoryReaderError({
          reason: 'invalid',
          operation: 'bind',
          path: config.root,
          message: 'root must be an absolute path without NUL',
        })
      const bound = binding(path, config)
      return {
        open: Effect.fn('DirectoryReader.open')(function* (
          value: string,
          requirements?: OpenOptions,
        ) {
          const fail = (
            reason: DirectoryReaderError['reason'],
            operation: string,
            message: string,
          ) => new DirectoryReaderError({ reason, operation, path: value, message })
          if (!validPath(value))
            return yield* fail('invalid', 'open', 'path must be nonempty without NUL')
          const resolved = bound.resolve(value)
          if (!resolved.allowed)
            return yield* fail('permission', 'open', 'path is outside the bound root')
          if (
            requirements?.requireOpenedIdentity &&
            adapter.capabilities.identity !== 'opened-directory'
          )
            return yield* fail(
              'unsupported',
              'open',
              'adapter cannot preserve opened directory identity',
            )
          if (
            requirements?.requireBoundedEnumeration &&
            adapter.capabilities.allocation !== 'native-buffer'
          )
            return yield* fail(
              'unsupported',
              'open',
              'adapter enumerates the complete directory in host memory',
            )
          // A child scope also makes explicit close release adapter-owned resources.
          const scope = yield* Scope.fork(yield* Scope.Scope)
          const handle = yield* adapter.open(resolved.resolved).pipe(
            Effect.provideService(Scope.Scope, scope),
            Effect.onError(() => Scope.close(scope, Exit.void)),
          )
          const mutex = yield* Semaphore.make(1)
          let closed = false
          let done = false
          const close = mutex
            .withPermit(
              Effect.suspend(() => {
                if (closed) return Effect.void
                closed = true
                return Effect.ensuring(handle.close, Scope.close(scope, Exit.void))
              }),
            )
            .pipe(Effect.uninterruptible)
          yield* Effect.addFinalizer(() => Effect.orDie(close))
          if (
            requirements?.requireOpenedIdentity &&
            handle.capabilities.identity !== 'opened-directory'
          ) {
            yield* close
            return yield* fail(
              'unsupported',
              'open',
              'adapter cannot preserve opened directory identity',
            )
          }
          if (
            requirements?.requireBoundedEnumeration &&
            handle.capabilities.allocation !== 'native-buffer'
          ) {
            yield* close
            return yield* fail(
              'unsupported',
              'open',
              'adapter enumerates the complete directory in host memory',
            )
          }
          return {
            path: resolved.resolved,
            capabilities: handle.capabilities,
            close,
            next: Effect.fn('DirectoryReader.next')(function* (count: number) {
              if (!Number.isSafeInteger(count) || count <= 0 || count > maxPageEntries)
                return yield* fail(
                  'invalid',
                  'next',
                  `maxEntries must be between 1 and ${maxPageEntries}`,
                )
              return yield* mutex.withPermit(
                Effect.gen(function* () {
                  if (closed) return yield* fail('closed', 'next', 'directory reader is closed')
                  const entries: Array<Entry> = []
                  while (!done && entries.length < count) {
                    const entry = yield* handle.read
                    if (Option.isNone(entry)) done = true
                    else entries.push(entry.value)
                  }
                  return { entries, done }
                }),
              )
            }),
          }
        }),
      }
    }),
  )
}

/** Explicit portable fallback: bounded output slices, unbounded host enumeration, snapshot names. */
export const fileSystemAdapter: Layer.Layer<Adapter, never, FileSystem.FileSystem> = Layer.effect(
  Adapter,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    return {
      capabilities: {
        identity: 'names-snapshot',
        allocation: 'whole-directory-array',
        membership: 'enumeration-time',
      } as const,
      open: Effect.fn('DirectoryReader.fileSystem.open')(function* (path: string) {
        const names = yield* fs.readDirectory(path).pipe(
          Effect.mapError(
            (cause) =>
              new DirectoryReaderError({
                reason: 'backend',
                operation: 'open',
                path,
                message: cause.message,
                cause,
              }),
          ),
        )
        let index = 0
        let closed = false
        const close = Effect.sync(() => {
          closed = true
        })
        yield* Effect.addFinalizer(() => close)
        return {
          capabilities: {
            identity: 'names-snapshot',
            allocation: 'whole-directory-array',
            membership: 'enumeration-time',
          } as const,
          close,
          read: Effect.suspend(() => {
            if (closed)
              return Effect.fail(
                new DirectoryReaderError({
                  reason: 'closed',
                  operation: 'read',
                  path,
                  message: 'directory reader is closed',
                }),
              )
            const name = names[index++]
            return Effect.succeed(
              name === undefined ? Option.none() : Option.some({ name, kind: 'unknown' as const }),
            )
          }),
        }
      }),
    }
  }),
)
