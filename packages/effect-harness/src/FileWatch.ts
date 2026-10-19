import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import type * as ToolkitFileSystem from './ToolkitFileSystem.js'
import { binding, validPath } from './internal/DirectoryWatchBinding.js'

export class FileWatchError extends Schema.TaggedError<FileWatchError>()('FileWatchError', {
  reason: Schema.Literals(['invalid', 'permission', 'unsupported', 'coverage', 'backend']),
  operation: Schema.String,
  path: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export interface Target {
  readonly path: string
  readonly recursive?: boolean
  /** Each name excludes a path component below this target, including descendants. */
  readonly exclude?: { readonly hidden?: boolean; readonly names?: ReadonlyArray<string> }
}

export type Event =
  | { readonly _tag: 'Change'; readonly path: string; readonly kind: 'change' | 'rename' }
  | {
      readonly _tag: 'Rescan'
      readonly paths: ReadonlyArray<string>
      readonly reason: 'overflow' | 'coverage' | 'native' | 'poll'
    }

/** Coverage describes invalidations, not a lossless operation log or an automatic content snapshot. */
export interface Coverage {
  readonly mode: 'native' | 'polling'
  readonly recursive: 'native' | 'rescan' | 'not-requested'
  readonly ancestors: 'native' | 'rescan'
  /** Only explicitly watched file targets are followed; directory symlink trees are not traversed. */
  readonly symlinks: 'file-targets' | 'unsupported'
  readonly reliability: 'best-effort'
}

export interface Options {
  readonly queueCapacity?: number
  /** Native handle budget, including ancestors. Exhaustion is a typed coverage failure. */
  readonly maxWatchers?: number
  /** Explicit periodic invalidations using Effect Clock, supplementing native coverage. */
  readonly rescanIntervalMs?: number
}

export interface Observer {
  readonly emit: (event: Event) => void
  readonly fail: (error: FileWatchError) => void
}

/**
 * Explicit custom/remote seam. Targets are resolved in the same Path/backend namespace.
 * subscribe must establish coverage before succeeding and scope every callback/resource.
 * It must report unsupported recursion/errors; it must never select a hidden native fallback.
 */
export class Adapter extends Context.Service<
  Adapter,
  {
    readonly subscribe: (
      targets: ReadonlyArray<Target>,
      observer: Observer,
      options: Options,
    ) => Effect.Effect<Coverage, FileWatchError, Scope.Scope>
  }
>()('effect-harness/FileWatch/Adapter') {}

export interface Watch {
  readonly coverage: Coverage
  /** One consumer; queue is shared, so multiple consumers divide events. */
  readonly events: Stream.Stream<Event, FileWatchError>
  /** Idempotent; closes native resources and ends pending stream pulls. */
  readonly close: Effect.Effect<void>
}

export class FileWatch extends Context.Service<
  FileWatch,
  {
    readonly open: (
      targets: ReadonlyArray<Target>,
      options?: Options,
    ) => Effect.Effect<Watch, FileWatchError, Scope.Scope>
  }
>()('effect-harness/FileWatch') {}

export const layer = (
  options: ToolkitFileSystem.Options,
): Layer.Layer<FileWatch, FileWatchError, Adapter | Path.Path> => {
  const config = { ...options }
  return Layer.effect(
    FileWatch,
    Effect.gen(function* () {
      const path = yield* Path.Path
      const adapter = yield* Adapter
      if (!validPath(config.root) || !path.isAbsolute(config.root))
        return yield* new FileWatchError({
          reason: 'invalid',
          operation: 'bind',
          path: config.root,
          message: 'root must be absolute without NUL',
        })
      const bound = binding(path, config)
      return {
        open: Effect.fn('FileWatch.open')(function* (
          targets: ReadonlyArray<Target>,
          options: Options = {},
        ) {
          options = { ...options }
          const fail = (
            message: string,
            value = '',
            reason: FileWatchError['reason'] = 'invalid',
          ) => new FileWatchError({ reason, operation: 'open', path: value, message })
          const capacity = options.queueCapacity ?? 256
          const budget = options.maxWatchers ?? 512
          if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4096)
            return yield* fail('queueCapacity must be between 1 and 4096')
          if (!Number.isSafeInteger(budget) || budget < 1 || budget > 10000)
            return yield* fail('maxWatchers must be between 1 and 10000')
          if (
            options.rescanIntervalMs !== undefined &&
            (!Number.isFinite(options.rescanIntervalMs) || options.rescanIntervalMs <= 0)
          )
            return yield* fail('rescanIntervalMs must be positive and finite')
          if (targets.length < 1 || targets.length > 128)
            return yield* fail('one to 128 watch targets are required')
          const resolved: Array<Target> = []
          for (const target of targets) {
            if (!validPath(target.path))
              return yield* fail('path must be nonempty without NUL', target.path)
            for (const name of target.exclude?.names ?? []) {
              if (!validPath(name) || name === '.' || name === '..' || name.includes(path.sep))
                return yield* fail('exclusion names must be single path components', name)
            }
            const value = bound.resolve(target.path)
            if (!value.allowed)
              return yield* fail('target is outside the bound root', target.path, 'permission')
            resolved.push({
              path: value.resolved,
              recursive: target.recursive ?? false,
              exclude: {
                hidden: target.exclude?.hidden ?? false,
                names: [...(target.exclude?.names ?? [])],
              },
            })
          }
          const paths = resolved.map((t) => t.path)
          const accepts = (value: string): boolean =>
            resolved.some((target) => {
              const relative = path.relative(target.path, value)
              if (relative === '') return true
              if (
                relative === '..' ||
                relative.startsWith(`..${path.sep}`) ||
                path.isAbsolute(relative)
              )
                return false
              const parts = relative.split(path.sep)
              if (!target.recursive && parts.length > 1) return false
              return !parts.some(
                (part) =>
                  (target.exclude?.hidden && part.startsWith('.')) ||
                  target.exclude?.names?.includes(part),
              )
            })
          const scope = yield* Scope.fork(yield* Scope.Scope)
          const queue = yield* Queue.make<Event, FileWatchError | Cause.Done>({
            capacity,
            strategy: 'dropping',
          })
          let closed = false
          let overflow = false
          const observer: Observer = {
            emit: (event) => {
              if (closed || overflow) return
              const eventPaths = event._tag === 'Change' ? [event.path] : event.paths
              if (eventPaths.some((value) => !validPath(value) || !path.isAbsolute(value))) {
                observer.fail(
                  new FileWatchError({
                    reason: 'backend',
                    operation: 'event',
                    path: '',
                    message: 'Adapter events must use absolute paths without NUL',
                  }),
                )
                return
              }
              let bounded: Event
              if (event._tag === 'Change') {
                if (!accepts(event.path)) return
                bounded = { ...event }
              } else {
                const rescans = [
                  ...new Set(
                    event.paths.flatMap((value) => {
                      if (accepts(value)) return [value]
                      return resolved
                        .filter((target) => {
                          const relative = path.relative(value, target.path)
                          return (
                            relative === '' ||
                            !(
                              relative === '..' ||
                              relative.startsWith(`..${path.sep}`) ||
                              path.isAbsolute(relative)
                            )
                          )
                        })
                        .map((target) => target.path)
                    }),
                  ),
                ]
                if (rescans.length === 0) return
                bounded = { ...event, paths: rescans }
              }
              if (Queue.offerUnsafe(queue, bounded)) return
              // Replace a full queue with one mandatory invalidation. No loss is reported as success.
              while (Queue.sizeUnsafe(queue) > 0) Queue.takeUnsafe(queue)
              overflow = true
              Queue.offerUnsafe(queue, { _tag: 'Rescan', paths, reason: 'overflow' })
            },
            fail: (error) => {
              if (!closed) Queue.failCauseUnsafe(queue, Cause.fail(error))
            },
          }
          const close = Effect.suspend(() => {
            if (closed) return Effect.void
            closed = true
            while (Queue.sizeUnsafe(queue) > 0) Queue.takeUnsafe(queue)
            Queue.endUnsafe(queue)
            return Scope.close(scope, Exit.void)
          }).pipe(Effect.uninterruptible)
          yield* Effect.addFinalizer(() => close)
          const coverage = yield* adapter
            .subscribe(resolved, observer, {
              ...options,
              queueCapacity: capacity,
              maxWatchers: budget,
            })
            .pipe(
              Effect.provideService(Scope.Scope, scope),
              Effect.onError(() => close),
            )
          if (options.rescanIntervalMs !== undefined)
            yield* Stream.tick(options.rescanIntervalMs).pipe(
              Stream.runForEach(() =>
                Effect.sync(() => observer.emit({ _tag: 'Rescan', paths, reason: 'poll' })),
              ),
              Effect.forkIn(scope),
            )
          return {
            coverage,
            close,
            events: Stream.fromQueue(queue).pipe(
              Stream.tap((event) =>
                Effect.sync(() => {
                  if (event._tag === 'Rescan' && event.reason === 'overflow') overflow = false
                }),
              ),
            ),
          }
        }),
      }
    }),
  )
}

/** Explicit periodic invalidation adapter; consumers perform their own bounded rescan. */
export const pollingAdapter = (intervalMs: number): Layer.Layer<Adapter> =>
  Layer.succeed(Adapter, {
    subscribe: Effect.fn('FileWatch.polling')(function* (
      targets: ReadonlyArray<Target>,
      observer: Observer,
    ) {
      if (!Number.isFinite(intervalMs) || intervalMs <= 0)
        return yield* new FileWatchError({
          reason: 'invalid',
          operation: 'subscribe',
          path: '',
          message: 'poll interval must be positive and finite',
        })
      yield* Stream.tick(intervalMs).pipe(
        Stream.runForEach(() =>
          Effect.sync(() =>
            observer.emit({ _tag: 'Rescan', paths: targets.map((t) => t.path), reason: 'poll' }),
          ),
        ),
        Effect.forkScoped,
      )
      return {
        mode: 'polling',
        recursive: targets.some((t) => t.recursive) ? 'rescan' : 'not-requested',
        ancestors: 'rescan',
        symlinks: 'unsupported',
        reliability: 'best-effort',
      } as const
    }),
  })
