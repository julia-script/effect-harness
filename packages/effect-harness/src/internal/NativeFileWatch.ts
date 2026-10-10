import { watch, type FSWatcher } from 'node:fs'
import { realpath, stat } from 'node:fs/promises'
import * as NativePath from 'node:path'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Queue from 'effect/Queue'
import * as Stream from 'effect/Stream'
import type * as FileWatch from '../FileWatch.js'
import { FileWatchError } from '../FileWatch.js'
import { native, nativeCode, nativeMessage } from './DirectoryWatchBinding.js'

interface Installed {
  readonly watcher: FSWatcher
  readonly ino: bigint
  readonly dev: bigint
  readonly recursive: boolean
  active: boolean
}

const within = (value: string, root: string) => {
  const relative = NativePath.relative(root, value)
  return (
    relative === '' ||
    !(
      relative === '..' ||
      relative.startsWith(`..${NativePath.sep}`) ||
      NativePath.isAbsolute(relative)
    )
  )
}

/** Native callbacks only enqueue invalidations. One scoped Effect worker repairs coverage. */
export const subscribe: FileWatch.Adapter['Service']['subscribe'] = Effect.fn(
  'NativeFileWatch.subscribe',
)(function* (targets, observer, options) {
  if (process.platform === 'win32')
    return yield* new FileWatchError({
      reason: 'unsupported',
      operation: 'subscribe',
      path: '',
      message:
        'Native Windows watches can prevent directory renames; select the explicit polling/rescan adapter',
    })
  const maxWatchers = options.maxWatchers ?? 512
  const fail = (operation: string, path: string) => (cause: unknown) => {
    const code = nativeCode(cause)
    let reason: FileWatchError['reason'] = 'backend'
    if (code === 'ERR_FEATURE_UNAVAILABLE_ON_PLATFORM' || code === 'ENOSYS' || code === 'ENOTSUP')
      reason = 'unsupported'
    else if (code === 'EACCES' || code === 'EPERM') reason = 'permission'
    return new FileWatchError({ reason, operation, path, message: nativeMessage(cause), cause })
  }
  const installed = new Map<string, Installed>()
  const aliases = new Map<string, ReadonlyArray<string>>()
  const wakes = yield* Queue.make<boolean, Cause.Done>({ capacity: 1, strategy: 'dropping' })
  let closed = false
  let settling = false
  const stop = Effect.sync(() => {
    if (closed) return
    closed = true
    for (const entry of installed.values()) {
      entry.active = false
      entry.watcher.close()
    }
    installed.clear()
    Queue.endUnsafe(wakes)
  })
  yield* Effect.addFinalizer(() => stop)
  const paths = targets.map((t) => t.path)
  const inspect = (path: string) =>
    native(() => stat(path, { bigint: true }), fail('stat', path)).pipe(
      Effect.catchIf(
        (error) => nativeCode(error.cause) === 'ENOENT' || nativeCode(error.cause) === 'ENOTDIR',
        () => Effect.void,
      ),
    )
  const reconcile = Effect.fn('NativeFileWatch.reconcile')(function* () {
    const desired = new Map<string, boolean>()
    const nextAliases = new Map<string, Array<string>>()
    const add = (value: string, recursive: boolean) => {
      desired.set(value, recursive || (desired.get(value) ?? false))
    }
    for (const target of targets) {
      const info = yield* inspect(target.path)
      let closest: string | undefined
      // Ancestor watchers keep missing/deleted/recreated targets reachable, up to the filesystem root.
      for (
        let ancestor = NativePath.dirname(target.path);
        ;
        ancestor = NativePath.dirname(ancestor)
      ) {
        const info = yield* inspect(ancestor)
        if (info?.isDirectory()) {
          add(ancestor, false)
          closest ??= ancestor
        }
        if (NativePath.dirname(ancestor) === ancestor) break
      }
      if (info?.isDirectory()) add(target.path, target.recursive ?? false)
      else if (target.recursive && closest) add(closest, true)
      if (info?.isFile()) {
        const physical = yield* native(
          () => realpath(target.path),
          fail('realpath', target.path),
        ).pipe(
          Effect.catchIf(
            (error) =>
              nativeCode(error.cause) === 'ENOENT' || nativeCode(error.cause) === 'ENOTDIR',
            () => Effect.void,
          ),
        )
        if (!physical) {
          observer.emit({ _tag: 'Rescan', paths, reason: 'coverage' })
          Queue.offerUnsafe(wakes, true)
        } else if (physical !== target.path) {
          add(physical, false)
          const logical = nextAliases.get(physical) ?? []
          logical.push(target.path)
          nextAliases.set(physical, logical)
        }
      }
      if (!closest)
        return yield* new FileWatchError({
          reason: 'coverage',
          operation: 'subscribe',
          path: target.path,
          message: 'No accessible existing ancestor can cover this target',
        })
    }
    if (desired.size > maxWatchers)
      return yield* new FileWatchError({
        reason: 'coverage',
        operation: 'subscribe',
        path: '',
        message: `Required ancestor/native coverage exceeds ${maxWatchers} handles`,
      })
    aliases.clear()
    for (const [physical, logical] of nextAliases) aliases.set(physical, logical)
    for (const [path, recursive] of desired) {
      const info = yield* inspect(path)
      if (!info) {
        observer.emit({ _tag: 'Rescan', paths, reason: 'coverage' })
        Queue.offerUnsafe(wakes, true)
        continue
      }
      const prior = installed.get(path)
      if (prior?.ino === info.ino && prior.dev === info.dev && prior.recursive === recursive)
        continue
      const entry = yield* Effect.try({
        try: () => {
          const entry: Installed = {
            ino: info.ino,
            dev: info.dev,
            recursive,
            active: true,
            watcher: watch(path, { recursive, persistent: false }, (kind, filename) => {
              if (closed || !entry.active) return
              if (filename === null) observer.emit({ _tag: 'Rescan', paths, reason: 'native' })
              else {
                const physical = info.isDirectory()
                  ? NativePath.resolve(path, filename.toString())
                  : path
                const values = [physical, ...(aliases.get(physical) ?? [])]
                for (const value of values) observer.emit({ _tag: 'Change', path: value, kind })
                // An ancestor invalidation may represent an entire newly created/replaced subtree.
                const impacted = targets.filter((t) =>
                  values.some((value) => within(t.path, value)),
                )
                if (impacted.length > 0)
                  observer.emit({
                    _tag: 'Rescan',
                    paths: impacted.map((t) => t.path),
                    reason: 'coverage',
                  })
              }
              Queue.offerUnsafe(wakes, true)
            }),
          }
          entry.watcher.on('error', (cause) => {
            if (!closed && entry.active) observer.fail(fail('watch', path)(cause))
          })
          entry.watcher.on('close', () => {
            if (!closed && entry.active) {
              installed.delete(path)
              observer.emit({ _tag: 'Rescan', paths, reason: 'coverage' })
              Queue.offerUnsafe(wakes, true)
            }
          })
          return entry
        },
        catch: fail(recursive ? 'recursive-watch' : 'watch', path),
      }).pipe(
        Effect.catchIf(
          (error) => nativeCode(error.cause) === 'ENOENT' || nativeCode(error.cause) === 'ENOTDIR',
          () => Effect.void,
        ),
      )
      if (!entry) {
        observer.emit({ _tag: 'Rescan', paths, reason: 'coverage' })
        Queue.offerUnsafe(wakes, true)
        continue
      }
      installed.set(path, entry)
      settling = true
      if (prior) {
        prior.active = false
        prior.watcher.close()
      }
    }
    for (const [path, entry] of installed) {
      if (!desired.has(path)) {
        entry.active = false
        entry.watcher.close()
        installed.delete(path)
      }
    }
  })
  yield* reconcile().pipe(Effect.onError(() => stop))
  yield* Queue.take(wakes).pipe(
    Effect.andThen(reconcile()),
    Effect.forever,
    Effect.catchCause((cause) =>
      Effect.gen(function* () {
        if (!Cause.hasInterrupts(cause) && !closed) {
          const errors = Cause.findError(cause)
          if (errors._tag === 'Success' && errors.success instanceof FileWatchError)
            observer.fail(errors.success)
          else
            observer.fail(
              new FileWatchError({
                reason: 'backend',
                operation: 'reconcile',
                path: '',
                message: Cause.pretty(cause),
                cause,
              }),
            )
        }
        yield* stop
      }),
    ),
    Effect.forkScoped,
  )
  // Every reattachment can have a native startup gap, not only the initial open.
  // One Clock-driven settling worker coalesces these invalidations without an unbounded timer per event.
  yield* Stream.tick('500 millis').pipe(
    Stream.runForEach(() =>
      Effect.sync(() => {
        if (!closed && settling) {
          settling = false
          observer.emit({ _tag: 'Rescan', paths, reason: 'coverage' })
          Queue.offerUnsafe(wakes, true)
        }
      }),
    ),
    Effect.forkScoped,
  )
  return {
    mode: 'native',
    recursive: targets.some((t) => t.recursive) ? 'native' : 'not-requested',
    ancestors: 'native',
    symlinks: 'file-targets',
    reliability: 'best-effort',
  } as const
})
