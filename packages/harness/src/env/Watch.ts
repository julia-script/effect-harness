import * as Serialization from '../Serialization.ts'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Base64 from 'effect/encoding/Base64'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import type * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import type * as Path from 'effect/Path'
import * as Queue from 'effect/Queue'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import {
  FileError,
  fromPlatform,
  type NativeFiles,
  type WatchChange,
  type WatchOptions,
  type WatchTarget,
  FileInvalid,
  FileUnknown,
} from '../Env.ts'

interface Scan {
  readonly values: Map<string, string>
  readonly directories: Map<string, string>
  readonly nestedSymlinks: Set<string>
}
export const make = Effect.fnUntraced(function* (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  native: NativeFiles['Service'],
  targets: ReadonlyArray<WatchTarget>,
  options: WatchOptions = {},
) {
  const interval = options.pollIntervalMs ?? 2000
  const budget = options.directoryBudget ?? 10000
  let mode: 'native' | 'polling' = options.mode ?? 'native'
  if (!Number.isFinite(interval) || interval <= 0 || !Number.isSafeInteger(budget) || budget <= 0)
    return yield* new FileError({ reason: new FileInvalid({ message: 'Invalid watch options' }) })
  const output = yield* Queue.unbounded<WatchChange, Cause.Done>()
  const events = yield* Queue.unbounded<{
    readonly path?: string | undefined
    readonly error?: FileError | undefined
    readonly owner?: string | undefined
    readonly settle?: boolean | undefined
  }>()
  let closed = false
  const release = (scope: Scope.Closeable) =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        if (closed) return
        closed = true
        yield* Scope.close(scope, Exit.void).pipe(
          Effect.ensuring(Queue.shutdown(events).pipe(Effect.andThen(Queue.shutdown(output)))),
        )
      }),
    )
  const scope = yield* Effect.acquireRelease(Scope.make(), release)
  const close = release(scope)
  const excluded = (target: WatchTarget, value: string): boolean =>
    path
      .relative(target.path, value)
      .split(path.sep)
      .some(
        (part) =>
          part !== '' &&
          ((target.exclude?.hidden === true && part.startsWith('.')) ||
            target.exclude?.names?.includes(part) === true),
      )
  const covered = (value: string): boolean =>
    targets.some(
      (target) =>
        value === target.path ||
        target.path.startsWith(value + path.sep) ||
        (value.startsWith(target.path + path.sep) &&
          !excluded(target, value) &&
          (target.recursive === true || path.dirname(value) === target.path)),
    )
  const reported = (value: string): string =>
    targets.some((target) => value === target.path || value.startsWith(target.path + path.sep))
      ? value
      : (targets.find((target) => target.path.startsWith(value + path.sep))?.path ?? value)
  const scan = Effect.fnUntraced(function* (): Effect.fn.Return<Scan, FileError> {
    const values = new Map<string, string>()
    const directories = new Map<string, string>()
    const nestedSymlinks = new Set<string>()
    const counted = new Set<string>()
    const stat = (value: string) =>
      native.lstat(value).pipe(
        Effect.catchIf(
          (error) => error.code === 'not_found',
          () => Effect.void,
        ),
      )
    const addDirectory = Effect.fnUntraced(function* (value: string, count = false) {
      const metadata = yield* fs.stat(value).pipe(Effect.option)
      if (Option.isNone(metadata)) return
      directories.set(
        value,
        JSON.stringify([metadata.value.dev, Option.getOrElse(metadata.value.ino, () => 0)]),
      )
      if (count) counted.add(value)
      if (counted.size > budget)
        return yield* new FileError({
          reason: new FileInvalid({
            message: `Watch directory budget ${budget} exceeded`,
            path: value,
          }),
        })
    })
    for (const target of targets) {
      let ancestor = path.dirname(target.path)
      while (true) {
        const parent = yield* stat(ancestor).pipe(Effect.orElseSucceed(() => undefined))
        if (parent?.kind === 'directory') {
          yield* addDirectory(ancestor)
          if (!values.has(ancestor)) values.set(ancestor, directories.get(ancestor) ?? '')
        }
        const next = path.dirname(ancestor)
        if (next === ancestor) break
        ancestor = next
      }
      const visit = Effect.fnUntraced(function* (
        value: string,
        root: boolean,
      ): Effect.fn.Return<void, FileError> {
        if (!root && excluded(target, value)) return
        const metadata = yield* stat(value).pipe(
          Effect.catchIf(
            (error) => !root || error.code !== 'permission_denied',
            () => Effect.void,
          ),
        )
        if (metadata === undefined) return
        if (
          !root &&
          metadata.kind === 'symlink' &&
          !targets.some((target) => target.path === value)
        )
          nestedSymlinks.add(value)
        let kind: string = metadata.kind
        let size = metadata.size
        let mtime = metadata.mtimeMs
        let identity = metadata.identity ?? ''
        if (root && kind === 'symlink') {
          const followed = yield* fs.stat(value).pipe(
            Effect.mapError((error) => fromPlatform(error, value)),
            Effect.catchIf(
              (error) => error.code !== 'permission_denied',
              () => Effect.void,
            ),
          )
          if (followed === undefined) return
          kind = followed.type === 'Directory' ? 'directory' : 'file'
          size = Number(followed.size)
          mtime = Option.isSome(followed.mtime) ? followed.mtime.value.getTime() : 0
          const real = yield* fs
            .realPath(value)
            .pipe(Effect.mapError((error) => fromPlatform(error, value)))
          yield* addDirectory(path.dirname(real))
        }
        if (kind !== 'symlink') {
          const platform = yield* fs.stat(value).pipe(Effect.option)
          if (Option.isSome(platform))
            identity = JSON.stringify([
              platform.value.dev,
              Option.getOrElse(platform.value.ino, () => 0),
            ])
        }
        let hash = ''
        if (kind === 'file' && size <= 256 * 1024) {
          const bytes = yield* fs.readFile(value).pipe(Effect.option)
          if (Option.isSome(bytes)) hash = Base64.encode(bytes.value)
        }
        if (root || !targets.some((target) => target.path === value))
          values.set(
            value,
            JSON.stringify([
              metadata.kind,
              kind,
              kind === 'directory' ? 0 : size,
              kind === 'directory' ? 0 : mtime,
              identity,
              hash,
            ]),
          )
        if (kind === 'directory') {
          if (root || target.recursive === true) {
            yield* addDirectory(value, true)
            const names = yield* fs.readDirectory(value).pipe(
              Effect.mapError((error) => fromPlatform(error, value)),
              Effect.catchIf(
                (error) =>
                  error.code === 'not_found' ||
                  error.code === 'not_directory' ||
                  (!root && error.code === 'permission_denied'),
                () => Effect.succeed([]),
              ),
            )
            for (const name of names) yield* visit(path.join(value, name), false)
          }
        }
      })
      yield* visit(target.path, true)
    }
    return { values, directories, nestedSymlinks }
  })
  const watchers = new Map<string, { readonly scope: Scope.Closeable; readonly identity: string }>()
  const closeWatchers = Effect.fnUntraced(function* () {
    for (const installed of watchers.values()) yield* Scope.close(installed.scope, Exit.void)
    watchers.clear()
  })
  const syncWatchers = Effect.fnUntraced(function* (wanted: Map<string, string>) {
    if (mode === 'polling') return false
    for (const [value, installed] of watchers)
      if (wanted.get(value) !== installed.identity) {
        yield* Scope.close(installed.scope, Exit.void)
        watchers.delete(value)
      }
    let added = false
    for (const [value, identity] of wanted)
      if (!watchers.has(value)) {
        const child = yield* Scope.fork(scope)
        const installed = yield* Effect.gen(function* () {
          const notifications = yield* native.watchDirectory(value)
          const consumer = yield* notifications.changes.pipe(
            Stream.runForEach((changed) =>
              Effect.suspend(() =>
                closed ? Effect.void : Queue.offer(events, { path: changed, owner: value }),
              ),
            ),
            Effect.andThen(
              Effect.fail(
                new FileError({
                  reason: new FileUnknown({
                    message: 'Native directory notifications ended',
                    path: value,
                  }),
                }),
              ),
            ),
            Effect.catchCause((cause) => {
              if (Cause.hasInterrupts(cause)) return Effect.failCause(cause)
              const caught = Cause.squash(cause)
              const error =
                caught instanceof FileError
                  ? caught
                  : new FileError({
                      reason: new FileUnknown({
                        message: Serialization.errorText(caught),
                        path: value,
                        cause,
                      }),
                    })
              return Effect.suspend(() =>
                closed
                  ? Effect.fail(error)
                  : Queue.offer(events, { error, owner: value }).pipe(
                      Effect.andThen(Effect.fail(error)),
                    ),
              )
            }),
            Effect.forkScoped,
          )
          yield* notifications.started.pipe(Effect.raceFirst(Fiber.join(consumer)))
          return true
        }).pipe(
          Scope.provide(child),
          Effect.onError(() => Scope.close(child, Exit.void)),
          Effect.catchIf(
            (error) => error.code === 'not_found' || error.code === 'permission_denied',
            () => Effect.succeed(false),
          ),
        )
        if (installed) {
          watchers.set(value, { scope: child, identity })
          added = true
        } else yield* Scope.close(child, Exit.void)
      }
    if (added)
      yield* Effect.sleep(500).pipe(
        Effect.andThen(
          Effect.sync(() => {
            if (!closed) Queue.offerUnsafe(events, { settle: true })
          }),
        ),
        Effect.forkScoped,
        Effect.provideService(Scope.Scope, scope),
      )
    return added
  })
  const establish = (directories: Map<string, string>) =>
    syncWatchers(directories).pipe(
      Effect.catchIf(
        () => true,
        () =>
          Effect.gen(function* () {
            mode = 'polling'
            yield* closeWatchers()
            yield* Queue.offer(output, { overflow: true })
            return false
          }),
      ),
    )
  let previous = yield* scan().pipe(Effect.onError(() => close))
  yield* establish(previous.directories).pipe(Effect.onError(() => close))
  const established = yield* scan().pipe(Effect.onError(() => close))
  yield* establish(established.directories).pipe(Effect.onError(() => close))
  const initial = [...new Set([...previous.values.keys(), ...established.values.keys()])].filter(
    (value) => previous.values.get(value) !== established.values.get(value),
  )
  previous = established
  if (initial.length > 0)
    yield* Queue.offer(output, { paths: [...new Set(initial.map(reported))].sort() })
  const worker = Effect.forever(
    Effect.gen(function* () {
      const raw: string[] = []
      if (mode === 'polling') yield* Effect.sleep(interval)
      else {
        const event = yield* Queue.take(events)
        if (event.error !== undefined && event.owner !== undefined) {
          const installed = watchers.get(event.owner)
          if (installed !== undefined) yield* Scope.close(installed.scope, Exit.void)
          watchers.delete(event.owner)
        }
        if (event.path !== undefined) raw.push(event.path)
        else if (event.settle !== true && event.error === undefined)
          yield* Queue.offer(output, { overflow: true })
        yield* Effect.sleep(50)
      }
      // Native backends can emit a nested link when its external target changes.
      // Such links are not followed; only a snapshot change reports their own mutation.
      const changes = new Set(
        raw.filter((value) => covered(value) && !previous.nestedSymlinks.has(value)).map(reported),
      )
      for (let round = 0; round < 10 && !closed; round++) {
        const next = yield* scan()
        for (const value of new Set([...previous.values.keys(), ...next.values.keys()]))
          if (previous.values.get(value) !== next.values.get(value)) changes.add(reported(value))
        previous = next
        if (!(yield* establish(next.directories))) break
      }
      if (changes.size > 0 && !closed) yield* Queue.offer(output, { paths: [...changes].sort() })
    }),
  ).pipe(
    Effect.catchCause((cause) => {
      if (Cause.hasInterrupts(cause))
        return Effect.failCause(
          Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
        )
      return Effect.gen(function* () {
        const error = Cause.squash(cause)
        yield* closeWatchers()
        yield* Queue.shutdown(events)
        yield* Queue.offer(output, {
          error:
            error instanceof FileError
              ? error
              : new FileError({
                  reason: new FileUnknown({ message: Serialization.errorText(error), cause }),
                }),
        })
        yield* Queue.end(output)
      })
    }),
  )
  yield* worker.pipe(Effect.forkScoped, Effect.provideService(Scope.Scope, scope))
  return {
    get mode() {
      return mode
    },
    changes: Stream.fromQueue(output),
  }
})
