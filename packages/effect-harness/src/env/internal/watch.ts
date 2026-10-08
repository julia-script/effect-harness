import type { Env } from '../../Env.ts'
import * as Function from 'effect/Function'
import { constTrue, constUndefined, constant } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as Ref from 'effect/Ref'
import * as HashMap from 'effect/HashMap'
import * as HashSet from 'effect/HashSet'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Time from '../../Time.ts'
import * as Serialization from '../../Serialization.ts'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Base64 from 'effect/encoding/Base64'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Queue from 'effect/Queue'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import { NativeFiles } from '../../NativeFiles.ts'
import { makeWatcher, fromPlatform, WatchChange, type WatchTarget } from '../../Env.ts'
import { FileError, FileInvalidError, FileUnknownError } from '../../FileError.ts'

interface Scan {
  readonly values: HashMap.HashMap<string, string>
  readonly directories: HashMap.HashMap<string, string>
  readonly nestedSymlinks: HashSet.HashSet<string>
}
/** Captures infrastructure without allocating a watcher or taking ownership of a call scope. */
export const acquire = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const native = yield* NativeFiles
  return Effect.fnUntraced(function* (
    targets: ReadonlyArray<WatchTarget>,
    options: Env.WatchOptions = {},
  ): Effect.fn.Return<import('../../Env.ts').Watcher, FileError, Scope.Scope> {
    const interval = yield* Time.duration(options.pollInterval ?? '2 seconds').pipe(
      Effect.mapError(
        (cause) =>
          new FileError({
            reason: new FileInvalidError({ message: 'Invalid watch options', cause }),
          }),
      ),
    )
    const budget = options.directoryBudget ?? 10000
    const mode = yield* Ref.make<'native' | 'polling'>(options.mode ?? 'native')
    if (
      !Duration.isFinite(interval) ||
      Duration.toMillis(interval) <= 0 ||
      !Number.isSafeInteger(budget) ||
      budget <= 0
    )
      return yield* new FileError({
        reason: new FileInvalidError({ message: 'Invalid watch options' }),
      })
    const output = yield* Queue.unbounded<WatchChange, Cause.Done>()
    const events = yield* Queue.unbounded<{
      readonly path?: string | undefined
      readonly error?: FileError | undefined
      readonly owner?: string | undefined
      readonly settle?: boolean | undefined
    }>()
    const closed = yield* Ref.make(false)
    const release = Effect.fnUntraced(function* (scope: Scope.Closeable) {
      if (yield* Ref.getAndSet(closed, true)) return
      yield* Scope.close(scope, Exit.void).pipe(
        Effect.ensuring(Queue.shutdown(events).pipe(Effect.andThen(Queue.shutdown(output)))),
      )
    }, Effect.uninterruptible)
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
        : Option.getOrElse(
            Option.map(
              Arr.findFirst(targets, (target) => target.path.startsWith(value + path.sep)),
              (target) => target.path,
            ),
            Function.constant(value),
          )
    const scan: Effect.Effect<Scan, FileError> = Effect.gen(function* () {
      const values = new Map<string, string>()
      const directories = new Map<string, string>()
      const nestedSymlinks = new Set<string>()
      const counted = new Set<string>()
      // Fresh no-follow samples distinguish replacements/links across each scan and installation pass; no deduplication.
      const stat = (value: string) =>
        native.lstat(value).pipe(
          Effect.catchIf(
            (error) => error.code === 'not_found',
            () => Effect.void,
          ),
        )
      const addDirectory = Effect.fnUntraced(function* (value: string, count = false) {
        // Directory inode identity is sampled separately from lstat; native APIs provide no atomic bulk snapshot.
        const metadata = yield* fs.stat(value).pipe(Effect.option)
        const identity = Option.map(metadata, (self) =>
          JSON.stringify([self.dev, Option.getOrElse(self.ino, constant(0))]),
        )
        if (Option.isNone(identity)) return
        Option.map(identity, (self) => directories.set(value, self))
        if (count) counted.add(value)
        if (counted.size > budget)
          return yield* new FileError({
            reason: new FileInvalidError({
              message: `Watch directory budget ${budget} exceeded`,
              path: value,
            }),
          })
      })
      for (const target of targets) {
        let ancestor = path.dirname(target.path)
        while (true) {
          const parent = yield* stat(ancestor).pipe(Effect.orElseSucceed(constUndefined))
          if (parent?.kind === 'directory') {
            yield* addDirectory(ancestor)
            if (!values.has(ancestor))
              values.set(
                ancestor,
                Option.getOrElse(
                  Option.fromUndefinedOr(directories.get(ancestor)),
                  Function.constant(''),
                ),
              )
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
            // Explicit roots deliberately follow links; keep this sample separate from the no-follow recursive traversal.
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
            mtime = Time.fromEpochMillis(
              followed.mtime.pipe(
                Option.map((self) => self.getTime()),
                Option.getOrElse(constant(0)),
              ),
            )
            // Resolve the current root alias on every scan so retargeting can reinstall the correct parent watcher.
            const real = yield* fs
              .realPath(value)
              .pipe(Effect.mapError((error) => fromPlatform(error, value)))
            yield* addDirectory(path.dirname(real))
          }
          if (kind !== 'symlink') {
            // Following inode metadata supplies identity independently of the no-follow sample, without stale request caching.
            const platform = yield* fs.stat(value).pipe(Effect.option)
            identity = Option.match(platform, {
              onNone: () => identity,
              onSome: (self) => JSON.stringify([self.dev, Option.getOrElse(self.ino, constant(0))]),
            })
          }
          let hash = ''
          if (kind === 'file' && size <= 256 * 1024) {
            // Read current bytes even when timestamps/size are unchanged; caching would lose same-size content changes.
            const bytes = yield* fs.readFile(value).pipe(Effect.option)
            hash = Option.match(bytes, { onNone: () => hash, onSome: Base64.encode })
          }
          if (root || !targets.some((target) => target.path === value))
            values.set(
              value,
              JSON.stringify([
                metadata.kind,
                kind,
                kind === 'directory' ? 0 : size,
                kind === 'directory' ? 0 : DateTime.toEpochMillis(mtime),
                identity,
                hash,
              ]),
            )
          if (kind === 'directory') {
            if (root || target.recursive === true) {
              yield* addDirectory(value, true)
              // Enumerate current children each scan; mutations between passes must remain visible, and no bulk API exists.
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
      return {
        values: HashMap.fromIterable(values),
        directories: HashMap.fromIterable(directories),
        nestedSymlinks: HashSet.fromIterable(nestedSymlinks),
      }
    })
    const watchers = yield* Ref.make(
      HashMap.empty<string, { readonly scope: Scope.Closeable; readonly identity: string }>(),
    )
    const closeWatchers = Effect.gen(function* () {
      const current = yield* Ref.getAndSet(watchers, HashMap.empty())
      for (const installed of HashMap.values(current))
        yield* Scope.close(installed.scope, Exit.void)
    })
    const syncWatchers = Effect.fnUntraced(function* (wanted: HashMap.HashMap<string, string>) {
      if ((yield* Ref.get(mode)) === 'polling') return false
      for (const [value, installed] of yield* Ref.get(watchers))
        if (!Option.contains(HashMap.get(wanted, value), installed.identity)) {
          yield* Scope.close(installed.scope, Exit.void)
          yield* Ref.update(watchers, HashMap.remove(value))
        }
      let added = false
      for (const [value, identity] of wanted)
        if (!HashMap.has(yield* Ref.get(watchers), value)) {
          const child = yield* Scope.fork(scope)
          const installed = yield* Effect.gen(function* () {
            const notifications = yield* native.watchDirectory(value)
            const consumer = yield* notifications.changes.pipe(
              Stream.runForEach((changed) =>
                Effect.suspend(() =>
                  Ref.getUnsafe(closed)
                    ? Effect.void
                    : Queue.offer(events, { path: changed, owner: value }),
                ),
              ),
              Effect.andThen(
                Effect.fail(
                  new FileError({
                    reason: new FileUnknownError({
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
                        reason: new FileUnknownError({
                          message: Serialization.errorText(caught),
                          path: value,
                          cause,
                        }),
                      })
                return Effect.suspend(() =>
                  Ref.getUnsafe(closed)
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
            yield* Ref.update(watchers, HashMap.set(value, { scope: child, identity }))
            added = true
          } else yield* Scope.close(child, Exit.void)
        }
      if (added)
        yield* Effect.sleep('500 millis').pipe(
          Effect.andThen(
            Effect.suspend(() =>
              Ref.getUnsafe(closed)
                ? Effect.void
                : Queue.offer(events, { settle: true }).pipe(Effect.asVoid),
            ),
          ),
          Effect.forkScoped,
          Effect.provideService(Scope.Scope, scope),
        )
      return added
    })
    const establish = (directories: HashMap.HashMap<string, string>) =>
      syncWatchers(directories).pipe(
        Effect.catchIf(
          constTrue,
          Effect.fnUntraced(function* () {
            yield* Ref.set(mode, 'polling')
            yield* closeWatchers
            yield* Queue.offer(output, WatchChange.Overflow())
            return false
          }),
        ),
      )
    const previous = yield* Ref.make(yield* scan.pipe(Effect.onError(constant(close))))
    yield* establish((yield* Ref.get(previous)).directories).pipe(Effect.onError(constant(close)))
    const established = yield* scan.pipe(Effect.onError(constant(close)))
    yield* establish(established.directories).pipe(Effect.onError(constant(close)))
    const initial = Arr.filter(
      Arr.dedupe([
        ...HashMap.keys((yield* Ref.get(previous)).values),
        ...HashMap.keys(established.values),
      ]),
      (value) =>
        !Option.makeEquivalence<string>((a, b) => a === b)(
          HashMap.get(Ref.getUnsafe(previous).values, value),
          HashMap.get(established.values, value),
        ),
    )
    yield* Ref.set(previous, established)
    if (initial.length > 0)
      yield* Queue.offer(
        output,
        WatchChange.Paths({ paths: Arr.dedupe(initial.map(reported)).sort() }),
      )
    const worker = Effect.forever(
      Effect.gen(function* () {
        const raw: Array<string> = []
        if ((yield* Ref.get(mode)) === 'polling') yield* Effect.sleep(interval)
        else {
          const event = yield* Queue.take(events)
          if (event.error !== undefined && event.owner !== undefined) {
            const installed = HashMap.get(yield* Ref.get(watchers), event.owner)
            yield* Option.match(installed, {
              onNone: () => Effect.void,
              onSome: (self) => Scope.close(self.scope, Exit.void),
            })
            yield* Ref.update(watchers, HashMap.remove(event.owner))
          }
          if (event.path !== undefined) raw.push(event.path)
          else if (event.settle !== true && event.error === undefined)
            yield* Queue.offer(output, WatchChange.Overflow())
          yield* Effect.sleep('50 millis')
        }
        // Native backends can emit a nested link when its external target changes.
        // Such links are not followed; only a snapshot change reports their own mutation.
        const changes = Arr.filter(
          raw,
          (value) => covered(value) && !HashSet.has(Ref.getUnsafe(previous).nestedSymlinks, value),
        ).map(reported)
        for (let round = 0; round < 10 && !Ref.getUnsafe(closed); round++) {
          const next = yield* scan
          for (const value of Arr.dedupe([
            ...HashMap.keys((yield* Ref.get(previous)).values),
            ...HashMap.keys(next.values),
          ]))
            if (
              !Option.makeEquivalence<string>((a, b) => a === b)(
                HashMap.get(Ref.getUnsafe(previous).values, value),
                HashMap.get(next.values, value),
              )
            )
              changes.push(reported(value))
          yield* Ref.set(previous, next)
          if (!(yield* establish(next.directories))) break
        }
        if (changes.length > 0 && !Ref.getUnsafe(closed))
          yield* Queue.offer(output, WatchChange.Paths({ paths: Arr.dedupe(changes).sort() }))
      }),
    ).pipe(
      Effect.catchCause(
        Effect.fnUntraced(function* (cause) {
          if (Cause.hasInterrupts(cause))
            return yield* Effect.failCause(
              // effect-nit-allow P1-stdlib-collection-replacements: native Cause.fromReasons retains its caller array, which may be sparse. Native filtering skips missing reasons while retaining interruption order; Effect Array.filter would call isInterruptReason(undefined).
              Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
            )
          const error = Cause.squash(cause)
          yield* closeWatchers
          yield* Queue.shutdown(events)
          yield* Queue.offer(
            output,
            WatchChange.Error({
              error:
                error instanceof FileError
                  ? error
                  : new FileError({
                      reason: new FileUnknownError({
                        message: Serialization.errorText(error),
                        cause,
                      }),
                    }),
            }),
          )
          yield* Queue.end(output)
        }),
      ),
    )
    yield* worker.pipe(Effect.forkScoped, Effect.provideService(Scope.Scope, scope))
    return makeWatcher({
      get mode() {
        return Ref.getUnsafe(mode)
      },
      changes: Stream.fromQueue(output),
    })
  })
})

// effect-nit-allow B-no-service-arguments: This scoped capability constructor installs explicitly selected filesystem/path/native implementations and allocates a new Watcher owned by the caller's Scope. Direct WatchChannels and EnvLifetime fixtures require their exact substituted producers; Env uses acquire once and passes only target/options data per call.
export const make = (input: {
  readonly fs: FileSystem.FileSystem
  readonly path: Path.Path
  readonly native: NativeFiles['Service']
  readonly targets: ReadonlyArray<WatchTarget>
  readonly options?: Env.WatchOptions | undefined
}) =>
  acquire.pipe(
    Effect.provideService(FileSystem.FileSystem, input.fs),
    Effect.provideService(Path.Path, input.path),
    Effect.provideService(NativeFiles, input.native),
    Effect.flatMap((open) => open(input.targets, input.options)),
  )
