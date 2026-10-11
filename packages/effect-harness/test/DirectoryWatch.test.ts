import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Result from 'effect/Result'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import { vi } from 'vitest'
import * as DirectoryReader from '../src/DirectoryReader.ts'
import * as DirectoryReaderNode from '../src/DirectoryReaderNode.ts'
import * as FileWatch from '../src/FileWatch.ts'
import * as FileWatchNode from '../src/FileWatchNode.ts'

interface NativeHandle {
  readonly path: string
  closed: number
}
const nativeObservations = vi.hoisted((): { callback: boolean; handles: Array<NativeHandle> } => ({
  callback: false,
  handles: [],
}))
vi.mock('node:fs', async (importOriginal) => {
  const fs = await importOriginal<typeof import('node:fs')>()
  const watch = (
    path: import('node:fs').PathLike,
    options: import('node:fs').WatchOptionsWithStringEncoding,
    listener: import('node:fs').WatchListener<string>,
  ) => {
    const handle = { path: path.toString(), closed: 0 }
    const watcher = fs.watch(path, options, (kind, filename) => {
      const previous = nativeObservations.callback
      nativeObservations.callback = true
      try {
        listener(kind, filename)
      } finally {
        nativeObservations.callback = previous
      }
    })
    nativeObservations.handles.push(handle)
    const close = watcher.close.bind(watcher)
    watcher.close = () => {
      handle.closed++
      close()
    }
    return watcher
  }
  return { ...fs, watch }
})

// Observe real handles and the existing settling fiber; callback rescans are never readiness.
const observeNativeReadiness = (holdStartup = false) =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock
    const startup = yield* Deferred.make<void>()
    let held = yield* Deferred.make<void>()
    const callbackCoverage = yield* Deferred.make<void>()
    const firstHandle = nativeObservations.handles.length
    const settled = new Map<string, NativeHandle>()
    let changed = Deferred.makeUnsafe<void>()
    let worker: number | undefined
    let release: (() => void) | undefined
    let holding = holdStartup
    const observedClock: Clock.Clock = {
      currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
      currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
      monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
      currentTimeMillis: clock.currentTimeMillis,
      currentTimeNanos: clock.currentTimeNanos,
      monotonicTimeNanos: clock.monotonicTimeNanos,
      sleep: (duration) =>
        Effect.withFiber((fiber) => {
          worker = fiber.id
          return clock.sleep(duration)
        }),
    }
    const adapter = Layer.effect(
      FileWatch.Adapter,
      Effect.map(FileWatch.Adapter, (native) => ({
        subscribe: (targets, observer, options) =>
          native
            .subscribe(
              targets,
              {
                fail: observer.fail,
                emit: (event) => {
                  if (event._tag === 'Rescan' && event.reason === 'coverage') {
                    if (nativeObservations.callback)
                      Deferred.doneUnsafe(callbackCoverage, Effect.void)
                    else if (worker !== undefined && Fiber.getCurrent()?.id === worker) {
                      const publish = () => {
                        for (const handle of nativeObservations.handles.slice(firstHandle))
                          if (handle.closed === 0) settled.set(handle.path, handle)
                        observer.emit(event)
                        Deferred.doneUnsafe(startup, Effect.void)
                        const previous = changed
                        changed = Deferred.makeUnsafe<void>()
                        Deferred.doneUnsafe(previous, Effect.void)
                      }
                      if (holding) {
                        release = publish
                        Deferred.doneUnsafe(held, Effect.void)
                      } else publish()
                      return
                    }
                  }
                  observer.emit(event)
                },
              },
              options,
            )
            .pipe(Effect.provideService(Clock.Clock, observedClock)),
      })),
    ).pipe(Layer.provide(FileWatchNode.adapter))
    const awaitAttachment = (path: string): Effect.Effect<void> =>
      Effect.suspend(() => {
        const handle = nativeObservations.handles
          .slice(firstHandle)
          .findLast((handle) => handle.path === path && handle.closed === 0)
        if (handle && settled.get(path) === handle) return Effect.void
        return Deferred.await(changed).pipe(Effect.andThen(awaitAttachment(path)))
      })
    return {
      adapter,
      startup,
      held,
      hold: Effect.sync(() => {
        holding = true
        held = Deferred.makeUnsafe<void>()
        return held
      }),
      callbackCoverage,
      awaitAttachment,
      release: Effect.sync(() => {
        holding = false
        release?.()
      }),
      handles: () => nativeObservations.handles.slice(firstHandle),
    }
  })

const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const temporary = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.makeTempDirectoryScoped({ prefix: 'directory-watch-' })
})
const error = <A, E>(value: Result.Result<A, E>): E => {
  if (Result.isFailure(value)) return value.failure
  return assert.fail('Expected a typed failure')
}

const expectInvalidation = <A, E, R, E2, R2>(
  events: Array<FileWatch.Event>,
  target: string,
  mutate: Effect.Effect<A, E, R>,
  verify: Effect.Effect<void, E2, R2>,
) =>
  Effect.gen(function* () {
    const path = yield* Path.Path
    const boundary = events.length
    yield* mutate
    for (let i = 0; i < 100; i++) {
      const invalidated = events.slice(boundary).some((event) => {
        if (event._tag === 'Change') return event.path === target
        return event.paths.some((value) => {
          const relative = path.relative(value, target)
          return (
            relative === '' ||
            !(
              relative === '..' ||
              relative.startsWith(`..${path.sep}`) ||
              path.isAbsolute(relative)
            )
          )
        })
      })
      if (invalidated) {
        // A covering rescan requests a real read; an event alone never proves updated content.
        yield* verify
        return
      }
      yield* Effect.sleep('20 millis')
    }
    assert.fail(
      `Missing native invalidation for ${target}: ${JSON.stringify(events.slice(boundary))}`,
    )
  })

const collectInvalidations = (
  watcher: FileWatch.Watch,
  events: Array<FileWatch.Event>,
  readiness: Deferred.Deferred<void>,
) =>
  Effect.gen(function* () {
    const consumer = yield* watcher.events.pipe(
      Stream.runForEach((event) => Effect.sync(() => events.push(event))),
      Effect.forkScoped,
    )
    // A callback can request a rescan before reconciliation/settling has completed.
    yield* Deferred.await(readiness).pipe(Effect.timeout('2 seconds'))
    yield* Effect.yieldNow
    return consumer
  })

describe('DirectoryReader native opened handles', () => {
  it.effect(
    'bounds large pages, preserves identity before first read across rename, and closes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const root = yield* temporary
          const directory = path.join(root, 'original')
          yield* fs.makeDirectory(directory)
          const names = Array.from({ length: 2053 }, (_, i) => `entry-${i}`)
          yield* Effect.forEach(
            names,
            (name) => fs.writeFileString(path.join(directory, name), ''),
            { concurrency: 16, discard: true },
          )
          yield* fs.makeDirectory(path.join(directory, 'sub'))
          yield* fs.symlink(path.join(directory, names[0]!), path.join(directory, 'link'))
          const reader = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (d) =>
            d.open('original', { requireOpenedIdentity: true, requireBoundedEnumeration: true }),
          ).pipe(Effect.provide(DirectoryReaderNode.layer({ root })))
          yield* fs.rename(directory, path.join(root, 'renamed'))
          yield* fs.makeDirectory(directory)
          yield* fs.writeFileString(path.join(directory, 'replacement-only'), '')
          const entries: Array<DirectoryReader.Entry> = []
          let done = false
          while (!done) {
            const page = yield* reader.next(37)
            assert.isAtMost(page.entries.length, 37)
            entries.push(...page.entries)
            done = page.done
          }
          assert.deepStrictEqual(
            entries.map((e) => e.name).sort(),
            [...names, 'sub', 'link'].sort(),
          )
          assert.strictEqual(entries.find((e) => e.name === 'sub')?.kind, 'directory')
          assert.strictEqual(entries.find((e) => e.name === 'link')?.kind, 'symlink')
          assert.strictEqual(reader.capabilities.allocation, 'native-buffer')
          assert.deepStrictEqual(yield* reader.next(1), { entries: [], done: true })
          assert.strictEqual(error(yield* reader.next(1025).pipe(Effect.result)).reason, 'invalid')
          yield* reader.close
          yield* reader.close
          assert.strictEqual(error(yield* reader.next(1).pipe(Effect.result)).reason, 'closed')
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect('refuses files and missing paths, and scope closure rejects further reads', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* temporary
        yield* fs.writeFileString(`${root}/file`, '')
        const probe = Effect.gen(function* () {
          const service = yield* DirectoryReader.DirectoryReader
          assert.strictEqual(
            error(yield* service.open('missing').pipe(Effect.result)).reason,
            'notFound',
          )
          assert.strictEqual(
            error(yield* service.open('file').pipe(Effect.result)).reason,
            'notDirectory',
          )
          return yield* Effect.scoped(service.open('.'))
        }).pipe(Effect.provide(DirectoryReaderNode.layer({ root })))
        const reader = yield* probe
        assert.strictEqual(error(yield* reader.next(1).pipe(Effect.result)).reason, 'closed')
      }),
    ).pipe(Effect.provide(files)),
  )
})

describe('DirectoryReader replaceable backends', () => {
  it.effect('isolates remote roots, serializes cursor reads and releases exactly once', () => {
    const opened: Array<string> = []
    const closed: Array<string> = []
    const adapter = Layer.succeed(DirectoryReader.Adapter, {
      capabilities: {
        identity: 'opened-directory',
        allocation: 'native-buffer',
        membership: 'enumeration-time',
      },
      open: Effect.fn(function* (path: string) {
        opened.push(path)
        let cursor = 0
        let released = false
        const close = Effect.sync(() => {
          if (!released) {
            closed.push(path)
            released = true
          }
        })
        yield* Effect.addFinalizer(() => close)
        return {
          capabilities: {
            identity: 'opened-directory',
            allocation: 'native-buffer',
            membership: 'enumeration-time',
          } as const,
          read: Effect.sync(() =>
            cursor++ < 3
              ? Option.some({ name: `${path}:${cursor}`, kind: 'file' as const })
              : Option.none(),
          ),
          close,
        }
      }),
    })
    const run = (root: string) =>
      Effect.scoped(
        Effect.gen(function* () {
          const service = yield* DirectoryReader.DirectoryReader
          assert.strictEqual(
            error(yield* service.open('../escape').pipe(Effect.result)).reason,
            'permission',
          )
          const reader = yield* service.open('same')
          const pages = yield* Effect.all([reader.next(2), reader.next(2)], {
            concurrency: 'unbounded',
          })
          return pages.flatMap((p) => p.entries.map((e) => e.name))
        }),
      ).pipe(
        Effect.provide(
          DirectoryReader.layer({ root, pathPolicy: 'within-root' }).pipe(
            Layer.provide([adapter, Path.layer]),
          ),
        ),
      )
    return Effect.gen(function* () {
      const results = yield* Effect.all([run('/remote/a'), run('/remote/b')], {
        concurrency: 'unbounded',
      })
      assert.deepStrictEqual(results, [
        ['/remote/a/same:1', '/remote/a/same:2', '/remote/a/same:3'],
        ['/remote/b/same:1', '/remote/b/same:2', '/remote/b/same:3'],
      ])
      assert.deepStrictEqual(opened.sort(), ['/remote/a/same', '/remote/b/same'])
      assert.deepStrictEqual(closed.sort(), opened)
    })
  })

  it.effect('refuses array fallback requirements before touching the supplied backend', () => {
    let enumerations = 0
    const backend = FileSystem.layerNoop({
      readDirectory: () =>
        Effect.sync(() => {
          enumerations++
          return ['a', 'b', 'c']
        }),
    })
    const adapter = DirectoryReader.fileSystemAdapter.pipe(Layer.provide(backend))
    return Effect.scoped(
      Effect.gen(function* () {
        const service = yield* DirectoryReader.DirectoryReader
        assert.strictEqual(
          error(yield* service.open('.', { requireBoundedEnumeration: true }).pipe(Effect.result))
            .reason,
          'unsupported',
        )
        assert.strictEqual(
          error(yield* service.open('.', { requireOpenedIdentity: true }).pipe(Effect.result))
            .reason,
          'unsupported',
        )
        assert.strictEqual(enumerations, 0)
        const reader = yield* service.open('.')
        assert.strictEqual(reader.capabilities.allocation, 'whole-directory-array')
        assert.deepStrictEqual(
          (yield* reader.next(2)).entries.map((e) => e.name),
          ['a', 'b'],
        )
        assert.deepStrictEqual(
          (yield* reader.next(2)).entries.map((e) => e.name),
          ['c'],
        )
        assert.strictEqual(enumerations, 1)
      }),
    ).pipe(
      Effect.provide(
        DirectoryReader.layer({ root: '/remote' }).pipe(Layer.provide([adapter, Path.layer])),
      ),
    )
  })
})

describe('FileWatch native coverage', () => {
  it.live('keeps callback coverage behind the actual settling readiness fence', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* temporary
        const readiness = yield* observeNativeReadiness(true)
        const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (service) =>
          service.open([{ path: '.' }, { path: 'skills', recursive: true }]),
        ).pipe(
          Effect.provide(
            FileWatch.layer({ root }).pipe(Layer.provide([readiness.adapter, NodePath.layer])),
          ),
        )
        yield* Deferred.await(readiness.held).pipe(Effect.timeout('2 seconds'))
        const events: Array<FileWatch.Event> = []
        const collecting = yield* collectInvalidations(watcher, events, readiness.startup).pipe(
          Effect.forkChild,
        )
        yield* Effect.yieldNow
        yield* fs.chmod(root, 0o750)
        yield* Deferred.await(readiness.callbackCoverage).pipe(Effect.timeout('2 seconds'))
        yield* Effect.yieldNow
        assert.isUndefined(
          collecting.pollUnsafe(),
          'A genuine callback-origin coverage rescan must not release settling readiness',
        )
        yield* readiness.release
        const consumer = yield* Fiber.join(collecting)
        const created = yield* readiness.hold
        yield* fs.makeDirectory(`${root}/skills`)
        yield* fs.writeFileString(`${root}/skills/value`, 'created')
        yield* Deferred.await(created).pipe(Effect.timeout('2 seconds'))
        const firstAttachment = readiness.handles().findLast((h) => h.path === `${root}/skills`)
        assert.isDefined(firstAttachment, 'The real recursive target handle must be installed')
        const attaching = yield* readiness.awaitAttachment(`${root}/skills`).pipe(Effect.forkChild)
        yield* Effect.yieldNow
        assert.isUndefined(attaching.pollUnsafe(), 'Installation alone must not release settling')
        yield* readiness.release
        yield* Fiber.join(attaching).pipe(Effect.timeout('2 seconds'))
        const firstIdentity = yield* fs.stat(`${root}/skills`)
        const recreated = yield* readiness.hold
        yield* fs.rename(`${root}/skills`, `${root}/old-skills`)
        yield* fs.makeDirectory(`${root}/skills`)
        yield* fs.writeFileString(`${root}/skills/value`, 'recreated')
        yield* Deferred.await(recreated).pipe(Effect.timeout('2 seconds'))
        const secondAttachment = readiness.handles().findLast((h) => h.path === `${root}/skills`)
        assert.isDefined(secondAttachment)
        assert.notStrictEqual(secondAttachment, firstAttachment)
        assert.strictEqual(firstAttachment?.closed, 1)
        const reattaching = yield* readiness
          .awaitAttachment(`${root}/skills`)
          .pipe(Effect.forkChild)
        yield* Effect.yieldNow
        assert.isUndefined(
          reattaching.pollUnsafe(),
          'A replacement handle must receive subsequent worker coverage before the next mutation',
        )
        yield* readiness.release
        yield* Fiber.join(reattaching).pipe(Effect.timeout('2 seconds'))
        const secondIdentity = yield* fs.stat(`${root}/skills`)
        assert.notStrictEqual(
          Option.getOrThrow(firstIdentity.ino),
          Option.getOrThrow(secondIdentity.ino),
        )
        yield* expectInvalidation(
          events,
          `${root}/skills/value`,
          fs.writeFileString(`${root}/skills/value`, 'continued'),
          fs
            .readFileString(`${root}/skills/value`)
            .pipe(Effect.map((content) => assert.strictEqual(content, 'continued'))),
        )
        yield* watcher.close
        yield* Fiber.join(consumer).pipe(Effect.timeout('2 seconds'))
        for (const handle of readiness.handles()) assert.strictEqual(handle.closed, 1)
      }),
    ).pipe(Effect.provide(files)),
  )

  it.live('re-reads each mutation when the real native adapter delivers only rescans', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* temporary
        const events: Array<FileWatch.Event> = []
        const readiness = yield* observeNativeReadiness()
        // Normalize actual host callbacks, retaining native acquisition, coverage and finalizers.
        // This exercises the valid Rescan-only contract without depending on FSEvents coalescing.
        const rescans = Layer.effect(
          FileWatch.Adapter,
          Effect.map(FileWatch.Adapter, (native) => ({
            subscribe: (targets, observer, options) =>
              native.subscribe(
                targets,
                {
                  emit: (event) =>
                    observer.emit(
                      event._tag === 'Change'
                        ? { _tag: 'Rescan', paths: [event.path], reason: 'native' }
                        : event,
                    ),
                  fail: observer.fail,
                },
                options,
              ),
          })),
        ).pipe(Layer.provide(readiness.adapter))
        const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (service) =>
          service.open([{ path: 'skills', recursive: true }]),
        ).pipe(
          Effect.provide(FileWatch.layer({ root }).pipe(Layer.provide([rescans, NodePath.layer]))),
        )
        assert.deepStrictEqual(watcher.coverage, {
          mode: 'native',
          recursive: 'native',
          ancestors: 'native',
          symlinks: 'file-targets',
          reliability: 'best-effort',
        })
        const consumer = yield* collectInvalidations(watcher, events, readiness.startup)
        const content = (name: string, expected: string) =>
          fs
            .readFileString(`${root}/${name}`)
            .pipe(Effect.map((actual) => assert.strictEqual(actual, expected)))
        yield* expectInvalidation(
          events,
          `${root}/skills`,
          fs
            .makeDirectory(`${root}/skills`)
            .pipe(Effect.andThen(fs.writeFileString(`${root}/skills/value`, 'created:1'))),
          content('skills/value', 'created:1'),
        )
        yield* readiness.awaitAttachment(`${root}/skills`).pipe(Effect.timeout('2 seconds'))
        const firstAttachment = readiness.handles().findLast((h) => h.path === `${root}/skills`)
        yield* expectInvalidation(
          events,
          `${root}/skills/value`,
          fs.writeFileString(`${root}/skills/value`, 'updated:2'),
          content('skills/value', 'updated:2'),
        )
        yield* expectInvalidation(
          events,
          `${root}/skills`,
          fs.rename(`${root}/skills`, `${root}/old-skills`),
          Effect.gen(function* () {
            assert.isFalse(yield* fs.exists(`${root}/skills`))
            yield* content('old-skills/value', 'updated:2')
          }),
        )
        yield* expectInvalidation(
          events,
          `${root}/skills`,
          fs
            .makeDirectory(`${root}/skills`)
            .pipe(Effect.andThen(fs.writeFileString(`${root}/skills/value`, 'recreated:3'))),
          Effect.gen(function* () {
            assert.deepStrictEqual(yield* fs.readDirectory(`${root}/skills`), ['value'])
            yield* content('skills/value', 'recreated:3')
          }),
        )
        yield* readiness.awaitAttachment(`${root}/skills`).pipe(Effect.timeout('2 seconds'))
        assert.notStrictEqual(
          readiness.handles().findLast((h) => h.path === `${root}/skills`),
          firstAttachment,
        )
        assert.strictEqual(firstAttachment?.closed, 1)
        yield* expectInvalidation(
          events,
          `${root}/skills/value`,
          fs.writeFileString(`${root}/skills/value`, 'continued:4'),
          content('skills/value', 'continued:4'),
        )
        assert.isTrue(events.every((event) => event._tag === 'Rescan'))
        yield* watcher.close
        yield* watcher.close
        yield* Fiber.join(consumer).pipe(Effect.timeout('2 seconds'))
        for (const handle of readiness.handles()) assert.strictEqual(handle.closed, 1)
        assert.deepStrictEqual(yield* watcher.events.pipe(Stream.runCollect), [])
        const count = events.length
        yield* fs.writeFileString(`${root}/skills/value`, 'after-close:5')
        yield* Effect.sleep('100 millis')
        assert.strictEqual(events.length, count)
      }),
    ).pipe(Effect.provide(files)),
  )

  it.live(
    'covers missing ancestors, creation/deletion/recreation, replacement, exclusions and file symlinks',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const root = yield* temporary
          const events: Array<FileWatch.Event> = []
          const readiness = yield* observeNativeReadiness()
          const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
            s.open(
              [
                { path: 'missing/deep/file' },
                {
                  path: 'skills',
                  recursive: true,
                  exclude: { hidden: true, names: ['node_modules'] },
                },
                { path: 'skills', exclude: { hidden: true, names: ['node_modules'] } },
              ],
              { queueCapacity: 16 },
            ),
          ).pipe(
            Effect.provide(
              FileWatch.layer({ root }).pipe(Layer.provide([readiness.adapter, NodePath.layer])),
            ),
          )
          assert.strictEqual(watcher.coverage.mode, 'native')
          assert.strictEqual(watcher.coverage.ancestors, 'native')
          assert.strictEqual(watcher.coverage.recursive, 'native')
          const consumer = yield* collectInvalidations(watcher, events, readiness.startup)
          const expectUpdate = <A, E, E2>(
            suffix: string,
            mutate: Effect.Effect<A, E>,
            verify: Effect.Effect<void, E2>,
          ) => expectInvalidation(events, `${root}/${suffix}`, mutate, verify)
          const content = (suffix: string, expected: string) =>
            fs
              .readFileString(`${root}/${suffix}`)
              .pipe(Effect.map((actual) => assert.strictEqual(actual, expected)))
          yield* expectUpdate(
            'missing/deep/file',
            fs
              .makeDirectory(`${root}/missing/deep`, { recursive: true })
              .pipe(
                Effect.andThen(
                  fs.writeFileString(`${root}/missing/deep/file`, 'missing-created:1'),
                ),
              ),
            content('missing/deep/file', 'missing-created:1'),
          )
          yield* readiness.awaitAttachment(`${root}/missing/deep`).pipe(Effect.timeout('2 seconds'))
          yield* expectUpdate(
            'missing/deep/file',
            fs.remove(`${root}/missing/deep/file`),
            fs
              .exists(`${root}/missing/deep/file`)
              .pipe(Effect.map((exists) => assert.isFalse(exists))),
          )
          yield* expectUpdate(
            'missing/deep/file',
            fs.writeFileString(`${root}/missing/deep/file`, 'missing-recreated:2'),
            content('missing/deep/file', 'missing-recreated:2'),
          )
          yield* expectUpdate(
            'skills',
            fs
              .makeDirectory(`${root}/skills/a`, { recursive: true })
              .pipe(Effect.andThen(fs.writeFileString(`${root}/skills/a/x`, 'skills-created:3'))),
            content('skills/a/x', 'skills-created:3'),
          )
          yield* readiness.awaitAttachment(`${root}/skills`).pipe(Effect.timeout('2 seconds'))
          const firstAttachment = readiness.handles().findLast((h) => h.path === `${root}/skills`)
          yield* expectUpdate(
            'skills/a/y',
            fs.writeFileString(`${root}/skills/a/y`, 'skills-child:4'),
            content('skills/a/y', 'skills-child:4'),
          )
          events.length = 0
          yield* fs.makeDirectory(`${root}/skills/node_modules/dep`, { recursive: true })
          yield* fs.writeFileString(`${root}/skills/node_modules/dep/excluded`, '')
          yield* fs.writeFileString(`${root}/skills/.hidden`, '')
          yield* fs.writeFileString(`${root}/skills/.draft`, 'skills-visible:5')
          yield* expectUpdate(
            'skills/visible',
            fs.rename(`${root}/skills/.draft`, `${root}/skills/visible`),
            content('skills/visible', 'skills-visible:5'),
          )
          yield* Effect.sleep('250 millis')
          assert.isFalse(
            events.some(
              (e) =>
                e._tag === 'Change' &&
                (e.path.includes('node_modules') ||
                  e.path.includes('/.hidden') ||
                  e.path.includes('/.draft')),
            ),
          )
          yield* content('skills/visible', 'skills-visible:5')
          yield* expectUpdate(
            'skills',
            fs.rename(`${root}/skills`, `${root}/old-skills`),
            Effect.gen(function* () {
              assert.isFalse(yield* fs.exists(`${root}/skills`))
              yield* content('old-skills/visible', 'skills-visible:5')
            }),
          )
          yield* expectUpdate(
            'skills',
            fs
              .makeDirectory(`${root}/skills`)
              .pipe(
                Effect.andThen(
                  fs.writeFileString(`${root}/skills/generation`, 'skills-recreated:6'),
                ),
              ),
            Effect.gen(function* () {
              assert.deepStrictEqual(yield* fs.readDirectory(`${root}/skills`), ['generation'])
              yield* content('skills/generation', 'skills-recreated:6')
            }),
          )
          yield* readiness.awaitAttachment(`${root}/skills`).pipe(Effect.timeout('2 seconds'))
          assert.notStrictEqual(
            readiness.handles().findLast((h) => h.path === `${root}/skills`),
            firstAttachment,
          )
          assert.strictEqual(firstAttachment?.closed, 1)
          yield* expectUpdate(
            'skills/new',
            fs.writeFileString(`${root}/skills/new`, 'skills-child-after-recreation:7'),
            content('skills/new', 'skills-child-after-recreation:7'),
          )
          yield* fs.writeFileString(`${root}/physical`, 'old')
          yield* fs.symlink(`${root}/physical`, `${root}/link`)
          yield* fs.symlink(`${root}/physical`, `${root}/link-two`)
          const linked = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
            s.open([{ path: 'link' }, { path: 'link-two' }, { path: 'physical' }]),
          ).pipe(Effect.provide(FileWatchNode.layer({ root })))
          const seen = yield* Deferred.make<void>()
          const aliases = new Set<string>()
          yield* linked.events.pipe(
            Stream.runForEach((event) =>
              Effect.gen(function* () {
                if (event._tag === 'Change') aliases.add(event.path)
                if (
                  ['link', 'link-two', 'physical'].every((name) => aliases.has(`${root}/${name}`))
                )
                  yield* Deferred.succeed(seen, undefined)
              }),
            ),
            Effect.forkScoped,
          )
          yield* Effect.sleep('600 millis')
          yield* fs.writeFileString(`${root}/physical`, 'aliases-updated:8')
          yield* Deferred.await(seen).pipe(Effect.timeout('3 seconds'))
          for (const alias of ['link', 'link-two', 'physical'])
            yield* content(alias, 'aliases-updated:8')
          yield* linked.close
          yield* watcher.close
          yield* watcher.close
          yield* Fiber.join(consumer).pipe(Effect.timeout('2 seconds'))
          for (const handle of readiness.handles()) assert.strictEqual(handle.closed, 1)
          assert.deepStrictEqual(yield* watcher.events.pipe(Stream.runCollect), [])
          const length = events.length
          yield* fs.writeFileString(`${root}/skills/after-close`, 'closed')
          yield* Effect.sleep('100 millis')
          assert.strictEqual(events.length, length)
        }),
      ).pipe(Effect.provide(files)),
  )

  it.live('a stalled native consumer receives a rescan after its bounded queue overflows', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* temporary
        const settled = yield* Deferred.make<void>()
        const delivered = yield* Deferred.make<void>()
        const nativeChanges = new Set<string>()
        let released = 0
        const observed = Layer.effect(
          FileWatch.Adapter,
          Effect.map(FileWatch.Adapter, (native) => ({
            subscribe: (targets, observer, options) =>
              Effect.gen(function* () {
                yield* Effect.addFinalizer(() => Effect.sync(() => released++))
                return yield* native.subscribe(
                  targets,
                  {
                    emit: (event) => {
                      observer.emit(event)
                      if (
                        event._tag === 'Rescan' &&
                        event.reason === 'coverage' &&
                        event.paths.includes(root)
                      )
                        Deferred.doneUnsafe(settled, Effect.void)
                      if (event._tag === 'Change' && event.path.startsWith(`${root}/burst-`)) {
                        nativeChanges.add(event.path)
                        if (nativeChanges.size >= 2) Deferred.doneUnsafe(delivered, Effect.void)
                      }
                    },
                    fail: observer.fail,
                  },
                  options,
                )
              }),
          })),
        ).pipe(Layer.provide(FileWatchNode.adapter))
        const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (service) =>
          service.open([{ path: '.' }], { queueCapacity: 1 }),
        ).pipe(
          Effect.provide(FileWatch.layer({ root }).pipe(Layer.provide([observed, NodePath.layer]))),
        )
        assert.strictEqual(watcher.coverage.mode, 'native')
        // No consumer runs until two distinct native changes reach the one-slot queue.
        // First observe the adapter's delayed startup coverage, then actual burst delivery.
        yield* Effect.gen(function* () {
          yield* Deferred.await(settled)
          yield* Effect.forEach(
            Array.from({ length: 128 }, (_, index) => index),
            (index) => fs.writeFileString(`${root}/burst-${index}`, `burst:${index}`),
            { concurrency: 16, discard: true },
          )
          yield* Deferred.await(delivered)
        }).pipe(Effect.timeout('3 seconds'))
        assert.isAtLeast(nativeChanges.size, 2)
        assert.deepStrictEqual(
          yield* watcher.events.pipe(
            Stream.take(1),
            Stream.runCollect,
            Effect.timeout('3 seconds'),
          ),
          [{ _tag: 'Rescan', paths: [root], reason: 'overflow' }],
        )
        assert.strictEqual((yield* fs.readDirectory(root)).length, 128)
        for (let index = 0; index < 128; index++)
          assert.strictEqual(yield* fs.readFileString(`${root}/burst-${index}`), `burst:${index}`)
        const events: Array<FileWatch.Event> = []
        const consumer = yield* watcher.events.pipe(
          Stream.runForEach((event) => Effect.sync(() => events.push(event))),
          Effect.forkScoped,
        )
        for (const value of ['continued:1', 'continued:2'])
          yield* expectInvalidation(
            events,
            `${root}/continued`,
            fs.writeFileString(`${root}/continued`, value),
            fs
              .readFileString(`${root}/continued`)
              .pipe(Effect.map((actual) => assert.strictEqual(actual, value))),
          )
        yield* watcher.close
        yield* watcher.close
        assert.strictEqual(released, 1)
        yield* Fiber.join(consumer).pipe(Effect.timeout('2 seconds'))
        assert.deepStrictEqual(yield* watcher.events.pipe(Stream.runCollect), [])
      }),
    ).pipe(Effect.provide(files)),
  )

  it.live('fails with a typed coverage budget instead of silently dropping ancestor watches', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const root = yield* temporary
        const outcome = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
          s.open([{ path: 'a/b/c' }], { maxWatchers: 1 }),
        ).pipe(Effect.provide(FileWatchNode.layer({ root })), Effect.result)
        assert.strictEqual(error(outcome).reason, 'coverage')
      }),
    ).pipe(Effect.provide(files)),
  )
})

describe('FileWatch custom adapter and Clock modes', () => {
  it.effect(
    'isolates remote roots, bounds adapter invalidations and rejects relative event paths',
    () => {
      const subscribed: Array<string> = []
      const released: Array<string> = []
      const observers = new Map<string, FileWatch.Observer>()
      const adapter = Layer.succeed(FileWatch.Adapter, {
        subscribe: Effect.fn(function* (targets, observer) {
          const target = targets[0]!.path
          subscribed.push(target)
          observers.set(target, observer)
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              released.push(target)
            }),
          )
          return {
            mode: 'native',
            recursive: 'native',
            ancestors: 'native',
            symlinks: 'unsupported',
            reliability: 'best-effort',
          } as const
        }),
      })
      const run = (root: string) =>
        Effect.scoped(
          Effect.gen(function* () {
            const service = yield* FileWatch.FileWatch
            assert.strictEqual(
              error(yield* service.open([{ path: '../escape' }]).pipe(Effect.result)).reason,
              'permission',
            )
            const watcher = yield* service.open([
              { path: 'tree', recursive: true, exclude: { hidden: true } },
            ])
            const observer = observers.get(`${root}/tree`)!
            observer.emit({ _tag: 'Change', path: '/unrelated/file', kind: 'change' })
            observer.emit({
              _tag: 'Rescan',
              paths: ['/unrelated', `${root}/tree/.hidden`],
              reason: 'native',
            })
            observer.emit({ _tag: 'Rescan', paths: ['/'], reason: 'coverage' })
            assert.deepStrictEqual(yield* watcher.events.pipe(Stream.take(1), Stream.runCollect), [
              { _tag: 'Rescan', paths: [`${root}/tree`], reason: 'coverage' },
            ])
            observer.emit({ _tag: 'Change', path: 'relative', kind: 'change' })
            const failure = error(yield* watcher.events.pipe(Stream.runCollect, Effect.result))
            assert.strictEqual(failure.reason, 'backend')
            assert.strictEqual(failure.operation, 'event')
          }),
        ).pipe(
          Effect.provide(
            FileWatch.layer({ root, pathPolicy: 'within-root' }).pipe(
              Layer.provide([adapter, Path.layer]),
            ),
          ),
        )
      return Effect.gen(function* () {
        yield* Effect.all([run('/remote/a'), run('/remote/b')], { concurrency: 'unbounded' })
        assert.deepStrictEqual(subscribed.sort(), ['/remote/a/tree', '/remote/b/tree'])
        assert.deepStrictEqual(released.sort(), subscribed)
      })
    },
  )

  it.effect('bounds queue overflow with a required rescan and rejects events after close', () => {
    let emit: FileWatch.Observer['emit'] = () => {}
    let releases = 0
    const adapter = Layer.succeed(FileWatch.Adapter, {
      subscribe: Effect.fn(function* (targets, observer) {
        assert.strictEqual(targets[0]?.path, '/remote/tree')
        emit = observer.emit
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            releases++
          }),
        )
        return {
          mode: 'native',
          recursive: 'native',
          ancestors: 'native',
          symlinks: 'unsupported',
          reliability: 'best-effort',
        } as const
      }),
    })
    return Effect.scoped(
      Effect.gen(function* () {
        const service = yield* FileWatch.FileWatch
        const watcher = yield* service.open(
          [{ path: 'tree', recursive: true, exclude: { hidden: true } }],
          { queueCapacity: 2 },
        )
        emit({ _tag: 'Change', path: '/remote/tree/.hidden', kind: 'change' })
        for (let i = 0; i < 10000; i++)
          emit({ _tag: 'Change', path: `/remote/tree/${i}`, kind: 'change' })
        const first = yield* watcher.events.pipe(Stream.take(1), Stream.runCollect)
        assert.deepStrictEqual(first, [
          { _tag: 'Rescan', paths: ['/remote/tree'], reason: 'overflow' },
        ])
        emit({ _tag: 'Change', path: '/remote/tree/new', kind: 'rename' })
        assert.deepStrictEqual(yield* watcher.events.pipe(Stream.take(1), Stream.runCollect), [
          { _tag: 'Change', path: '/remote/tree/new', kind: 'rename' },
        ])
        yield* watcher.close
        yield* watcher.close
        emit({ _tag: 'Change', path: '/remote/tree/late', kind: 'change' })
        assert.deepStrictEqual(yield* watcher.events.pipe(Stream.runCollect), [])
        assert.strictEqual(releases, 1)
      }),
    ).pipe(
      Effect.provide(
        FileWatch.layer({ root: '/remote' }).pipe(Layer.provide([adapter, Path.layer])),
      ),
    )
  })

  it.effect('uses Effect Clock for explicit periodic rescan and closes on interruption', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const service = yield* FileWatch.FileWatch
        const watcher = yield* service.open([{ path: 'missing' }])
        assert.strictEqual(watcher.coverage.mode, 'polling')
        const result = yield* watcher.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild,
        )
        yield* TestClock.adjust('1 second')
        assert.deepStrictEqual(yield* Fiber.join(result), [
          { _tag: 'Rescan', paths: ['/remote/missing'], reason: 'poll' },
        ])
        yield* watcher.close
        const final = yield* watcher.events.pipe(Stream.runCollect)
        assert.deepStrictEqual(final, [])
      }),
    ).pipe(
      Effect.provide(
        FileWatch.layer({ root: '/remote' }).pipe(
          Layer.provide([FileWatch.pollingAdapter(1000), Path.layer]),
        ),
      ),
    ),
  )

  it.effect(
    'preserves custom acquisition failures and finalizes a partially acquired subscription',
    () => {
      const failure = new FileWatch.FileWatchError({
        reason: 'unsupported',
        operation: 'remote-connect',
        path: '/remote/file',
        message: 'Remote watch is unsupported',
      })
      let released = false
      const adapter = Layer.succeed(FileWatch.Adapter, {
        subscribe: Effect.fn(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              released = true
            }),
          )
          return yield* failure
        }),
      })
      return Effect.scoped(
        Effect.gen(function* () {
          const service = yield* FileWatch.FileWatch
          assert.strictEqual(
            error(yield* service.open([{ path: 'file' }]).pipe(Effect.result)),
            failure,
          )
          assert.isTrue(released)
        }),
      ).pipe(
        Effect.provide(
          FileWatch.layer({ root: '/remote' }).pipe(Layer.provide([adapter, Path.layer])),
        ),
      )
    },
  )

  it.effect('interruption closes the custom watch and its pending pull exactly once', () => {
    let releases = 0
    const ready = Deferred.make<FileWatch.Watch>()
    const adapter = Layer.succeed(FileWatch.Adapter, {
      subscribe: Effect.fn(function* () {
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            releases++
          }),
        )
        return {
          mode: 'native',
          recursive: 'not-requested',
          ancestors: 'native',
          symlinks: 'unsupported',
          reliability: 'best-effort',
        } as const
      }),
    })
    return Effect.gen(function* () {
      const signal = yield* ready
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const watch = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
            s.open([{ path: 'file' }]),
          )
          yield* Deferred.succeed(signal, watch)
          yield* watch.events.pipe(Stream.runDrain)
        }),
      ).pipe(
        Effect.provide(
          FileWatch.layer({ root: '/remote' }).pipe(Layer.provide([adapter, Path.layer])),
        ),
        Effect.forkChild,
      )
      const watch = yield* Deferred.await(signal)
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(releases, 1)
      assert.deepStrictEqual(yield* watch.events.pipe(Stream.runCollect), [])
    })
  })
})
