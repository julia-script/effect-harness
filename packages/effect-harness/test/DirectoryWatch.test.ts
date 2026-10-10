import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
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
import * as DirectoryReader from '../src/DirectoryReader.ts'
import * as DirectoryReaderNode from '../src/DirectoryReaderNode.ts'
import * as FileWatch from '../src/FileWatch.ts'
import * as FileWatchNode from '../src/FileWatchNode.ts'

const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const temporary = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.makeTempDirectoryScoped({ prefix: 'directory-watch-' })
})
const error = <A, E>(value: Result.Result<A, E>): E => {
  if (Result.isFailure(value)) return value.failure
  return assert.fail('Expected a typed failure')
}

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
  it.live(
    'covers missing ancestors, creation/deletion/recreation, replacement, exclusions and file symlinks',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const root = yield* temporary
          const events: Array<FileWatch.Event> = []
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
          ).pipe(Effect.provide(FileWatchNode.layer({ root })))
          assert.strictEqual(watcher.coverage.mode, 'native')
          assert.strictEqual(watcher.coverage.ancestors, 'native')
          assert.strictEqual(watcher.coverage.recursive, 'native')
          yield* watcher.events.pipe(
            Stream.runForEach((event) => Effect.sync(() => events.push(event))),
            Effect.forkScoped,
          )
          // Wait for FSEvents startup; no tests pretend host watches are instant/lossless.
          yield* Effect.sleep('600 millis')
          const expectChange = <A, E>(suffix: string, mutate: Effect.Effect<A, E>) =>
            Effect.gen(function* () {
              events.length = 0
              yield* mutate
              for (let i = 0; i < 100; i++) {
                if (events.some((e) => e._tag === 'Change' && e.path === `${root}/${suffix}`))
                  return
                yield* Effect.sleep('20 millis')
              }
              assert.fail(`Missing native invalidation for ${suffix}: ${JSON.stringify(events)}`)
            })
          yield* expectChange(
            'missing/deep/file',
            fs
              .makeDirectory(`${root}/missing/deep`, { recursive: true })
              .pipe(Effect.andThen(fs.writeFileString(`${root}/missing/deep/file`, 'first'))),
          )
          yield* expectChange('missing/deep/file', fs.remove(`${root}/missing/deep/file`))
          yield* expectChange(
            'missing/deep/file',
            fs.writeFileString(`${root}/missing/deep/file`, 'second'),
          )
          yield* expectChange(
            'skills',
            fs
              .makeDirectory(`${root}/skills/a`, { recursive: true })
              .pipe(Effect.andThen(fs.writeFileString(`${root}/skills/a/x`, 'x'))),
          )
          yield* Effect.sleep('600 millis')
          yield* expectChange('skills/a/y', fs.writeFileString(`${root}/skills/a/y`, 'y'))
          events.length = 0
          yield* fs.makeDirectory(`${root}/skills/node_modules/dep`, { recursive: true })
          yield* fs.writeFileString(`${root}/skills/node_modules/dep/excluded`, '')
          yield* fs.writeFileString(`${root}/skills/.hidden`, '')
          yield* fs.writeFileString(`${root}/skills/.draft`, 'draft')
          yield* fs.rename(`${root}/skills/.draft`, `${root}/skills/visible`)
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
          assert.isTrue(
            events.some((e) => e._tag === 'Change' && e.path.endsWith('/skills/visible')),
          )
          yield* expectChange('skills', fs.rename(`${root}/skills`, `${root}/old-skills`))
          yield* expectChange('skills', fs.makeDirectory(`${root}/skills`))
          yield* Effect.sleep('600 millis')
          yield* expectChange('skills/new', fs.writeFileString(`${root}/skills/new`, 'new'))
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
          yield* fs.writeFileString(`${root}/physical`, 'changed')
          yield* Deferred.await(seen).pipe(Effect.timeout('3 seconds'))
          yield* linked.close
          yield* watcher.close
          yield* watcher.close
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
        const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (service) =>
          service.open([{ path: '.' }], { queueCapacity: 1 }),
        ).pipe(Effect.provide(FileWatchNode.layer({ root })))
        // A stalled consumer keeps the startup invalidation queued while real host changes arrive.
        yield* Effect.sleep('600 millis')
        yield* Effect.forEach(
          Array.from({ length: 128 }, (_, index) => index),
          (index) => fs.writeFileString(`${root}/burst-${index}`, ''),
          { concurrency: 16, discard: true },
        )
        yield* Effect.sleep('100 millis')
        assert.deepStrictEqual(
          yield* watcher.events.pipe(
            Stream.take(1),
            Stream.runCollect,
            Effect.timeout('3 seconds'),
          ),
          [{ _tag: 'Rescan', paths: [root], reason: 'overflow' }],
        )
        yield* watcher.close
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
