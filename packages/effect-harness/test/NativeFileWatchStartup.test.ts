import { assert, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import { basename, resolve } from 'node:path'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Scope from 'effect/Scope'
import * as TestClock from 'effect/testing/TestClock'
import { vi } from 'vitest'
import * as FileWatch from '../src/FileWatch.ts'
import * as FileWatchNode from '../src/FileWatchNode.ts'

interface NativeCallback {
  readonly path: string
  readonly kind: import('node:fs').WatchEventType
  readonly filename: string | null
}
interface NativeHandle {
  readonly forward: import('node:fs').WatchListener<string>
  closed: number
}
const observations = vi.hoisted(
  (): {
    active: NativeCallback | undefined
    handles: Array<NativeHandle>
    afterCallback: ((callback: NativeCallback) => void) | undefined
  } => ({ active: undefined, handles: [], afterCallback: undefined }),
)
vi.mock('node:fs', async (importOriginal) => {
  const fs = await importOriginal<typeof import('node:fs')>()
  const watch = (
    path: import('node:fs').PathLike,
    options: import('node:fs').WatchOptionsWithStringEncoding,
    listener: import('node:fs').WatchListener<string>,
  ) => {
    const forward: import('node:fs').WatchListener<string> = (kind, filename) => {
      const callback = { path: path.toString(), kind, filename }
      const previous = observations.active
      observations.active = callback
      try {
        listener(kind, filename)
      } finally {
        observations.active = previous
        observations.afterCallback?.(callback)
      }
    }
    const watcher = fs.watch(path, options, forward)
    const handle = { forward, closed: 0 }
    observations.handles.push(handle)
    const close = watcher.close.bind(watcher)
    watcher.close = () => {
      handle.closed++
      close()
    }
    return watcher
  }
  return { ...fs, watch }
})

it.effect(
  'settles startup coverage at exactly 500ms while verifying real burst content and handle cleanup',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: 'native-startup-' })
        const adapter = yield* FileWatch.Adapter
        const clock = yield* Clock.Clock
        const watchScope = yield* Scope.make()
        yield* Effect.addFinalizer((exit) => Scope.close(watchScope, exit))
        const firstHandle = observations.handles.length
        const sleeps: Array<Fiber.Fiber<void>> = []
        const ready = yield* Deferred.make<{ start: number; duration: number }>()
        const events: Array<{
          event: FileWatch.Event
          native: NativeCallback | undefined
        }> = []
        const settling = () =>
          events
            .filter(
              ({ event, native }) =>
                !native && event._tag === 'Rescan' && event.reason === 'coverage',
            )
            .map(({ event }) => event)
        // Start the actual TestClock sleep synchronously before acknowledging admission.
        // The joined child remains owned by the settling worker and its subscription Scope.
        const observedClock: Clock.Clock = {
          currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
          currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
          monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
          currentTimeMillis: clock.currentTimeMillis,
          currentTimeNanos: clock.currentTimeNanos,
          monotonicTimeNanos: clock.monotonicTimeNanos,
          sleep: (duration) =>
            Effect.gen(function* () {
              const start = clock.currentTimeMillisUnsafe()
              const sleeper = yield* Effect.forkChild(clock.sleep(duration), {
                startImmediately: true,
              })
              sleeps.push(sleeper)
              yield* Deferred.succeed(ready, { start, duration: Duration.toMillis(duration) })
              yield* Fiber.join(sleeper)
            }),
        }
        yield* adapter
          .subscribe(
            [{ path: root }],
            {
              emit: (event) => events.push({ event, native: observations.active }),
              fail: (error) => assert.fail(error.message),
            },
            {},
          )
          .pipe(
            Effect.provideService(Scope.Scope, watchScope),
            Effect.provideService(Clock.Clock, observedClock),
          )
        assert.deepStrictEqual(yield* Deferred.await(ready), { start: 0, duration: 500 })
        assert.deepStrictEqual(settling(), [])

        yield* Effect.forEach(
          Array.from({ length: 128 }, (_, index) => index),
          (index) => fs.writeFileString(`${root}/burst-${index}`, `burst:${index}`),
          { concurrency: 16, discard: true },
        )
        assert.deepStrictEqual(
          settling(),
          [],
          'Settling coverage must follow its admitted deadline',
        )
        yield* TestClock.adjust('499 millis')
        assert.deepStrictEqual(settling(), [])
        yield* TestClock.adjust('1 millis')
        assert.deepStrictEqual(settling(), [{ _tag: 'Rescan', paths: [root], reason: 'coverage' }])
        assert.strictEqual((yield* fs.readDirectory(root)).length, 128)
        for (let index = 0; index < 128; index++)
          assert.strictEqual(yield* fs.readFileString(`${root}/burst-${index}`), `burst:${index}`)

        yield* Scope.close(watchScope, Exit.void)
        const handles = observations.handles.slice(firstHandle)
        assert.isAbove(handles.length, 0)
        for (const handle of handles) assert.strictEqual(handle.closed, 1)
        for (const sleeper of sleeps) assert.isDefined(sleeper.pollUnsafe())
        const afterClose = events.length
        for (const handle of handles) handle.forward('rename', basename(root))
        yield* TestClock.adjust('1000 millis')
        assert.strictEqual(
          events.length,
          afterClose,
          'Closed subscriptions reject queued late callbacks',
        )
      }),
    ).pipe(Effect.provide([NodeFileSystem.layer, FileWatchNode.adapter])),
)

// OS readiness has no TestClock acknowledgement. Exercise real callbacks on the live Clock.
it.live('forwards a genuine ancestor callback and its complete synchronous coverage output', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: 'native-ancestor-' })
      const adapter = yield* FileWatch.Adapter
      const scope = yield* Scope.make()
      yield* Effect.addFinalizer((exit) => Scope.close(scope, exit))
      const firstHandle = observations.handles.length
      const settled = yield* Deferred.make<void>()
      const ancestor = yield* Deferred.make<{
        callback: NativeCallback
        events: Array<FileWatch.Event>
      }>()
      const events: Array<{ event: FileWatch.Event; native: NativeCallback | undefined }> = []
      observations.afterCallback = (callback) => {
        if (callback.filename !== null && resolve(callback.path, callback.filename) === root)
          Deferred.doneUnsafe(
            ancestor,
            Effect.succeed({
              callback,
              events: events.filter(({ native }) => native === callback).map(({ event }) => event),
            }),
          )
      }
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          observations.afterCallback = undefined
        }),
      )
      yield* adapter
        .subscribe(
          [{ path: root }],
          {
            emit: (event) => {
              events.push({ event, native: observations.active })
              if (!observations.active && event._tag === 'Rescan' && event.reason === 'coverage')
                Deferred.doneUnsafe(settled, Effect.void)
            },
            fail: (error) => assert.fail(error.message),
          },
          {},
        )
        .pipe(Effect.provideService(Scope.Scope, scope))
      yield* Deferred.await(settled).pipe(Effect.timeout('2 seconds'))
      yield* fs.chmod(root, 0o750)
      const immediate = yield* Deferred.await(ancestor).pipe(Effect.timeout('2 seconds'))
      assert.deepStrictEqual(immediate.events, [
        { _tag: 'Change', path: root, kind: immediate.callback.kind },
        { _tag: 'Rescan', paths: [root], reason: 'coverage' },
      ])
      yield* Scope.close(scope, Exit.void)
      const handles = observations.handles.slice(firstHandle)
      assert.isAbove(handles.length, 0)
      for (const handle of handles) assert.strictEqual(handle.closed, 1)
      const boundary = events.length
      yield* fs.writeFileString(`${root}/after-close`, 'closed')
      for (const handle of handles) handle.forward('rename', basename(root))
      assert.strictEqual(events.length, boundary, 'Closed subscriptions reject native callbacks')
    }),
  ).pipe(Effect.provide([NodeFileSystem.layer, FileWatchNode.adapter])),
)
