import { assert, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Result from 'effect/Result'
import * as Stream from 'effect/Stream'
import { vi } from 'vitest'
import * as FileWatch from '../src/FileWatch.ts'
import * as FileWatchNode from '../src/FileWatchNode.ts'

const counters = vi.hoisted(() => ({ acquired: 0, closed: 0, recursiveAttempts: 0 }))
vi.mock('node:fs', async (importOriginal) => {
  const fs = await importOriginal<typeof import('node:fs')>()
  const watch = (
    path: import('node:fs').PathLike,
    options: import('node:fs').WatchOptionsWithStringEncoding,
    listener: import('node:fs').WatchListener<string>,
  ) => {
    if (options.recursive) {
      counters.recursiveAttempts++
      throw Object.assign(new Error('Host does not support recursive watches'), {
        code: 'ERR_FEATURE_UNAVAILABLE_ON_PLATFORM',
      })
    }
    const watcher = fs.watch(path, options, listener)
    counters.acquired++
    const close = watcher.close.bind(watcher)
    watcher.close = () => {
      counters.closed++
      close()
    }
    return watcher
  }
  return { ...fs, watch }
})

it.live(
  'keeps native recursive capability failure typed and closes partial ancestor acquisition',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: 'unsupported-native-watch-' })
        const result = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
          s.open([{ path: '.', recursive: true }]),
        ).pipe(Effect.provide(FileWatchNode.layer({ root })), Effect.result)
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.strictEqual(result.failure.reason, 'unsupported')
          assert.strictEqual(result.failure.operation, 'recursive-watch')
        }
        assert.strictEqual(counters.recursiveAttempts, 1)
        assert.isAbove(counters.acquired, 0)
        assert.strictEqual(counters.closed, counters.acquired)
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
)

it.live(
  'interruption closes every acquired native watcher exactly once and ends pending pulls',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.makeTempDirectoryScoped({ prefix: 'interrupted-native-watch-' })
        const acquired = counters.acquired
        const closed = counters.closed
        const ready = yield* Deferred.make<FileWatch.Watch>()
        const fiber = yield* Effect.scoped(
          Effect.gen(function* () {
            const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (service) =>
              service.open([{ path: 'missing/file' }]),
            )
            yield* Deferred.succeed(ready, watcher)
            yield* watcher.events.pipe(Stream.runDrain)
          }),
        ).pipe(Effect.provide(FileWatchNode.layer({ root })), Effect.forkChild)
        const watcher = yield* Deferred.await(ready)
        assert.isAbove(counters.acquired - acquired, 0)
        yield* Fiber.interrupt(fiber)
        assert.strictEqual(counters.closed - closed, counters.acquired - acquired)
        yield* watcher.close
        assert.strictEqual(counters.closed - closed, counters.acquired - acquired)
        assert.deepStrictEqual(yield* watcher.events.pipe(Stream.runCollect), [])
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
)
