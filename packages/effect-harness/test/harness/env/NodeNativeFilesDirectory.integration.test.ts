import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'
import { assert, describe, it } from '@effect/vitest'
import { vi } from 'vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import { Env, NativeFiles } from 'effect-harness/Env'
import { withEnv } from '../tools/Helpers.ts'

interface WatcherProbe {
  readonly close: () => void
  readonly emit: (event: string, ...args: ReadonlyArray<unknown>) => boolean
  readonly listenerCount: (event: string) => number
}
const native = vi.hoisted(() => ({
  path: '',
  watcher: undefined as WatcherProbe | undefined,
  beforeClose: undefined as (() => Promise<void>) | undefined,
}))
vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>()
  return {
    ...actual,
    watch: (...args: Parameters<typeof actual.watch>) => {
      const watcher = actual.watch(...args)
      if (args[0] === native.path) {
        native.watcher = watcher
        const close = watcher.close.bind(watcher)
        Object.defineProperty(watcher, 'close', {
          value: () => {
            if (native.beforeClose === undefined) close()
            else void native.beforeClose().then(close)
          },
        })
      }
      return watcher
    },
  }
})

describe('NodeNativeFilesDirectory', () => {
  const directory = Effect.gen(function* () {
    const env = yield* Env
    const files = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layerNative))
    yield* Effect.sync(() => {
      native.path = env.cwd
      native.watcher = undefined
      native.beforeClose = undefined
    })
    return yield* files.watchDirectory(env.cwd)
  })
  const noListeners = () => {
    const watcher = native.watcher
    assert.ok(watcher)
    for (const event of ['change', 'error', 'close'])
      assert.strictEqual(watcher.listenerCount(event), 0)
  }

  describe('NativeDirectory', () => {
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'actual native error callback fails changes with its exact Error and removes listeners',
      () =>
        withEnv(
          Effect.gen(function* () {
            const notifications = yield* directory
            const consuming = yield* notifications.changes.pipe(Stream.runDrain, Effect.forkChild)
            yield* notifications.started
            const watcher = native.watcher
            assert.ok(watcher)
            const error = Object.assign(new Error('injected native I/O error'), { code: 'EIO' })
            yield* Effect.sync(() => watcher.emit('error', error))
            const caught = yield* Effect.flip(Fiber.join(consuming)).pipe(Effect.timeout(2000))
            assert.strictEqual(caught.cause, error)
            assert.strictEqual(caught.reason._tag, 'FileUnknown')
            assert.strictEqual(caught.code, 'unknown')
            noListeners()
          }),
        ),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live('native null filename produces undefined and finalizes the native handle', () =>
      withEnv(
        Effect.gen(function* () {
          const notifications = yield* directory
          const consuming = yield* notifications.changes.pipe(
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild,
          )
          yield* notifications.started
          const watcher = native.watcher
          assert.ok(watcher)
          yield* Effect.sync(() => watcher.emit('change', 'change', null))
          assert.deepStrictEqual(yield* Fiber.join(consuming).pipe(Effect.timeout(2000)), [
            undefined,
          ])
          noListeners()
        }),
      ),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'Scope.close waits actual native close notification before queue and consumer settle',
      () =>
        withEnv(
          Effect.gen(function* () {
            const notifications = yield* directory
            const owner = yield* Scope.fork(yield* Scope.Scope)
            const consuming = yield* notifications.changes.pipe(
              Stream.runDrain,
              Effect.forkScoped,
              Scope.provide(owner),
            )
            yield* notifications.started
            const closingNative = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, undefined).pipe(Effect.asVoid),
            )
            const services = yield* Effect.context<never>()
            yield* Effect.sync(() => {
              native.beforeClose = async () => {
                Effect.runSyncWith(services)(Deferred.succeed(closingNative, undefined))
                await Effect.runPromiseWith(services)(Deferred.await(release))
              }
            })
            const stopped = yield* Ref.make(false)
            const closing = yield* Scope.close(owner, Exit.void).pipe(
              Effect.andThen(Ref.set(stopped, true)),
              Effect.forkChild,
            )
            yield* Deferred.await(closingNative)
            assert.strictEqual(yield* Ref.get(stopped), false)
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(closing)
            const exit = yield* Fiber.await(consuming)
            assert.strictEqual(exit._tag, 'Failure')
            if (exit._tag === 'Failure')
              assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
            noListeners()
            yield* Scope.close(owner, Exit.void)
          }),
        ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('spontaneous native close completes changes and removes all owned listeners', () =>
      withEnv(
        Effect.gen(function* () {
          const notifications = yield* directory
          const consuming = yield* notifications.changes.pipe(Stream.runDrain, Effect.forkChild)
          yield* notifications.started
          const watcher = native.watcher
          assert.ok(watcher)
          yield* Effect.sync(() => watcher.close())
          yield* Fiber.join(consuming).pipe(Effect.timeout(2000))
          noListeners()
        }),
      ),
    )
  })
})
