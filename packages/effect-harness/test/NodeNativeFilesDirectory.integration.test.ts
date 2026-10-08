import * as Context from 'effect/Context'

// effect-nit-allow P8-tests-import-public-specifiers: this same-package fixture delegates real Node resource acquisition through the null-exported native adapter; package-denial tests remain unchanged.
// effect-nit-allow P9-no-internal-cross-import: this same-package fixture delegates real Node resource acquisition through the null-exported native adapter; package-denial tests remain unchanged.
import * as NodeResourceAdapter from '../src/internal/NodeResourceAdapter.ts'

import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'

import { assert, describe, it } from '@effect/vitest'

import * as Cause from 'effect/Cause'

import * as Effect from 'effect/Effect'

import * as Exit from 'effect/Exit'

import * as Fiber from 'effect/Fiber'

import * as Ref from 'effect/Ref'

import * as Scope from 'effect/Scope'

import * as Stream from 'effect/Stream'

import { Env } from 'effect-harness/Env'
import { NativeFiles } from 'effect-harness/NativeFiles'

import { withEnv as withBaseEnv } from './EnvFixture.ts'

interface WatcherProbe {
  readonly close: () => void
  readonly emit: (event: string, ...args: ReadonlyArray<unknown>) => boolean
  readonly listenerCount: (event: string) => number
}
class NativeProbe extends Context.Service<NativeProbe, ReturnType<typeof makeState>>()(
  'effect-harness/test/env/NodeNativeFilesDirectory/NativeProbe',
) {}
const makeState = () => ({
  path: '',
  watcher: undefined as WatcherProbe | undefined,
  beforeClose: undefined as (() => Promise<void>) | undefined,
})
const withEnv = <A, E, R>(program: Effect.Effect<A, E, R | NativeProbe>) =>
  Effect.gen(function* () {
    const native = yield* Effect.sync(makeState)
    const adapter = NodeResourceAdapter.NodeResourceAdapter.of({
      ...NodeResourceAdapter.native,
      watch: new Proxy(NodeResourceAdapter.native.watch, {
        apply: (target, receiver, args) => {
          const watcher: ReturnType<typeof NodeResourceAdapter.native.watch> = Reflect.apply(
            target,
            receiver,
            args,
          )
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
      }),
    })
    return yield* withBaseEnv(program.pipe(Effect.provideService(NativeProbe, native))).pipe(
      Effect.provideService(NodeResourceAdapter.NodeResourceAdapter, adapter),
    )
  })

describe('NodeNativeFilesDirectory', () => {
  const directory = Effect.gen(function* () {
    const native = yield* NativeProbe
    const env = yield* Env
    const files = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layer))
    yield* Effect.sync(() => {
      native.path = env.cwd
      native.watcher = undefined
      native.beforeClose = undefined
    })
    return yield* files.watchDirectory(env.cwd)
  })
  const noListeners = () =>
    Effect.map(NativeProbe, (native) => {
      const watcher = native.watcher
      assert.ok(watcher)
      for (const event of ['change', 'error', 'close'])
        assert.strictEqual(watcher.listenerCount(event), 0)
    })

  describe('NativeDirectory', () => {
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'actual native error callback fails changes with its exact Error and removes listeners',
      () =>
        withEnv(
          Effect.gen(function* () {
            const native = yield* NativeProbe
            const notifications = yield* directory
            const consuming = yield* notifications.changes.pipe(Stream.runDrain, Effect.forkChild)
            yield* notifications.started
            const watcher = native.watcher
            assert.ok(watcher)
            const error = Object.assign(new Error('injected native I/O error'), { code: 'EIO' })
            yield* Effect.sync(() => watcher.emit('error', error))
            const caught = yield* Effect.flip(Fiber.join(consuming)).pipe(Effect.timeout(2000))
            assert.strictEqual(caught.cause, error)
            assert.strictEqual(caught.reason._tag, 'FileUnknownError')
            assert.strictEqual(caught.code, 'unknown')
            yield* noListeners()
          }),
        ),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live('native null filename produces undefined and finalizes the native handle', () =>
      withEnv(
        Effect.gen(function* () {
          const native = yield* NativeProbe
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
          yield* noListeners()
        }),
      ),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'Scope.close waits actual native close notification before queue and consumer settle',
      () =>
        withEnv(
          Effect.gen(function* () {
            const native = yield* NativeProbe
            const notifications = yield* directory
            const owner = yield* Scope.fork(yield* Scope.Scope)
            const consuming = yield* notifications.changes.pipe(
              Stream.runDrain,
              Effect.forkScoped,
              Scope.provide(owner),
            )
            yield* notifications.started
            const closingNative = yield* Effect.sync(() => Promise.withResolvers<void>())
            const release = yield* Effect.sync(() => Promise.withResolvers<void>())
            yield* Effect.addFinalizer(() => Effect.sync(() => release.resolve()))
            yield* Effect.sync(() => {
              native.beforeClose = async () => {
                closingNative.resolve()
                await release.promise
              }
            })
            const stopped = yield* Ref.make(false)
            const closing = yield* Scope.close(owner, Exit.void).pipe(
              Effect.andThen(Ref.set(stopped, true)),
              Effect.forkChild,
            )
            yield* Effect.promise(() => closingNative.promise)
            assert.strictEqual(yield* Ref.get(stopped), false)
            yield* Effect.sync(() => release.resolve())
            yield* Fiber.join(closing)
            const exit = yield* Fiber.await(consuming)
            assert.strictEqual(exit._tag, 'Failure')
            if (exit._tag === 'Failure')
              assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
            yield* noListeners()
            yield* Scope.close(owner, Exit.void)
          }),
        ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('spontaneous native close completes changes and removes all owned listeners', () =>
      withEnv(
        Effect.gen(function* () {
          const native = yield* NativeProbe
          const notifications = yield* directory
          const consuming = yield* notifications.changes.pipe(Stream.runDrain, Effect.forkChild)
          yield* notifications.started
          const watcher = native.watcher
          assert.ok(watcher)
          yield* Effect.sync(() => watcher.close())
          yield* Fiber.join(consuming).pipe(Effect.timeout(2000))
          yield* noListeners()
        }),
      ),
    )
  })
})
