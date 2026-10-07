import * as NodeNativeFiles from '@effect-harness/harness/NodeNativeFiles'
import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Queue from 'effect/Queue'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import {
  Env,
  NativeFiles,
  FileError,
  FileNotFound,
  FilePermissionDenied,
  FileNotSupported,
  FileUnknown,
} from '@effect-harness/harness/Env'
// effect-review-allow P8-tests-import-public-specifiers: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
import * as Watch from '../../src/env/internal/watch.ts'
import * as DirectoryFixture from '../tools/DirectoryFixture.ts'
import { withEnv } from '../tools/Helpers.ts'

describe('WatchChannels', () => {
  const nativeResult = <A>(effect: Effect.Effect<A, FileError>) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: 1000,
        orElse: () => Effect.die('Native directory stream did not settle'),
      }),
    )

  const fixture = Effect.gen(function* () {
    const env = yield* Env
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const native = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layerNative))
    const owner = yield* Scope.fork(yield* Scope.Scope)
    return { env, fs, path, native, owner }
  })

  describe('DirectoryChannels', () => {
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('Watch acquisition waits for delayed observable native installation', () =>
      withEnv(
        Effect.gen(function* () {
          const { env, fs, path, native, owner } = yield* fixture
          const admitted = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const handshake = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
          const settled = yield* Ref.make(false)
          let active = 0
          let first = true
          const opening = yield* Watch.make({
            fs: fs,
            path: path,
            native: {
              ...native,
              watchDirectory: () =>
                DirectoryFixture.notifications(() =>
                  Effect.acquireRelease(
                    Effect.gen(function* () {
                      if (first) {
                        first = false
                        yield* Deferred.succeed(admitted, undefined)
                        yield* Deferred.await(release)
                      }
                      active++
                    }),
                    () =>
                      Effect.sync(() => {
                        active--
                      }),
                  ),
                ).pipe(
                  Effect.map((notifications) => ({
                    ...notifications,
                    started: Deferred.succeed(handshake, undefined).pipe(
                      Effect.andThen(notifications.started),
                    ),
                  })),
                ),
            },
            targets: [{ path: env.cwd }],
          }).pipe(
            Scope.provide(owner),
            Effect.tap(() => Ref.set(settled, true)),
            Effect.forkChild,
          )
          yield* Deferred.await(admitted)
          yield* Deferred.await(handshake).pipe(Effect.timeout(2000))
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(settled), false)
          yield* Deferred.succeed(release, undefined)
          const watcher = yield* Fiber.join(opening)
          assert.strictEqual(watcher.mode, 'native')
          assert.strictEqual(active > 0, true)
          yield* Scope.close(owner, Exit.void)
          assert.strictEqual(active, 0)
        }),
      ),
    )
    // This conformance case acquires real host filesystem/process resources; their completion callbacks are independent of TestClock.
    it.live('interrupting startup waits protected installation then releases every producer', () =>
      withEnv(
        Effect.gen(function* () {
          const { env, fs, path, native, owner } = yield* fixture
          const admitted = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
          let active = 0
          const opening = yield* Watch.make({
            fs: fs,
            path: path,
            native: {
              ...native,
              watchDirectory: () =>
                DirectoryFixture.notifications(() =>
                  Effect.acquireRelease(
                    Deferred.succeed(admitted, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                      Effect.andThen(
                        Effect.sync(() => {
                          active++
                        }),
                      ),
                    ),
                    () =>
                      Effect.sync(() => {
                        active--
                      }),
                  ),
                ),
            },
            targets: [{ path: env.cwd }],
          }).pipe(Scope.provide(owner), Effect.forkChild)
          yield* Deferred.await(admitted)
          const stoppingStarted = yield* Deferred.make<void>()
          const stopped = yield* Ref.make(false)
          const stopping = yield* Deferred.succeed(stoppingStarted, undefined).pipe(
            Effect.andThen(Fiber.interrupt(opening)),
            Effect.andThen(Ref.set(stopped, true)),
            Effect.forkChild,
          )
          yield* Deferred.await(stoppingStarted)
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(stopped), false)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(stopping)
          const exit = yield* Fiber.await(opening)
          assert.strictEqual(exit._tag, 'Failure')
          if (exit._tag === 'Failure') assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
          assert.strictEqual(active, 0)
          yield* Scope.close(owner, Exit.void)
        }),
      ),
    )
    for (const reason of [
      new FileNotFound({ message: 'gone' }),
      new FilePermissionDenied({ message: 'denied' }),
    ]) {
      // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
      it.live(`${reason._tag} installation is skipped and retried without polling fallback`, () =>
        withEnv(
          Effect.gen(function* () {
            const { env, fs, path, native, owner } = yield* fixture
            let attempts = 0
            let active = 0
            const error = new FileError({ reason })
            const watcher = yield* Watch.make({
              fs: fs,
              path: path,
              native: {
                ...native,
                watchDirectory: (directory) =>
                  DirectoryFixture.notifications(() =>
                    directory === env.cwd
                      ? Effect.sync(() => {
                          attempts++
                        }).pipe(Effect.andThen(Effect.fail(error)))
                      : Effect.acquireRelease(
                          Effect.sync(() => {
                            active++
                          }),
                          () =>
                            Effect.sync(() => {
                              active--
                            }),
                        ),
                  ),
              },
              targets: [{ path: env.cwd }],
            }).pipe(Scope.provide(owner))
            assert.strictEqual(watcher.mode, 'native')
            assert.strictEqual(attempts, 2)
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(active, 0)
          }),
        ),
      )
    }
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'generic installation failure switches to polling and joins all earlier producers',
      () =>
        withEnv(
          Effect.gen(function* () {
            const { env, fs, path, native, owner } = yield* fixture
            let active = 0
            const watcher = yield* Watch.make({
              fs: fs,
              path: path,
              native: {
                ...native,
                watchDirectory: (directory) =>
                  DirectoryFixture.notifications(() =>
                    directory === env.cwd
                      ? Effect.fail(
                          new FileError({
                            reason: new FileNotSupported({ message: 'native unavailable' }),
                          }),
                        )
                      : Effect.acquireRelease(
                          Effect.sync(() => {
                            active++
                          }),
                          () =>
                            Effect.sync(() => {
                              active--
                            }),
                        ),
                  ),
              },
              targets: [{ path: env.cwd }],
            }).pipe(Scope.provide(owner))
            assert.strictEqual(watcher.mode, 'polling')
            assert.strictEqual(active, 0)
            assert.deepStrictEqual(yield* watcher.changes.pipe(Stream.take(1), Stream.runCollect), [
              { _tag: 'Overflow' },
            ])
            yield* Scope.close(owner, Exit.void)
          }),
        ),
    )
    for (const terminal of ['failure', 'end'] as const) {
      // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
      it.live(`runtime ${terminal} reaches its owner and reinstalls only that producer`, () =>
        withEnv(
          Effect.gen(function* () {
            const { env, fs, path, native, owner } = yield* fixture
            const reinstalled = yield* Deferred.make<void>()
            const queues = new Map<
              string,
              Queue.Queue<string | undefined, FileError | Cause.Done>
            >()
            const attempts = new Map<string, number>()
            const releases = new Map<string, number>()
            const watcher = yield* Watch.make({
              fs: fs,
              path: path,
              native: {
                ...native,
                watchDirectory: (directory) =>
                  DirectoryFixture.notifications((queue) =>
                    Effect.acquireRelease(
                      Effect.gen(function* () {
                        queues.set(directory, queue)
                        const count = (attempts.get(directory) ?? 0) + 1
                        attempts.set(directory, count)
                        if (directory === env.cwd && count === 2)
                          yield* Deferred.succeed(reinstalled, undefined)
                      }),
                      () =>
                        Effect.sync(() => {
                          if (queues.get(directory) === queue) queues.delete(directory)
                          releases.set(directory, (releases.get(directory) ?? 0) + 1)
                        }),
                    ),
                  ),
              },
              targets: [{ path: env.cwd }],
            }).pipe(Scope.provide(owner))
            const original = queues.get(env.cwd)
            assert.ok(original)
            if (terminal === 'failure')
              yield* Queue.failCause(
                original,
                Cause.fail(new FileError({ reason: new FileUnknown({ message: 'native fault' }) })),
              )
            else yield* Queue.end(original)
            yield* Deferred.await(reinstalled).pipe(Effect.timeout(2000))
            assert.strictEqual(watcher.mode, 'native')
            assert.notStrictEqual(queues.get(env.cwd), original)
            assert.strictEqual(releases.get(env.cwd), 1)
            assert.strictEqual(attempts.get(env.cwd), 2)
            for (const [directory, count] of attempts)
              if (directory !== env.cwd) assert.strictEqual(count, 1)
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(queues.size, 0)
          }),
        ),
      )
    }
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('undefined native filename remains an observable overflow notification', () =>
      withEnv(
        Effect.gen(function* () {
          const { env, fs, path, native, owner } = yield* fixture
          const queues = new Map<string, Queue.Queue<string | undefined, FileError | Cause.Done>>()
          const watcher = yield* Watch.make({
            fs: fs,
            path: path,
            native: {
              ...native,
              watchDirectory: (directory) =>
                DirectoryFixture.notifications((queue) =>
                  Effect.acquireRelease(
                    Effect.sync(() => {
                      queues.set(directory, queue)
                    }),
                    () =>
                      Effect.sync(() => {
                        queues.delete(directory)
                      }),
                  ),
                ),
            },
            targets: [{ path: env.cwd }],
          }).pipe(Scope.provide(owner))
          const queue = queues.get(env.cwd)
          assert.ok(queue)
          yield* Queue.offer(queue, undefined)
          assert.deepStrictEqual(
            yield* watcher.changes.pipe(Stream.take(1), Stream.runCollect, Effect.timeout(2000)),
            [{ _tag: 'Overflow' }],
          )
          yield* Scope.close(owner, Exit.void)
          assert.strictEqual(queues.size, 0)
        }),
      ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('startup defect cannot hang an adapter receipt that never settles', () =>
      withEnv(
        Effect.gen(function* () {
          const { env, fs, path, native, owner } = yield* fixture
          const defect = new Error('producer startup defect')
          const watcher = yield* Watch.make({
            fs: fs,
            path: path,
            native: {
              ...native,
              watchDirectory: () =>
                Effect.succeed({
                  changes: Stream.fromEffect(Effect.die(defect)),
                  started: Effect.never,
                }),
            },
            targets: [{ path: env.cwd }],
          }).pipe(Scope.provide(owner), Effect.timeout(2000))
          assert.strictEqual(watcher.mode, 'polling')
          assert.deepStrictEqual(yield* watcher.changes.pipe(Stream.take(1), Stream.runCollect), [
            { _tag: 'Overflow' },
          ])
          yield* Scope.close(owner, Exit.void)
        }),
      ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'startup interruption preserves cancellation even when its adapter receipt never settles',
      () =>
        withEnv(
          Effect.gen(function* () {
            const { env, fs, path, native, owner } = yield* fixture
            const exit = yield* Watch.make({
              fs: fs,
              path: path,
              native: {
                ...native,
                watchDirectory: () =>
                  Effect.succeed({
                    changes: Stream.fromEffect(Effect.interrupt),
                    started: Effect.never,
                  }),
              },
              targets: [{ path: env.cwd }],
            }).pipe(Scope.provide(owner), Effect.timeout(2000), Effect.exit)
            assert.strictEqual(exit._tag, 'Failure')
            if (exit._tag === 'Failure')
              assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
            yield* Scope.close(owner, Exit.void)
          }),
        ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'native cold stream captures actual acquisition failure and rejects repeat consumption',
      () =>
        withEnv(
          Effect.gen(function* () {
            const { env, native } = yield* fixture
            const directory = yield* native.watchDirectory(env.path.join(env.cwd, 'missing'))
            const failure = yield* Effect.flip(nativeResult(Stream.runDrain(directory.changes)))
            assert.strictEqual(failure.reason._tag, 'FileNotFound')
            assert.ok(failure.cause instanceof Error)
            assert.strictEqual(yield* Effect.flip(directory.started), failure)
            const repeated = yield* Effect.flip(nativeResult(Stream.runDrain(directory.changes)))
            assert.strictEqual(repeated.reason._tag, 'FileInvalid')
            assert.strictEqual(repeated.message, 'Directory notifications already consumed')
            assert.strictEqual(yield* Effect.flip(directory.started), failure)
          }),
        ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('native successful installation receipt stays bound to its sole consumer', () =>
      withEnv(
        Effect.gen(function* () {
          const { env, native, owner } = yield* fixture
          const directory = yield* native.watchDirectory(env.cwd)
          const consuming = yield* directory.changes.pipe(
            Stream.runDrain,
            Effect.forkScoped,
            Scope.provide(owner),
          )
          yield* directory.started
          const repeated = yield* Effect.flip(nativeResult(Stream.runDrain(directory.changes)))
          assert.strictEqual(repeated.reason._tag, 'FileInvalid')
          yield* directory.started
          yield* Scope.close(owner, Exit.void)
          const exit = yield* Fiber.await(consuming)
          assert.strictEqual(exit._tag, 'Failure')
          if (exit._tag === 'Failure') assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
        }),
      ),
    )
  })
})
