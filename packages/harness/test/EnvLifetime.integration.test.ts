import * as NodeNativeFiles from '@effect-harness/harness/NodeNativeFiles'
import { assert, describe, it } from '@effect/vitest'
import { vi } from 'vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Queue from 'effect/Queue'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import { Env, NativeFiles } from '@effect-harness/harness/Env'
// effect-review-allow P8-tests-import-public-specifiers: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
import * as Exec from '../src/env/internal/exec.ts'
// effect-review-allow P8-tests-import-public-specifiers: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
import * as Watch from '../src/env/internal/watch.ts'
import { withEnv } from './tools/Helpers.ts'
import * as DirectoryFixture from './tools/DirectoryFixture.ts'

const native = vi.hoisted(() => ({
  path: '',
  closeError: undefined as unknown,
  closes: 0,
  beforeRead: undefined as (() => Promise<void>) | undefined,
  beforeClose: undefined as (() => Promise<void>) | undefined,
}))
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>()
  return {
    ...actual,
    open: async (...args: Parameters<typeof actual.open>) => {
      const file = await actual.open(...args)
      if (args[0] === native.path) {
        const close = file.close.bind(file)
        const read = file.read.bind(file)
        Object.defineProperty(file, 'close', {
          value: async () => {
            native.closes++
            await native.beforeClose?.()
            await close()
            if (native.closeError !== undefined) throw native.closeError
          },
        })
        Object.defineProperty(file, 'read', {
          value: async (...args: ReadonlyArray<unknown>) => {
            await native.beforeRead?.()
            return Reflect.apply(read, file, args)
          },
        })
      }
      return file
    },
    opendir: async (...args: Parameters<typeof actual.opendir>) => {
      const directory = await actual.opendir(...args)
      if (args[0] === native.path) {
        const close = directory.close.bind(directory)
        Object.defineProperty(directory, 'close', {
          value: async () => {
            native.closes++
            await close()
            if (native.closeError !== undefined) throw native.closeError
          },
        })
      }
      return directory
    },
  }
})

describe('EnvLifetime', () => {
  const reset = (path: string, closeError?: unknown) =>
    Effect.sync(() => {
      native.path = path
      native.closeError = closeError
      native.closes = 0
      native.beforeRead = undefined
      native.beforeClose = undefined
    })

  describe('ScopedLifetime', () => {
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'reader scope closes once after admitted native reads and invalidates later access',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            yield* env.writeFile('file', 'contents')
            yield* reset(env.path.join(env.cwd, 'file'))
            const owner = yield* Scope.fork(yield* Scope.Scope)
            const reader = yield* env.openBinaryReader('file').pipe(Scope.provide(owner))
            assert.strictEqual('close' in reader, false)
            const admitted = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, undefined).pipe(Effect.asVoid),
            )
            const services = yield* Effect.context<never>()
            yield* Effect.sync(() => {
              native.beforeRead = async () => {
                Effect.runSyncWith(services)(Deferred.succeed(admitted, undefined))
                await Effect.runPromiseWith(services)(Deferred.await(release))
              }
            })
            const reading = yield* reader.read(0, 1).pipe(Effect.forkChild)
            yield* Deferred.await(admitted)
            const closingEntered = yield* Deferred.make<void>()
            const closing = yield* Deferred.succeed(closingEntered, undefined).pipe(
              Effect.andThen(Scope.close(owner, Exit.void)),
              Effect.forkChild,
            )
            yield* Deferred.await(closingEntered)
            yield* Effect.yieldNow
            assert.strictEqual(native.closes, 0)
            yield* Deferred.succeed(release, undefined)
            assert.deepStrictEqual(Array.from(yield* Fiber.join(reading)), [99])
            yield* Fiber.join(closing)
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(native.closes, 1)
            assert.strictEqual((yield* Effect.flip(reader.info)).reason._tag, 'FileInvalid')
            assert.strictEqual((yield* Effect.flip(reader.read(0, 1))).reason._tag, 'FileInvalid')
          }),
        ),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live('interruption of unsupported-file acquisition joins the admitted private close', () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.createDir('unsupported')
          yield* reset(env.path.join(env.cwd, 'unsupported'))
          const admitted = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
          const services = yield* Effect.context<never>()
          yield* Effect.sync(() => {
            native.beforeClose = async () => {
              Effect.runSyncWith(services)(Deferred.succeed(admitted, undefined))
              await Effect.runPromiseWith(services)(Deferred.await(release))
            }
          })
          const opening = yield* env.openBinaryReader('unsupported').pipe(Effect.forkChild)
          yield* Deferred.await(admitted)
          const requested = yield* Deferred.make<void>()
          const stopped = yield* Ref.make(false)
          const stopping = yield* Deferred.succeed(requested, undefined).pipe(
            Effect.andThen(Fiber.interrupt(opening)),
            Effect.andThen(Ref.set(stopped, true)),
            Effect.forkChild,
          )
          yield* Deferred.await(requested)
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(stopped), false)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(stopping)
          const exit = yield* Fiber.await(opening)
          assert.strictEqual(exit._tag, 'Failure')
          if (exit._tag === 'Failure') assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
          assert.strictEqual(native.closes, 1)
        }),
      ),
    )
    for (const kind of ['file', 'directory'] as const) {
      // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
      it.live(`${kind} unexpected close rejection remains the exact native defect`, () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            if (kind === 'file') yield* env.writeFile('resource', 'value')
            else yield* env.createDir('resource')
            const error = Object.assign(new Error('Directory handle was closed'), { code: 'EIO' })
            yield* reset(env.path.join(env.cwd, 'resource'), error)
            const owner = yield* Scope.fork(yield* Scope.Scope)
            if (kind === 'file') yield* env.openBinaryReader('resource').pipe(Scope.provide(owner))
            else yield* env.openDirReader('resource').pipe(Scope.provide(owner))
            const exit = yield* Scope.close(owner, Exit.void).pipe(Effect.exit)
            assert.strictEqual(exit._tag, 'Failure')
            if (exit._tag === 'Failure') {
              assert.strictEqual(Cause.hasFails(exit.cause), false)
              assert.strictEqual(Cause.hasDies(exit.cause), true)
              assert.strictEqual(Cause.squash(exit.cause), error)
            }
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(native.closes, 1)
          }),
        ),
      )
    }
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live('directory close accepts exactly ERR_DIR_CLOSED irrespective of its message', () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.createDir('directory')
          yield* reset(
            env.path.join(env.cwd, 'directory'),
            Object.assign(new Error('arbitrary native wording'), { code: 'ERR_DIR_CLOSED' }),
          )
          const owner = yield* Scope.fork(yield* Scope.Scope)
          const reader = yield* env.openDirReader('directory').pipe(Scope.provide(owner))
          assert.strictEqual('close' in reader, false)
          yield* Scope.close(owner, Exit.void)
          yield* Scope.close(owner, Exit.void)
          assert.strictEqual(native.closes, 1)
          assert.strictEqual((yield* Effect.flip(reader.next(1))).reason._tag, 'FileInvalid')
        }),
      ),
    )
    // Native child-process spawn, pipe delivery and kill/join callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'owner release drains protected native spawn registration and rejects new commands',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const path = yield* Path.Path
            const spawner = yield* ChildProcessSpawner
            const admitted = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const handles: Array<import('effect/process/ChildProcessSpawner').ChildProcessHandle> =
              []
            const controlled: ChildProcessSpawner['Service'] = {
              ...spawner,
              spawn: (command) =>
                Effect.gen(function* () {
                  const handle = yield* spawner.spawn(command)
                  handles.push(handle)
                  yield* Deferred.succeed(admitted, undefined)
                  yield* Deferred.await(release)
                  return handle
                }),
            }
            const owner = yield* Scope.fork(yield* Scope.Scope)
            const executor = yield* Exec.make({
              fs: fs,
              path: path,
              spawner: controlled,
              defaults: {
                id: 'admission',
                cwd: env.cwd,
                shell: '/bin/sh',
              },
            }).pipe(Scope.provide(owner))
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, undefined).pipe(Effect.asVoid),
            )
            assert.strictEqual('cleanup' in executor, false)
            const running = yield* executor.exec('printf admitted; sleep 10').pipe(Effect.forkChild)
            yield* Deferred.await(admitted)
            const closingStarted = yield* Deferred.make<void>()
            yield* Scope.addFinalizer(
              owner,
              Deferred.succeed(closingStarted, undefined).pipe(Effect.asVoid),
            )
            const settled = yield* Ref.make(false)
            const closing = yield* Scope.close(owner, Exit.void).pipe(
              Effect.andThen(Ref.set(settled, true)),
              Effect.forkChild,
            )
            yield* Deferred.await(closingStarted)
            yield* Effect.yieldNow
            assert.strictEqual(yield* Ref.get(settled), false)
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(closing)
            const handle = handles[0]
            assert.ok(handle)
            assert.strictEqual(yield* handle.isRunning, false)
            yield* Fiber.await(running).pipe(Effect.timeout(2000))
            const rejected = yield* Effect.flip(executor.exec('printf escaped'))
            assert.strictEqual(rejected.reason._tag, 'ExecutionUnknown')
            assert.strictEqual(rejected.message, 'Execution owner is closed')
            assert.strictEqual(rejected.cause, undefined)
            assert.strictEqual(handles.length, 1)
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual((yield* env.exec('exit 0')).exitCode, 0)
          }),
        ),
    )
    // Native child-process spawn, pipe delivery and kill/join callbacks advance on the host loop; TestClock cannot drive them.
    it.live('owner close joins a blocked native kill and preserves the caught signal failure', () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const spawner = yield* ChildProcessSpawner
          const owner = yield* Scope.fork(yield* Scope.Scope)
          const killing = yield* Deferred.make<void>()
          const releaseKill = yield* Deferred.make<void>()
          const started = yield* Deferred.make<void>()
          const nativeFailure = yield* Ref.make<PlatformError.PlatformError | undefined>(undefined)
          const handles: Array<import('effect/process/ChildProcessSpawner').ChildProcessHandle> = []
          const controlled: ChildProcessSpawner['Service'] = {
            ...spawner,
            spawn: (command) =>
              spawner.spawn(command).pipe(
                Effect.map((handle) => {
                  handles.push(handle)
                  return {
                    ...handle,
                    exitCode: handle.exitCode.pipe(
                      Effect.tapError((error) => Ref.set(nativeFailure, error)),
                    ),
                    kill: (options) =>
                      Deferred.succeed(killing, undefined).pipe(
                        Effect.andThen(Deferred.await(releaseKill)),
                        Effect.andThen(handle.kill(options)),
                      ),
                  }
                }),
              ),
          }
          const executor = yield* Exec.make({
            fs: fs,
            path: path,
            spawner: controlled,
            defaults: {
              id: 'kill-join',
              cwd: env.cwd,
              shell: '/bin/sh',
            },
          }).pipe(Scope.provide(owner))
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(releaseKill, undefined).pipe(Effect.asVoid),
          )
          const running = yield* executor
            .exec('printf started; sleep 10', {
              onOutput: () => Deferred.succeed(started, undefined).pipe(Effect.asVoid),
            })
            .pipe(Effect.forkChild)
          yield* Deferred.await(started)
          const closeSettled = yield* Ref.make(false)
          const closing = yield* Scope.close(owner, Exit.void).pipe(
            Effect.andThen(Ref.set(closeSettled, true)),
            Effect.forkChild,
          )
          yield* Deferred.await(killing)
          assert.strictEqual(yield* Ref.get(closeSettled), false)
          const handle = handles[0]
          assert.ok(handle)
          assert.strictEqual(yield* handle.isRunning, true)
          yield* Deferred.succeed(releaseKill, undefined)
          yield* Fiber.join(closing)
          assert.strictEqual(yield* handle.isRunning, false)
          const failure = yield* Effect.flip(Fiber.join(running))
          assert.strictEqual(failure.reason._tag, 'ExecutionUnknown')
          const captured = yield* Ref.get(nativeFailure)
          assert.ok(captured)
          assert.strictEqual(failure.message, captured.message)
          assert.strictEqual(failure.cause, captured)
          assert.ok(captured.reason.cause instanceof Error)
          if (captured.reason.cause instanceof Error)
            assert.match(captured.reason.cause.message, /SIGTERM/)
        }),
      ),
    )
    // Native child-process spawn, pipe delivery and kill/join callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'successful native exit retains its admitted consumer until the caller execution settles',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const path = yield* Path.Path
            const spawner = yield* ChildProcessSpawner
            const owner = yield* Scope.fork(yield* Scope.Scope)
            const handles: Array<import('effect/process/ChildProcessSpawner').ChildProcessHandle> =
              []
            const controlled: ChildProcessSpawner['Service'] = {
              ...spawner,
              spawn: (command) =>
                spawner.spawn(command).pipe(
                  Effect.tap((handle) =>
                    Effect.sync(() => {
                      handles.push(handle)
                    }),
                  ),
                ),
            }
            const executor = yield* Exec.make({
              fs: fs,
              path: path,
              spawner: controlled,
              defaults: {
                id: 'consumer',
                cwd: env.cwd,
                shell: '/bin/sh',
              },
            }).pipe(Scope.provide(owner))
            const consuming = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, undefined).pipe(Effect.asVoid),
            )
            const settled = yield* Ref.make(false)
            const delivered = yield* Ref.make('')
            const interrupted = yield* Ref.make(false)
            const running = yield* executor
              .exec('printf admitted', {
                onOutput: (text) =>
                  Deferred.succeed(consuming, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.andThen(Ref.update(delivered, (value) => value + text)),
                    Effect.onInterrupt(() => Ref.set(interrupted, true)),
                  ),
              })
              .pipe(
                Effect.onExit(() => Ref.set(settled, true)),
                Effect.forkChild,
              )
            yield* Deferred.await(consuming)
            const handle = handles[0]
            assert.ok(handle)
            assert.strictEqual(Number(yield* handle.exitCode), 0)
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(yield* handle.isRunning, false)
            assert.strictEqual(yield* Ref.get(settled), false)
            assert.strictEqual(yield* Ref.get(interrupted), false)
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual((yield* Fiber.join(running)).exitCode, 0)
            assert.strictEqual(yield* Ref.get(delivered), 'admitted')
            assert.strictEqual(yield* Ref.get(interrupted), false)
          }),
        ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'watcher scope ends its stream and unregisters every native producer before returning',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const path = yield* Path.Path
            const files = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layerNative))
            const callbacks = new Map<string, (path: string | undefined) => void>()
            const owner = yield* Scope.fork(yield* Scope.Scope)
            const watcher = yield* Watch.make({
              fs: fs,
              path: path,
              native: {
                ...files,
                watchDirectory: (directory) =>
                  DirectoryFixture.notifications((queue) =>
                    Effect.acquireRelease(
                      Effect.sync(() => {
                        callbacks.set(directory, (changed) => {
                          Queue.offerUnsafe(queue, changed)
                        })
                      }),
                      () =>
                        Effect.sync(() => {
                          callbacks.delete(directory)
                        }),
                    ).pipe(Effect.asVoid),
                  ),
              },
              targets: [{ path: env.path.join(env.cwd, 'target') }],
            }).pipe(Scope.provide(owner))
            assert.strictEqual('close' in watcher, false)
            const listener = yield* watcher.changes.pipe(Stream.runDrain, Effect.forkChild)
            assert.strictEqual(callbacks.size > 0, true)
            const callback = callbacks.values().next().value
            assert.ok(callback)
            yield* Scope.close(owner, Exit.void)
            assert.strictEqual(callbacks.size, 0)
            yield* Effect.sync(() => callback(path.join(env.cwd, 'target')))
            const exit = yield* Fiber.await(listener)
            assert.strictEqual(exit._tag, 'Failure')
            // Queue.shutdown interrupts takers; require only interruption, with no worker defect/failure.
            if (exit._tag === 'Failure')
              assert.strictEqual(Cause.hasInterruptsOnly(exit.cause), true)
            yield* Scope.close(owner, Exit.void)
          }),
        ),
    )
  })
})
