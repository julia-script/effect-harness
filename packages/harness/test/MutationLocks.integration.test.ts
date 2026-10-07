import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as PlatformError from 'effect/PlatformError'
import * as Option from 'effect/Option'
import * as RcMap from 'effect/RcMap'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import { Env } from '@effect-harness/harness/Env'
import { Invocation } from '@effect-harness/harness/Invocation'
import * as MutationLocks from '@effect-harness/harness/MutationLocks'
import * as NodeEnv from '@effect-harness/harness/NodeEnv'
import * as Write from '@effect-harness/harness/tools/Write'
import { withEnv } from './tools/Helpers.ts'

describe('MutationLocks', () => {
  describe('host mutation ownership', () => {
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'one manager spans independently built Env layers and separate runPromise boundaries until noncancelable write settlement',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const manager = yield* MutationLocks.MutationLocks
            const invocation = yield* Invocation
            yield* env.writeFile('file', 'original')
            yield* fs.symlink(env.path.join(env.cwd, 'file'), env.path.join(env.cwd, 'alias'))
            const admitted = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const secondCanonical = yield* Deferred.make<void>()
            const secondWrites = yield* Ref.make(0)
            const slow: FileSystem.FileSystem = {
              ...fs,
              open: (path, options) =>
                fs.open(path, options).pipe(
                  Effect.map((handle) => ({
                    ...handle,
                    stat: handle.stat,
                    sync: handle.sync,
                    writeAll: (bytes) =>
                      handle.writeAll(bytes.subarray(0, 2)).pipe(
                        Effect.andThen(Deferred.succeed(admitted, undefined)),
                        Effect.andThen(Deferred.await(release)),
                        Effect.andThen(
                          Effect.fail(
                            PlatformError.badArgument({
                              module: 'FileSystem',
                              method: 'writeAll',
                              description: 'admitted partial write failed',
                            }),
                          ),
                        ),
                      ),
                  })),
                ),
            }
            const observed: FileSystem.FileSystem = {
              ...fs,
              open: (path, options) =>
                fs.open(path, options).pipe(
                  Effect.map((handle) => ({
                    ...handle,
                    stat: handle.stat,
                    sync: handle.sync,
                    writeAll: (bytes) =>
                      Ref.update(secondWrites, (value) => value + 1).pipe(
                        Effect.andThen(handle.writeAll(bytes)),
                      ),
                  })),
                ),
            }
            const firstEnv = Context.get(
              yield* Layer.build(NodeEnv.layer({ id: env.id, cwd: env.cwd })).pipe(
                Effect.provideService(FileSystem.FileSystem, slow),
              ),
              Env,
            )
            const secondBuilt = Context.get(
              yield* Layer.build(NodeEnv.layer({ id: env.id, cwd: env.cwd })).pipe(
                Effect.provideService(FileSystem.FileSystem, observed),
              ),
              Env,
            )
            const secondEnv = Env.of({
              ...secondBuilt,
              canonicalPath: (path) =>
                secondBuilt
                  .canonicalPath(path)
                  .pipe(Effect.tap(() => Deferred.succeed(secondCanonical, undefined))),
            })
            assert.notStrictEqual(firstEnv, secondBuilt)
            const host = Context.make(MutationLocks.MutationLocks, manager).pipe(
              Context.add(Invocation, invocation),
            )
            const firstRuntimeScope = yield* Scope.make()
            const signal = yield* Effect.abortSignal.pipe(
              Effect.provideService(Scope.Scope, firstRuntimeScope),
            )
            const first = Effect.runPromiseExitWith(Context.add(host, Env, firstEnv))(
              Write.handler({ path: 'file', content: 'first' }),
              { signal },
            )
            let second: Promise<Exit.Exit<unknown, unknown>> | undefined
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(release, undefined).pipe(
                Effect.andThen(
                  Effect.promise(async () => {
                    await first
                    await second
                  }),
                ),
              ),
            )
            yield* Deferred.await(admitted)
            yield* Scope.close(firstRuntimeScope, Exit.void)
            second = Effect.runPromiseExitWith(Context.add(host, Env, secondEnv))(
              Write.handler({ path: 'alias', content: 'second' }),
            )
            yield* Deferred.await(secondCanonical)
            yield* Effect.yieldNow
            const canonicalKey = JSON.stringify([env.id, yield* env.canonicalPath('file')])
            assert.deepStrictEqual(Array.from(yield* RcMap.keys(manager)), [canonicalKey])
            // Probe the actual host permit while the aborted native write is admitted.
            // This observes admission directly rather than guessing when the second runtime was scheduled.
            const available = yield* RcMap.get(manager, canonicalKey).pipe(
              Effect.flatMap((lock) => lock.withPermitsIfAvailable(1)(Effect.void)),
              Effect.scoped,
            )
            assert.strictEqual(Option.isNone(available), true)
            assert.strictEqual(yield* Ref.get(secondWrites), 0)
            assert.strictEqual(yield* env.readTextFile('file'), 'original')
            yield* Deferred.succeed(release, undefined)
            const firstExit = yield* Effect.promise(() => first)
            const secondExit = yield* Effect.promise(() => second!)
            assert.strictEqual(Exit.isFailure(firstExit), true)
            if (Exit.isFailure(firstExit))
              assert.strictEqual(
                Cause.hasInterruptsOnly(firstExit.cause),
                true,
                Cause.pretty(firstExit.cause),
              )
            assert.strictEqual(Exit.isSuccess(secondExit), true)
            assert.strictEqual(yield* Ref.get(secondWrites), 1)
            assert.strictEqual(yield* env.readTextFile('file'), 'second')
            assert.deepStrictEqual((yield* fs.readDirectory(env.cwd)).sort(), ['alias', 'file'])
            assert.deepStrictEqual(Array.from(yield* RcMap.keys(manager)), [])
          }),
        ),
    )
    it.effect(
      'manager construction is lazy and separate hosts retain independent namespace resources',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const first = yield* MutationLocks.make
            const second = yield* MutationLocks.make
            assert.notStrictEqual(first, second)
            const firstA = yield* RcMap.get(first, JSON.stringify(['a', '/file']))
            const firstB = yield* RcMap.get(first, JSON.stringify(['b', '/file']))
            assert.notStrictEqual(firstA, firstB)
            assert.strictEqual(yield* RcMap.get(first, JSON.stringify(['a', '/file'])), firstA)
          }),
        ),
    )
  })
})
