import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'
import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Queue from 'effect/Queue'
import * as Path from 'effect/Path'
// effect-review-allow P8-tests-import-public-specifiers: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
import * as Watch from '../../../src/env/internal/watch.ts'
import * as Stream from 'effect/Stream'
import { Env, NativeFiles, type Watcher, type WatchChange } from 'effect-harness/Env'
import { withEnv } from '../tools/Helpers.ts'
import * as DirectoryFixture from '../tools/DirectoryFixture.ts'
describe('Watch', () => {
  const hasPath = (value: WatchChange, path: string): boolean =>
    'paths' in value &&
    value.paths.some((changed) => changed === path || path.startsWith(changed + '/'))
  const until = (watcher: Watcher, path: string) =>
    watcher.changes.pipe(
      Stream.filter((change) => hasPath(change, path)),
      Stream.take(1),
      Stream.runDrain,
      Effect.timeout(3000),
    )
  describe('watch supervision coverage and scoped close', () => {
    for (const mode of ['native', 'polling'] as const) {
      // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
      it.live(
        `${mode}: missing ancestry, recursive creation, replacement/removal and symlink target changes`,
        () =>
          withEnv(
            Effect.gen(function* () {
              const env = yield* Env
              const fs = yield* FileSystem.FileSystem
              const watcherScope = yield* Scope.fork(yield* Scope.Scope)
              const watcher = yield* env
                .watch([{ path: 'missing/tree', recursive: true }], {
                  mode,
                  pollIntervalMs: 20,
                })
                .pipe(Scope.provide(watcherScope))
              const target = env.path.join(env.cwd, 'missing/tree/a')
              const created = yield* until(watcher, target).pipe(Effect.forkChild)
              yield* env.writeFile('missing/tree/a', 'one')
              yield* Fiber.join(created)
              const changed = yield* until(watcher, target).pipe(Effect.forkChild)
              yield* env.writeFile('missing/tree/a', 'two')
              yield* Fiber.join(changed)
              const replaced = yield* until(watcher, target).pipe(Effect.forkChild)
              yield* env.renameFile('missing', 'old')
              yield* env.writeFile('missing/tree/a', 'new')
              yield* Fiber.join(replaced)
              const later = yield* until(watcher, target).pipe(Effect.forkChild)
              yield* env.writeFile('missing/tree/a', 'later')
              yield* Fiber.join(later)
              const removed = yield* until(watcher, target).pipe(Effect.forkChild)
              yield* env.remove('missing/tree/a')
              yield* Fiber.join(removed)
              yield* Scope.close(watcherScope, Exit.void)
              yield* env.writeFile('real', 'before')
              yield* fs.symlink(env.path.join(env.cwd, 'real'), env.path.join(env.cwd, 'link'))
              const linkedScope = yield* Scope.fork(yield* Scope.Scope)
              const linked = yield* env
                .watch([{ path: 'link' }], { mode, pollIntervalMs: 20 })
                .pipe(Scope.provide(linkedScope))
              const event = yield* until(linked, env.path.join(env.cwd, 'link')).pipe(
                Effect.forkChild,
              )
              yield* env.writeFile('real', 'after')
              yield* Fiber.join(event)
              yield* Scope.close(linkedScope, Exit.void)
            }),
          ),
      )
    }
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'excluded entries do not report, overlapping recursion retains coverage, close starts no further delivery',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            yield* env.createDir('tree', { recursive: true })
            const fs = yield* FileSystem.FileSystem
            yield* env.writeFile('external', 'before')
            yield* fs.symlink(
              env.path.join(env.cwd, 'external'),
              env.path.join(env.cwd, 'tree/link'),
            )
            const watcherScope = yield* Scope.fork(yield* Scope.Scope)
            const watcher = yield* env
              .watch(
                [
                  { path: 'tree', recursive: true, exclude: { hidden: true, names: ['skip'] } },
                  { path: 'tree/visible' },
                ],
                { mode: 'native' },
              )
              .pipe(Scope.provide(watcherScope))
            const seen = yield* Ref.make<ReadonlyArray<WatchChange>>([])
            const tracked = yield* Deferred.make<void>()
            const visible = env.path.join(env.cwd, 'tree/visible/sub/file')
            const listening = yield* watcher.changes.pipe(
              Stream.runForEach((change) =>
                Ref.update(seen, (values) => [...values, change]).pipe(
                  Effect.andThen(
                    hasPath(change, visible) ? Deferred.succeed(tracked, undefined) : Effect.void,
                  ),
                ),
              ),
              Effect.forkChild,
            )
            yield* env.renameFile('external', 'old-external')
            yield* env.writeFile('external', 'replacement')
            yield* env.writeFile('tree/.hidden/x', 'ignored')
            yield* env.writeFile('tree/skip/x', 'ignored')
            // Exclusion is an absence assertion about live native OS events. This
            // bounded observation window is not a watcher-installation barrier.
            yield* Effect.sleep(150)
            assert.strictEqual(
              (yield* Ref.get(seen)).some(
                (change) =>
                  'paths' in change &&
                  change.paths.some(
                    (path) =>
                      path.includes('.hidden') ||
                      path.includes('/skip') ||
                      path.endsWith('/tree/link'),
                  ),
              ),
              false,
            )
            yield* env.writeFile('tree/visible/sub/file', 'tracked')
            yield* Deferred.await(tracked).pipe(Effect.timeout(3000))
            assert.strictEqual(
              (yield* Ref.get(seen)).some((change) =>
                hasPath(change, env.path.join(env.cwd, 'tree/visible/sub/file')),
              ),
              true,
            )
            yield* Scope.close(watcherScope, Exit.void)
            yield* Scope.close(watcherScope, Exit.void)
            const stopped = yield* Fiber.await(listening).pipe(Effect.timeout(3000))
            assert.isTrue(Exit.isFailure(stopped))
            if (Exit.isFailure(stopped)) assert.isTrue(Cause.hasInterruptsOnly(stopped.cause))
            const count = (yield* Ref.get(seen)).length
            yield* env.writeFile('tree/visible/sub/file', 'late')
            assert.strictEqual((yield* Ref.get(seen)).length, count)
            yield* Fiber.interrupt(listening)
          }),
        ),
    )
    // Real filesystem acquisition and release callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'native hints for unchanged nested symlinks require snapshot changes; explicit targets remain covered',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const path = yield* Path.Path
            const native = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layerNative))
            yield* env.writeFile('external', 'before')
            yield* env.writeFile('tree/barrier', 'before')
            const nested = path.join(env.cwd, 'tree/link')
            const explicit = path.join(env.cwd, 'root-link')
            const barrier = path.join(env.cwd, 'tree/barrier')
            yield* fs.symlink(path.join(env.cwd, 'external'), nested)
            yield* fs.symlink(path.join(env.cwd, 'external'), explicit)
            const callbacks = new Map<string, (changed: string | undefined) => void>()
            const watcherScope = yield* Scope.fork(yield* Scope.Scope)
            const watcher = yield* Watch.make({
              fs: fs,
              path: path,
              native: {
                ...native,
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
              targets: [{ path: path.join(env.cwd, 'tree'), recursive: true }, { path: explicit }],
            }).pipe(Scope.provide(watcherScope))
            const observed = yield* Queue.unbounded<WatchChange>()
            const listening = yield* watcher.changes.pipe(
              Stream.runForEach((change) => Queue.offer(observed, change)),
              Effect.forkChild,
            )
            const emitTree = callbacks.get(path.dirname(nested))
            const emitRoot = callbacks.get(env.cwd)
            assert.ok(emitTree)
            assert.ok(emitRoot)
            yield* Effect.sync(() => {
              emitTree(nested)
              emitRoot(explicit)
            })
            const seen: Array<WatchChange> = []
            while (!seen.some((change) => hasPath(change, explicit)))
              seen.push(yield* Queue.take(observed).pipe(Effect.timeout(3000)))
            yield* env.writeFile('tree/barrier', 'settled')
            yield* Effect.sync(() => emitTree(barrier))
            while (!seen.some((change) => hasPath(change, barrier)))
              seen.push(yield* Queue.take(observed).pipe(Effect.timeout(3000)))
            assert.strictEqual(
              seen.some((change) => hasPath(change, nested)),
              false,
            )
            assert.strictEqual(
              seen.some((change) => hasPath(change, explicit)),
              true,
            )
            yield* fs.remove(nested)
            yield* fs.symlink(path.join(env.cwd, 'different-target'), nested)
            yield* Effect.sync(() => emitTree(nested))
            let replacement = yield* Queue.take(observed).pipe(Effect.timeout(3000))
            while (!hasPath(replacement, nested))
              replacement = yield* Queue.take(observed).pipe(Effect.timeout(3000))
            yield* Scope.close(watcherScope, Exit.void)
            yield* Fiber.interrupt(listening)
          }),
        ),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live('invalid options and initial directory budget reject without a live watcher', () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          assert.strictEqual(
            (yield* Effect.flip(env.watch([{ path: '.' }], { pollIntervalMs: 0 }))).code,
            'invalid',
          )
          yield* env.createDir('child')
          assert.strictEqual(
            (yield* Effect.flip(
              env.watch([{ path: '.', recursive: true }], { directoryBudget: 1 }),
            )).code,
            'invalid',
          )
        }),
      ),
    )
  })
})
