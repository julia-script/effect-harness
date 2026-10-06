import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Stream from 'effect/Stream'
import { Env, type Watcher, type WatchChange } from '../../src/Env.ts'
import { withEnv } from './Helpers.ts'
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
    it.live(
      `${mode}: missing ancestry, recursive creation, replacement/removal and symlink target changes`,
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const watcher = yield* env.watch([{ path: 'missing/tree', recursive: true }], {
              mode,
              pollIntervalMs: 20,
            })
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
            yield* Effect.sleep(600)
            const later = yield* until(watcher, target).pipe(Effect.forkChild)
            yield* env.writeFile('missing/tree/a', 'later')
            yield* Fiber.join(later)
            const removed = yield* until(watcher, target).pipe(Effect.forkChild)
            yield* env.remove('missing/tree/a')
            yield* Fiber.join(removed)
            yield* watcher.close
            yield* env.writeFile('real', 'before')
            yield* fs.symlink(env.path.join(env.cwd, 'real'), env.path.join(env.cwd, 'link'))
            const linked = yield* env.watch([{ path: 'link' }], { mode, pollIntervalMs: 20 })
            const event = yield* until(linked, env.path.join(env.cwd, 'link')).pipe(
              Effect.forkChild,
            )
            yield* env.writeFile('real', 'after')
            yield* Fiber.join(event)
            yield* linked.close
          }),
        ),
    )
  }
  it.live(
    'excluded entries do not report, overlapping recursion retains coverage, close starts no further delivery',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.createDir('tree', { recursive: true })
          const fs = yield* FileSystem.FileSystem
          yield* env.writeFile('external', 'before')
          yield* fs.symlink(env.path.join(env.cwd, 'external'), env.path.join(env.cwd, 'tree/link'))
          const watcher = yield* env.watch(
            [
              { path: 'tree', recursive: true, exclude: { hidden: true, names: ['skip'] } },
              { path: 'tree/visible' },
            ],
            { mode: 'native' },
          )
          const seen = yield* Ref.make<ReadonlyArray<WatchChange>>([])
          const listening = yield* watcher.changes.pipe(
            Stream.runForEach((change) => Ref.update(seen, (values) => [...values, change])),
            Effect.forkChild,
          )
          yield* env.renameFile('external', 'old-external')
          yield* env.writeFile('external', 'replacement')
          yield* env.writeFile('tree/.hidden/x', 'ignored')
          yield* env.writeFile('tree/skip/x', 'ignored')
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
          yield* Effect.sleep(150)
          assert.strictEqual(
            (yield* Ref.get(seen)).some((change) =>
              hasPath(change, env.path.join(env.cwd, 'tree/visible/sub/file')),
            ),
            true,
          )
          yield* watcher.close
          yield* watcher.close
          const count = (yield* Ref.get(seen)).length
          yield* env.writeFile('tree/visible/sub/file', 'late')
          yield* Effect.sleep(100)
          assert.strictEqual((yield* Ref.get(seen)).length, count)
          yield* Fiber.interrupt(listening)
        }),
      ),
  )
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
          (yield* Effect.flip(env.watch([{ path: '.', recursive: true }], { directoryBudget: 1 })))
            .code,
          'invalid',
        )
      }),
    ),
  )
})
