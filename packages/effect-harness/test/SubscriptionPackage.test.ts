import { fileURLToPath } from 'node:url'
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'

const consumer = fileURLToPath(new URL('./fixtures/SubscriptionConsumer.mjs', import.meta.url))

it.live(
  'built subscription exports run in an independent Node consumer and bundle without Node host modules',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner
        const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
        const execute = (command: string, args: string[]) =>
          Effect.gen(function* () {
            const child = yield* spawner.spawn(ChildProcess.make(command, args))
            const [stdout, stderr, code] = yield* Effect.all(
              [
                child.stdout.pipe(
                  Stream.decodeText,
                  Stream.runCollect,
                  Effect.map((parts) => parts.join('')),
                ),
                child.stderr.pipe(
                  Stream.decodeText,
                  Stream.runCollect,
                  Effect.map((parts) => parts.join('')),
                ),
                child.exitCode,
              ],
              { concurrency: 'unbounded' },
            )
            assert.strictEqual(code, 0, stderr)
            return stdout
          })
        assert.include(yield* execute(process.execPath, [consumer]), 'packaged consumer works')
        yield* execute('bun', [
          'build',
          consumer,
          '--target',
          'browser',
          '--outfile',
          `${directory}/consumer.mjs`,
        ])
        assert.include(
          yield* execute(process.execPath, [`${directory}/consumer.mjs`]),
          'packaged consumer works',
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)
