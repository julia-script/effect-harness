import { NodeServices } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { fileURLToPath } from 'node:url'
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'

const entrypoint = fileURLToPath(new URL('../dist/StructuredResult.js', import.meta.url))

it.live('the built public structured-result example reads committed channels', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner
      const child = yield* spawner.spawn(ChildProcess.make('node', [entrypoint]))
      const collect = (stream: typeof child.stdout) =>
        stream.pipe(
          Stream.decodeText,
          Stream.runCollect,
          Effect.map((chunks) => chunks.join('')),
        )
      const [stdout, stderr, exit] = yield* Effect.all(
        [collect(child.stdout), collect(child.stderr), child.exitCode],
        { concurrency: 'unbounded' },
      )
      assert.strictEqual(exit, 0, stderr || stdout)
      assert.include(stdout, 'HELLO')
      assert.include(stdout, 'length: 5')
      assert.include(stdout, 'Converted 5 characters.')
      assert.include(stdout, 'conversion')
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
