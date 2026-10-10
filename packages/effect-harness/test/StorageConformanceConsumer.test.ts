import { fileURLToPath } from 'node:url'
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'

const consumer = fileURLToPath(
  new URL('./fixtures/StorageConformanceConsumer.mjs', import.meta.url),
)

it.live('runs memory, SQLite and JSONL cases from the built public Testing subpath', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner
      const child = yield* spawner.spawn(ChildProcess.make(process.execPath, [consumer]))
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
      assert.include(stdout, 'public storage consumer: 29 cases passed; temporary stores removed')
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
