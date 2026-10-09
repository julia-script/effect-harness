import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { fileURLToPath } from 'node:url'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import * as Submission from 'effect-harness/Submission'

const worker = fileURLToPath(new URL('./fixtures/RestartWorker.mjs', import.meta.url))
const ResultSchema = Schema.fromJsonString(Submission.SettledSchema)

it.live('a fresh process resumes the same SQLite submission after a crash inside a safe tool', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const spawner = yield* ChildProcessSpawner
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-restart-' })
      const filename = path.join(directory, 'agent.sqlite')
      const first = yield* spawner.spawn(ChildProcess.make('node', [worker, filename, 'block']))
      const lines = yield* Stream.runCollect(
        first.stdout.pipe(
          Stream.decodeText,
          Stream.splitLines,
          Stream.takeUntil((line) => line === 'entered'),
        ),
      )
      const submitted = lines.find((line) => line.startsWith('submitted:'))
      assert.isDefined(submitted)
      yield* first.kill({ killSignal: 'SIGKILL' })
      const termination = yield* first.exitCode.pipe(Effect.flip)
      assert.strictEqual(termination.reason.method, 'exitCode')

      const second = yield* spawner.spawn(ChildProcess.make('node', [worker, filename, 'resume']))
      const [output, stderr, exit] = yield* Effect.all(
        [
          Stream.runCollect(second.stdout.pipe(Stream.decodeText, Stream.splitLines)),
          Stream.runCollect(second.stderr.pipe(Stream.decodeText)).pipe(
            Effect.map((chunks) => chunks.join('')),
          ),
          second.exitCode,
        ],
        { concurrency: 'unbounded' },
      )
      assert.strictEqual(exit, 0, stderr)
      assert.include(output, submitted)
      const receipt = output.find((line) => line.startsWith('settled:'))
      assert.isDefined(receipt)
      const settled = yield* Schema.decodeEffect(ResultSchema)(receipt.slice('settled:'.length))
      assert.strictEqual(settled.status, 'done')
      assert.strictEqual(`submitted:${settled.id}`, submitted)
      assert.strictEqual(output.filter((line) => line === 'entered').length, 1)
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
