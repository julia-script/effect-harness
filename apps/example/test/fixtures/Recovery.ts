/** Exercise the readable recovery example across real process boundaries. */
import { assert } from '@effect/vitest'
import { fileURLToPath } from 'node:url'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner, type ChildProcessHandle } from 'effect/process/ChildProcessSpawner'
import * as Conversation from 'effect-harness/Conversation'
import * as Identity from 'effect-harness/Identity'
import { Persistence } from 'effect-harness/Persistence'
import * as SqliteBun from 'effect-harness/storage/SqliteBun'
import { Result } from '../../dist/tour/Recovery.js'
import * as Runtime from '../../dist/tour/Runtime.js'

const example = fileURLToPath(new URL('../../dist/tour/Recovery.js', import.meta.url))
const ready = Effect.fn('recovery.ready')(function* (handle: ChildProcessHandle) {
  const line = yield* handle.stdout.pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.filter((value) => value.startsWith('Paused after committing tool progress.')),
    Stream.runHead,
    Effect.timeout('15 seconds'),
  )
  if (Option.isNone(line))
    return yield* new Runtime.ExampleError({ message: 'Example exited before committing progress' })
})
const complete = Effect.fn('recovery.complete')(function* (filename: string) {
  const spawner = yield* ChildProcessSpawner
  const child = yield* spawner.spawn(ChildProcess.make('bun', [example, filename, 'resume']))
  const [stdout, stderr, exitCode] = yield* Effect.all(
    [
      child.stdout.pipe(Stream.decodeText, Stream.runCollect),
      child.stderr.pipe(Stream.decodeText, Stream.runCollect),
      child.exitCode,
    ],
    { concurrency: 'unbounded' },
  ).pipe(Effect.timeout('15 seconds'))
  assert.strictEqual(exitCode, 0, stderr.join(''))
  const lines = stdout.join('').trim().split('\n')
  const settled = yield* Schema.decodeEffect(Schema.fromJsonString(Result))(lines.at(-1) ?? '')
  return {
    settled,
    invocations: lines.filter((line) => line === 'Searching the issue tracker...').length,
  }
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const spawner = yield* ChildProcessSpawner
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-recovery-test-' })
    const filename = path.join(directory, 'agent.sqlite')
    const start = yield* spawner.spawn(ChildProcess.make('bun', [example, filename, 'start']))
    yield* ready(start)
    yield* start.kill({ killSignal: 'SIGKILL' })
    yield* Effect.result(start.exitCode)
    const resumed = yield* complete(filename)
    assert.strictEqual(resumed.settled._tag, 'InputDone')
    assert.strictEqual(resumed.invocations, 1)
    const cached = yield* complete(filename)
    assert.deepEqual(cached.settled, resumed.settled)
    assert.strictEqual(cached.invocations, 0)

    const storage = yield* Layer.build(SqliteBun.layer({ filename }))
    const { harness } = yield* Runtime.open({ store: Context.get(storage, Persistence) })
    const root = yield* harness.root
    const answer = yield* Runtime.ask(root, 'Fix the flaky login test', {
      requestId: Identity.RequestId.make('job-42'),
    })
    assert.strictEqual(answer.text, 'Found issue #42.')
    assert.strictEqual(answer.submission.id, resumed.settled.id)
    const snapshot = yield* Conversation.snapshot(root)
    assert.strictEqual(snapshot.submissions.length, 1)
    assert.strictEqual(snapshot.submissions[0]?.status, 'done')
    assert.strictEqual(snapshot.entries.filter((entry) => entry.kind === 'harness.tool').length, 1)
  }),
)
