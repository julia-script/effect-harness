/** Kill a SQLite owner, resume in a fresh process, then reopen a completed submission. */
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner, type ChildProcessHandle } from 'effect/process/ChildProcessSpawner'
import * as Observation from 'effect-harness/Observation'
import * as Runtime from './Runtime.ts'

export const Result = Schema.Struct({
  killed: Schema.Literal('SIGKILL'),
  toolInvocations: Schema.Array(Schema.Literals(['start', 'resume'])),
  submissions: Schema.Int,
  toolEntries: Schema.Int,
  completedReplay: Schema.Literal('cached'),
})

const line = Effect.fn('tour.workerLine')(function* (handle: ChildProcessHandle, prefix: string) {
  const found = yield* handle.stdout.pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.filter((value) => value.startsWith(prefix)),
    Stream.runHead,
    Effect.timeout('15 seconds'),
  )
  if (Option.isNone(found))
    return yield* new Runtime.ExampleError({ message: `Worker exited before ${prefix}` })
  return found.value
})
export const run = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const spawner = yield* ChildProcessSpawner
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-tour-recovery-' })
    const filename = path.join(directory, 'agent.sqlite')
    const worker = path.join(import.meta.dirname, 'RecoveryWorker.js')
    const start = yield* spawner.spawn(ChildProcess.make('bun', [worker, filename, 'start']))
    yield* line(start, 'READY')
    // READY is printed only after the tool's partial output was committed.
    yield* start.kill({ killSignal: 'SIGKILL' })
    yield* Effect.result(start.exitCode)
    const resume = yield* spawner.spawn(ChildProcess.make('bun', [worker, filename, 'resume']))
    const output = yield* line(resume, 'DONE:')
    const snapshot = yield* Schema.decodeEffect(Schema.fromJsonString(Observation.Snapshot))(
      output.slice(5),
    )
    yield* Runtime.check((yield* resume.exitCode) === 0, 'The resumed process failed')
    yield* Runtime.check(
      snapshot.submissions.length === 1 && snapshot.submissions[0]?.status === 'done',
      'The retry did not reuse the saved submission',
    )
    yield* Runtime.check(
      snapshot.entries.filter((entry) => entry.kind === 'harness.tool').length === 1,
      'Recovery duplicated a committed tool result',
    )
    const audit = yield* fs.readFileString(filename + '.audit')
    yield* Runtime.check(audit === 'start\nresume\n', 'The safe tool was not replayed exactly once')
    const verify = yield* spawner.spawn(ChildProcess.make('bun', [worker, filename, 'verify']))
    const saved = yield* line(verify, 'DONE:')
    yield* Runtime.check(
      (yield* verify.exitCode) === 0 && saved === output,
      'Reopening changed the settled conversation',
    )
    yield* Runtime.check(
      (yield* fs.readFileString(filename + '.audit')) === audit,
      'Completed work ran again',
    )
    return yield* Schema.decodeUnknownEffect(Result)({
      killed: 'SIGKILL',
      toolInvocations: audit.trim().split('\n'),
      submissions: snapshot.submissions.length,
      toolEntries: 1,
      completedReplay: 'cached',
    })
  }),
)
