import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import type { ChildProcessHandle } from 'effect/process/ChildProcessSpawner'
import * as ToolRegistration from '../../src/ToolRegistration.ts'
import * as Observation from '../../src/Observation.ts'
import * as State from '../../src/internal/ConversationState.ts'
import * as Transcript from '../../src/Transcript.ts'

const marker = Effect.fn('restart.marker')(function* (handle: ChildProcessHandle, prefix: string) {
  const line = yield* handle.stdout.pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.filter((line) => line.startsWith(prefix)),
    Stream.runHead,
    Effect.timeout('15 seconds'),
  )
  if (Option.isNone(line)) return yield* Effect.die(`Worker exited before ${prefix}`)
  return line.value
})

describe('embedded abrupt process recovery', () => {
  for (const replay of ['safe', 'unsafe', 'model'] as const) {
    it.live(
      `SIGKILL during ${replay === 'model' ? 'model inference' : `${replay} tool execution`} resumes one saved submission in a new process`,
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const spawner = yield* ChildProcessSpawner
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'effect-harness-restart-' })
          const filename = path.join(directory, 'harness.sqlite')
          const worker = path.resolve('packages/effect-harness/test/embedded/RestartWorker.ts')
          const start = yield* spawner.spawn(
            ChildProcess.make('bun', [worker, filename, 'start', replay]),
          )
          yield* marker(start, 'READY')
          yield* start.kill({ killSignal: 'SIGKILL' })
          yield* Effect.result(start.exitCode)
          assert.strictEqual(yield* start.isRunning, false)
          const resumed = yield* spawner.spawn(
            ChildProcess.make('bun', [worker, filename, 'resume', replay]),
          )
          const output = yield* marker(resumed, 'DONE:')
          const snapshot = yield* Schema.decodeEffect(Schema.fromJsonString(Observation.Snapshot))(
            output.slice('DONE:'.length),
          )
          assert.strictEqual(yield* resumed.exitCode, 0)
          assert.strictEqual(snapshot.submissions.length, 1)
          assert.strictEqual(snapshot.submissions[0]?.status, 'done')
          assert.isTrue(snapshot.tasks.every((task) => task.state.status === 'terminal'))
          const tools = snapshot.entries.filter((entry) => entry.kind === 'harness.tool')
          if (replay === 'model') {
            assert.strictEqual(tools.length, 0)
            assert.isTrue(snapshot.tasks.every((task) => !task.abortRequested))
            const entries = yield* Effect.forEach(snapshot.entries, State.projectEntry)
            const aborted = entries.filter((entry) => entry.status === 'aborted')
            assert.strictEqual(aborted.length, 1)
            const partial = aborted[0]?.messages?.[0]
            assert.strictEqual(partial?.role, 'assistant')
            if (partial?.role === 'assistant')
              assert.isTrue(
                partial.content.some(
                  (part) => part.type === 'text' && part.text === 'committed partial model output',
                ),
              )
            const visible = Transcript.derive(entries)
            assert.isFalse(
              visible.messages.some(
                (message) =>
                  message.role === 'assistant' &&
                  message.content.some(
                    (part) =>
                      part.type === 'text' && part.text === 'committed partial model output',
                  ),
              ),
            )
            assert.isFalse(
              snapshot.documents.some((document) => document.record.kind === 'harness.progress'),
            )
          } else {
            assert.strictEqual(tools.length, 1)
            const execution = yield* Schema.decodeUnknownEffect(
              Schema.toCodecJson(ToolRegistration.Execution),
            )(tools[0]?.data)
            const encoded = yield* Schema.encodeEffect(
              Schema.fromJsonString(Schema.toCodecJson(ToolRegistration.Execution)),
            )(execution)
            assert.include(encoded, replay === 'safe' ? 'completed' : 'interrupted')
            if (replay === 'unsafe') assert.include(encoded, 'committed partial output')
          }
          const audit = yield* fs.readFileString(filename + '.audit')
          assert.deepEqual(
            audit.trim().split('\n'),
            replay === 'unsafe' ? ['start'] : ['start', 'resume'],
          )
          const verify = yield* spawner.spawn(
            ChildProcess.make('bun', [worker, filename, 'verify', replay]),
          )
          const verified = yield* marker(verify, 'DONE:')
          const cached = yield* Schema.decodeEffect(Schema.fromJsonString(Observation.Snapshot))(
            verified.slice('DONE:'.length),
          )
          assert.strictEqual(yield* verify.exitCode, 0)
          assert.deepEqual(cached, snapshot)
          assert.strictEqual(yield* fs.readFileString(filename + '.audit'), audit)
        }).pipe(Effect.provide(NodeServices.layer)),
    )
  }
})
