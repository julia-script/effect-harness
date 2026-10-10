import { strict as assert } from 'node:assert'
import * as BunChildProcessSpawner from '@effect/platform-bun/BunChildProcessSpawner'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Identity from 'effect-harness/Identity'
import * as Record from 'effect-harness/Record'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolSpill from 'effect-harness/ToolSpill'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ToolkitProcess from 'effect-harness/ToolkitProcess'
import * as Bash from 'effect-harness/coding/Bash'
import * as ShellProcess from 'effect-harness/coding/ShellProcess'

const files = Layer.merge(BunFileSystem.layer, BunPath.layer)
const native = BunChildProcessSpawner.layer.pipe(Layer.provideMerge(files))
const binding = (root: string) => {
  const bound = Layer.merge(ToolkitFileSystem.layer({ root }), ToolkitProcess.layer({ root })).pipe(
    Layer.provide(native),
  )
  return Layer.merge(
    ShellProcess.layerNative,
    ToolSpill.layer({ provider: root, directory: `${root}/spills`, lifetime: 'persistent' }),
  ).pipe(Layer.provideMerge(bound))
}
BunRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
      const chunks: string[] = []
      const details: Schema.Json[] = []
      let ready = yield* Deferred.make<void>()
      const service = ToolExecution.of({
        conversationId: yield* Schema.decodeEffect(Identity.ConversationId)(1),
        taskId: yield* Schema.decodeEffect(Record.TaskId)(1),
        callId: 'bun-shell',
        snapshot: () => Effect.succeedNone,
        watch: () => Stream.empty,
        commit: () => Effect.die('Unexpected commit'),
        output: (s) =>
          Effect.sync(() => {
            chunks.push(s)
          }).pipe(Effect.andThen(Deferred.succeed(ready, undefined)), Effect.asVoid),
        details: (s) =>
          Effect.sync(() => {
            details.push(s)
          }),
        diagnostic: () => Effect.void,
      })
      yield* Effect.gen(function* () {
        const boundFs = yield* FileSystem.FileSystem
        const provider = yield* ToolSpill.ToolSpill
        const hostile = '$(touch SHOULD_NOT_EXIST); "quote" & * | > /tmp/nope'
        const literal = yield* Bash.run({
          command: ['/bin/bash', '-c', 'printf "%s" "$1"', 'name', hostile],
        })
        assert.equal(literal.structuredOutput?.output, hostile)
        assert.equal(yield* boundFs.exists('SHOULD_NOT_EXIST'), false)
        const split = yield* Bash.run({
          command: [
            process.execPath,
            '-e',
            'const fs=require("node:fs");fs.writeSync(1,Buffer.from([0xf0,0x9f]));fs.writeSync(2,Buffer.from([0xc3]));setTimeout(()=>{fs.writeSync(1,Buffer.from([0x98,0x80]));fs.writeSync(2,Buffer.from([0xa9]));},10)',
          ],
        })
        assert.ok(split.structuredOutput?.output.includes('😀'))
        assert.ok(split.structuredOutput?.output.includes('é'))
        assert.ok(!split.structuredOutput?.output.includes('�'))
        const flood = yield* Bash.run(
          { command: 'printf "%100000s" x; printf "\\nLATEST\\n"; printf stderr >&2' },
          { programs: ['/bin/bash'], retention: { retain: 'tail', maxBytes: 16, maxLines: 1 } },
        )
        const ref = flood.structuredOutput?.spill
        if (ref === undefined) return yield* Effect.die('Bun flood must spill')
        assert.deepEqual(yield* provider.revalidate(ref), ref)
        assert.equal((yield* boundFs.readFile(ref.stdoutPath)).length, 100008)
        assert.equal(yield* boundFs.readFileString(ref.stderrPath), 'stderr')
        assert.equal((yield* boundFs.stat(ref.path)).size, 100014n)
        const timeout = yield* Bash.run(
          { command: 'printf BEFORE_TIMEOUT; sleep 30', timeout: 0.1 },
          { programs: ['/bin/bash'] },
        ).pipe(Effect.result)
        assert.equal(timeout._tag, 'Failure')
        if (timeout._tag === 'Failure') {
          assert.ok(timeout.failure instanceof Bash.ShellError)
          if (timeout.failure instanceof Bash.ShellError) {
            assert.equal(timeout.failure.reason, 'timeout')
            assert.equal(timeout.failure.outcome?.output, 'BEFORE_TIMEOUT')
          }
        }
        ready = yield* Deferred.make<void>()
        const caller = yield* Bash.run(
          { command: 'printf BEFORE_CANCEL; sleep 30' },
          { programs: ['/bin/bash'], retention: { retain: 'tail', maxBytes: 4, maxLines: 1 } },
        ).pipe(Effect.forkChild)
        yield* Deferred.await(ready)
        yield* Fiber.interrupt(caller)
        const cancelled = yield* Fiber.await(caller)
        assert.ok(Exit.isFailure(cancelled))
        if (Exit.isFailure(cancelled)) assert.ok(Cause.hasInterrupts(cancelled.cause))
        const latest = yield* Schema.decodeUnknownEffect(Bash.Outcome)(details.at(-1))
        assert.equal(latest.output, 'NCEL')
        if (latest.spill === undefined)
          return yield* Effect.die('Cancellation must retain raw spill metadata')
        assert.equal(yield* boundFs.readFileString(latest.spill.stdoutPath), 'BEFORE_CANCEL')
        assert.deepEqual(yield* provider.revalidate(latest.spill), latest.spill)
        yield* Effect.log(
          'Bun shell conformance: literal argv, split Unicode, complete spill, timeout and final cancellation output passed.',
        )
      }).pipe(Effect.provide(binding(root)), Effect.provideService(ToolExecution, service))
    }),
  ).pipe(Effect.provide(files)),
)
