import { assert, describe, it } from '@effect/vitest'
import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Identity from '../src/Identity.ts'
import * as Record from '../src/Record.ts'
import { ToolExecution } from '../src/ToolExecution.ts'
import * as ToolSpill from '../src/ToolSpill.ts'
import * as ToolkitFileSystem from '../src/ToolkitFileSystem.ts'
import * as ToolkitProcess from '../src/ToolkitProcess.ts'
import * as Bash from '../src/coding/Bash.ts'
import * as ShellProcess from '../src/coding/ShellProcess.ts'

const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const native = NodeChildProcessSpawner.layer.pipe(Layer.provideMerge(files))
const binding = (root: string) => {
  const bound = Layer.merge(ToolkitFileSystem.layer({ root }), ToolkitProcess.layer({ root })).pipe(
    Layer.provide(native),
  )
  return Layer.merge(
    ShellProcess.layerNative,
    ToolSpill.layer({ provider: root, directory: `${root}/spills`, lifetime: 'persistent' }),
  ).pipe(Layer.provideMerge(bound))
}
const invocation = Effect.fnUntraced(function* (root: string) {
  const ready = yield* Deferred.make<readonly [number, number]>()
  const details: Schema.Json[] = []
  const chunks: string[] = []
  const service = ToolExecution.of({
    conversationId: yield* Schema.decodeEffect(Identity.ConversationId)(1),
    taskId: yield* Schema.decodeEffect(Record.TaskId)(1),
    callId: root,
    snapshot: () => Effect.succeedNone,
    watch: () => Stream.empty,
    commit: () => Effect.die('Unexpected commit'),
    diagnostic: () => Effect.void,
    details: (value) =>
      Effect.sync(() => {
        details.push(value)
      }),
    output: (chunk) =>
      Effect.gen(function* () {
        chunks.push(chunk)
        const text = chunks.join('')
        const match = /^(\d+) (\d+)\n/.exec(text)
        if (match !== null) yield* Deferred.succeed(ready, [Number(match[1]), Number(match[2])])
      }),
  })
  const run = Bash.run(
    { command: 'sleep 30 & printf "%s %s\\n" "$$" "$!"; wait' },
    { programs: ['/bin/bash'] },
  ).pipe(Effect.provide(binding(root)), Effect.provideService(ToolExecution, service))
  const fiber = yield* run.pipe(Effect.forkChild)
  const pids = yield* Deferred.await(ready)
  return { fiber, pids, details }
})
const running = Effect.fnUntraced(function* (pid: number) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const result = yield* spawner
    .string(ChildProcess.make('/bin/ps', ['-p', String(pid), '-o', 'stat=']))
    .pipe(Effect.result)
  if (result._tag === 'Failure') return false
  return result.success.trim().length > 0 && !result.success.trim().startsWith('Z')
})
describe('native best-effort process groups', () => {
  it.live(
    'caller cancellation closes its real Bash descendants and leaves another toolkit running',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const rootA = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
          const rootB = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
          const a = yield* invocation(rootA)
          const b = yield* invocation(rootB)
          assert.isTrue(yield* running(a.pids[0]))
          assert.isTrue(yield* running(a.pids[1]))
          assert.isTrue(yield* running(b.pids[0]))
          assert.isTrue(yield* running(b.pids[1]))
          yield* Fiber.interrupt(a.fiber)
          const exit = yield* Fiber.await(a.fiber)
          assert.isTrue(Exit.isFailure(exit))
          if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause))
          assert.isFalse(yield* running(a.pids[0]))
          assert.isFalse(yield* running(a.pids[1]))
          assert.isTrue(yield* running(b.pids[0]))
          assert.isTrue(yield* running(b.pids[1]))
          assert.strictEqual(a.details.length, 1)
          yield* Fiber.interrupt(b.fiber)
          assert.isFalse(yield* running(b.pids[0]))
          assert.isFalse(yield* running(b.pids[1]))
          assert.strictEqual(b.details.length, 1)
        }),
      ).pipe(Effect.provide(native)),
    15000,
  )
})
