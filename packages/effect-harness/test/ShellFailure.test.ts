import { assert, describe, it } from '@effect/vitest'
import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import { ExecutionError } from '../src/ExecutionError.ts'
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
describe('shell reporting failure boundaries', () => {
  it.effect('spill publication failure still emits final latest retained state', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
        const details: Schema.Json[] = []
        const service = ToolExecution.of({
          conversationId: yield* Schema.decodeEffect(Identity.ConversationId)(1),
          taskId: yield* Schema.decodeEffect(Record.TaskId)(1),
          callId: 'spill-failure',
          snapshot: () => Effect.succeedNone,
          watch: () => Stream.empty,
          commit: () => Effect.die('Unexpected commit'),
          output: () => Effect.void,
          diagnostic: () => Effect.void,
          details: (value) =>
            Effect.sync(() => {
              details.push(value)
            }),
        })
        const spill = Layer.succeed(
          ToolSpill.ToolSpill,
          ToolSpill.ToolSpill.of({
            open: Effect.succeed({
              append: () => Effect.void,
              commit: Effect.fail(
                new ToolSpill.ToolSpillError({ reason: 'write', message: 'Publication failed' }),
              ),
            }),
            revalidate: () =>
              Effect.fail(
                new ToolSpill.ToolSpillError({
                  reason: 'unavailable',
                  message: 'No published reference',
                }),
              ),
            remove: () => Effect.void,
          }),
        )
        const bound = Layer.merge(
          ToolkitFileSystem.layer({ root }),
          ToolkitProcess.layer({ root }),
        ).pipe(Layer.provide(native))
        const services = Layer.merge(ShellProcess.layerNative, spill).pipe(
          Layer.provideMerge(bound),
        )
        const result = yield* Bash.run(
          { command: 'printf LATEST' },
          { programs: ['/bin/bash'], retention: { retain: 'tail', maxBytes: 4, maxLines: 1 } },
        ).pipe(
          Effect.result,
          Effect.provide(services),
          Effect.provideService(ToolExecution, service),
        )
        assert.isTrue(Result.isFailure(result))
        if (Result.isFailure(result)) {
          assert.instanceOf(result.failure, Bash.ShellError)
          if (result.failure instanceof Bash.ShellError) {
            assert.strictEqual(result.failure.reason, 'spill')
            assert.strictEqual(result.failure.outcome?.output, 'TEST')
            assert.isUndefined(result.failure.outcome?.spill)
            assert.deepStrictEqual(details.at(-1), result.failure.outcome)
          }
        }
      }),
    ).pipe(Effect.provide(files)),
  )

  it.effect(
    'durable progress failures remain infrastructure failures and still attempt final details',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
          const details: Schema.Json[] = []
          const failure = new ExecutionError({
            reason: 'notFound',
            operation: 'output',
            message: 'Report storage failed',
          })
          const service = ToolExecution.of({
            conversationId: yield* Schema.decodeEffect(Identity.ConversationId)(1),
            taskId: yield* Schema.decodeEffect(Record.TaskId)(1),
            callId: 'report-failure',
            snapshot: () => Effect.succeedNone,
            watch: () => Stream.empty,
            commit: () => Effect.die('Unexpected commit'),
            output: () => Effect.fail(failure),
            diagnostic: () => Effect.void,
            details: (value) =>
              Effect.sync(() => {
                details.push(value)
              }),
          })
          const bound = Layer.merge(
            ToolkitFileSystem.layer({ root }),
            ToolkitProcess.layer({ root }),
          ).pipe(Layer.provide(native))
          const services = Layer.merge(
            ShellProcess.layerNative,
            ToolSpill.layer({
              provider: root,
              directory: `${root}/spills`,
              lifetime: 'persistent',
            }),
          ).pipe(Layer.provideMerge(bound))
          const result = yield* Bash.run(
            { command: 'printf BEFORE_REPORT_FAILURE; sleep 30' },
            { programs: ['/bin/bash'] },
          ).pipe(
            Effect.result,
            Effect.provide(services),
            Effect.provideService(ToolExecution, service),
          )
          assert.isTrue(Result.isFailure(result))
          if (Result.isFailure(result)) assert.strictEqual(result.failure, failure)
          const latest = yield* Schema.decodeUnknownEffect(Bash.Outcome)(details.at(-1))
          assert.strictEqual(latest.output, 'BEFORE_REPORT_FAILURE')
          assert.deepStrictEqual(yield* fs.readDirectory(`${root}/spills`), [])
        }),
      ).pipe(Effect.provide(files)),
  )
})
