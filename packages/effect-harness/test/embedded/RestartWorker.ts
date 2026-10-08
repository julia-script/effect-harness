/** Executable subprocess fixture: markers are emitted only after committed recovery boundaries. */
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as BunServices from '@effect/platform-bun/BunServices'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Executor from '../../src/Executor.ts'
import * as Harness from '../../src/Harness.ts'
import * as Invocation from '../../src/Invocation.ts'
import * as Observation from '../../src/Observation.ts'
import { ToolError, ToolExecutionError } from '../../src/ToolError.ts'
import * as Sqlite from '../../src/storage/SqliteBun.ts'
import * as Native from './NativeFixture.ts'

const Arguments = Schema.Tuple([
  Schema.String,
  Schema.Literals(['start', 'resume', 'verify']),
  Schema.Literals(['safe', 'unsafe', 'model']),
])
const main = Effect.gen(function* () {
  // effect-nit-allow P1-native-process-argv: this executable fixture's CLI boundary decodes argv once before constructing services.
  const [filename, phase, replay] = yield* Schema.decodeUnknownEffect(Arguments)(
    process.argv.slice(2),
  )
  const fs = yield* FileSystem.FileSystem
  const { executor: nativeExecutor } = yield* Native.makeExecutor({
    replay: replay === 'model' ? 'safe' : replay,
    ...(replay === 'model'
      ? {
          provider: {
            ...Native.provider,
            streamText: () =>
              phase === 'start'
                ? Stream.fromIterable(
                    Native.answer('committed partial model output').slice(0, 3),
                  ).pipe(
                    Stream.concat(Stream.fromEffect(Console.log('READY')).pipe(Stream.drain)),
                    Stream.concat(Stream.never),
                  )
                : Stream.fromIterable(Native.answer('Recovered model answer')),
          },
        }
      : {}),
    handle: Effect.fn('restart.tool')(function* ({ text }) {
      yield* fs.writeFileString(filename + '.audit', phase + '\n', { flag: 'a' }).pipe(
        Effect.mapError(
          (cause) =>
            new ToolError({
              reason: new ToolExecutionError({
                name: 'uppercase',
                message: cause.message,
                cause,
              }),
            }),
        ),
      )
      if (phase === 'start') {
        const call = yield* Invocation.ToolCall
        yield* call.output('committed partial output')
        yield* call.details({ committed: true })
        yield* Console.log('READY')
        return yield* Effect.never
      }
      return text.toUpperCase()
    }),
  })
  const executor: Executor.Executor['Service'] =
    replay !== 'model'
      ? nativeExecutor
      : {
          ...nativeExecutor,
          generate: (request, agent, options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const encoded = yield* Schema.encodeEffect(
                  Schema.fromJsonString(Schema.toCodecJson(Executor.Request)),
                )(request).pipe(Effect.orDie)
                if (phase === 'start')
                  yield* fs.writeFileString(filename + '.request', encoded).pipe(Effect.orDie)
                else {
                  const saved = yield* fs.readFileString(filename + '.request').pipe(Effect.orDie)
                  if (saved !== encoded)
                    return yield* Effect.die('Recovered generation changed its pinned request')
                }
                yield* fs
                  .writeFileString(filename + '.audit', phase + '\n', { flag: 'a' })
                  .pipe(Effect.orDie)
                return nativeExecutor.generate(request, agent, options)
              }),
            ),
        }
  yield* Effect.gen(function* () {
    const harness = yield* Harness.make({
      agent: { model: Native.ref },
      settings: {
        retry: { enabled: false },
        compaction: { enabled: false },
        progress: { partialInterval: '0 millis', outputInterval: '0 millis' },
      },
    })
    const conversation = yield* harness.root
    if (phase === 'start') {
      const submission = yield* harness.submit(conversation.id, 'hello')
      yield* Console.log(`ADMITTED:${submission.id}`)
      yield* harness.awaitSubmission(submission.id)
      return
    }
    const before = yield* harness.snapshot(conversation.id)
    const submission = before.submissions[0]
    if (submission === undefined) return yield* Effect.die('Missing committed submission')
    yield* harness.resume
    yield* harness.awaitSubmission(submission.id)
    const snapshot = yield* harness.snapshot(conversation.id)
    yield* Console.log(
      'DONE:' + (yield* Schema.encodeEffect(Schema.fromJsonString(Observation.Snapshot))(snapshot)),
    )
  }).pipe(
    Effect.provideService(Executor.Executor, executor),
    Effect.provide(Sqlite.layer({ filename })),
  )
})
BunRuntime.runMain(Effect.scoped(main).pipe(Effect.provide(BunServices.layer)))
