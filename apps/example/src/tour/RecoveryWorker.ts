/** A separate process is killed after a safe tool has committed partial progress. */
import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Console from 'effect/Console'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Conversation from 'effect-harness/Conversation'
import * as Identity from 'effect-harness/Identity'
import * as Invocation from 'effect-harness/Invocation'
import * as Observation from 'effect-harness/Observation'
import { Persistence } from 'effect-harness/Persistence'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as ToolError from 'effect-harness/ToolError'
import * as SqliteBun from 'effect-harness/storage/SqliteBun'
import * as Runtime from './Runtime.ts'

const Args = Schema.Tuple([Schema.NonEmptyString, Schema.Literals(['start', 'resume', 'verify'])])
const search = Tool.make('search', {
  description: 'Search a simulated issue tracker.',
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.String,
  failure: ToolError.ToolError,
}).addDependency(Invocation.ToolCall)
const toolkit = Toolkit.make(search)
const provider: Runtime.Provider = {
  generateText: () => Effect.succeed([{ type: 'text', text: 'summary' }, Runtime.finish('stop')]),
  streamText: ({ prompt }) =>
    prompt.content.some((message) => message.role === 'tool')
      ? Runtime.answer('Found issue #42.')
      : Stream.fromIterable([
          {
            type: 'tool-call' as const,
            id: 'search-42',
            name: 'search',
            params: { query: 'login' },
            providerExecuted: false,
          },
          Runtime.finish('tool-calls'),
        ]),
}

const program = Effect.scoped(
  Effect.gen(function* () {
    const [filename, mode] = yield* Schema.decodeUnknownEffect(Args)(process.argv.slice(2))
    const fs = yield* FileSystem.FileSystem
    const tools = yield* ToolRegistration.bind(toolkit, { search: { replay: 'safe' } }).pipe(
      Effect.provide(
        toolkit.toLayer({
          search: Effect.fn('tour.search')(function* () {
            yield* fs.writeFileString(filename + '.audit', mode + '\n', { flag: 'a' }).pipe(
              Effect.mapError(
                (cause) =>
                  new ToolError.ToolError({
                    reason: new ToolError.ToolExecutionError({
                      name: 'search',
                      message: cause.message,
                      cause,
                    }),
                  }),
              ),
            )
            if (mode === 'start') {
              yield* (yield* Invocation.ToolCall).output('Searching the issue tracker...')
              yield* Console.log('READY')
              return yield* Effect.never
            }
            return 'issue #42'
          }),
        }),
      ),
    )
    const storage = yield* Layer.build(SqliteBun.layer({ filename }))
    const { harness } = yield* Runtime.open({
      store: Context.get(storage, Persistence),
      provider,
      extensions: [{ name: 'issues', tools }],
    })
    const root = yield* harness.root
    if (mode === 'resume') yield* harness.resume
    const answer = yield* Runtime.ask(root, 'Find the login issue', {
      requestId: Identity.RequestId.make('issue-job-42'),
    })
    yield* Conversation.awaitIdle(root)
    const snapshot = yield* Conversation.snapshot(root)
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Observation.Snapshot))(
      snapshot,
    )
    yield* Runtime.check(answer.text === 'Found issue #42.', 'Recovery returned the wrong answer')
    yield* Console.log(`DONE:${encoded}`)
  }),
)

if (import.meta.main) BunRuntime.runMain(program.pipe(Effect.provide(BunServices.layer)))
