/**
 * Run: bun apps/example/src/tour/Recovery.ts ./agent.sqlite start
 * Send SIGKILL to that process after it prints "Paused", then run the same file:
 *      bun apps/example/src/tour/Recovery.ts ./agent.sqlite resume
 * The offline model below calls a safe search tool, then answers with its result.
 */
import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Console from 'effect/Console'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Conversation from 'effect-harness/Conversation'
import * as Identity from 'effect-harness/Identity'
import * as Invocation from 'effect-harness/Invocation'
import { Persistence } from 'effect-harness/Persistence'
import * as Submission from 'effect-harness/Submission'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as ToolError from 'effect-harness/ToolError'
import * as SqliteBun from 'effect-harness/storage/SqliteBun'
import * as Runtime from './Runtime.ts'

const Mode = Schema.Literals(['start', 'resume'])
const Args = Schema.Tuple([Schema.NonEmptyString, Mode])
export const Result = Submission.Record
const job = { content: 'Fix the flaky login test', requestId: Identity.RequestId.make('job-42') }
export const recover = Effect.fn('recovery')(function* (filename: string, mode: typeof Mode.Type) {
  const storage = yield* Layer.build(SqliteBun.layer({ filename }))
  const { harness } = yield* Runtime.open({
    store: Context.get(storage, Persistence),
    provider,
    extensions: [{ name: 'issues', tools: yield* searchTools(mode) }],
  })
  // A fresh process opens the same SQLite file and resumes interrupted work.
  yield* harness.resume
  const root = yield* harness.root
  const submission = yield* Conversation.submit(root, job.content, { requestId: job.requestId })
  return yield* Submission.wait(submission)
})

// The tour runs the completion path; the test fixture verifies a real process kill.
export const run = Effect.scoped(recover(':memory:', 'resume'))

// Offline model and tool setup, including a deliberate pause to simulate a crash.
const toolkit = Toolkit.make(
  Tool.make('search', {
    description: 'Search a simulated issue tracker.',
    parameters: Schema.Struct({ query: Schema.String }),
    success: Schema.String,
    failure: ToolError.ToolError,
  }).addDependency(Invocation.ToolCall),
)
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

const searchTools = Effect.fn('recovery.searchTools')(function* (mode: typeof Mode.Type) {
  return yield* ToolRegistration.bind(toolkit, { search: { replay: 'safe' } }).pipe(
    Effect.provide(
      toolkit.toLayer({
        search: Effect.fn('recovery.search')(function* () {
          yield* Console.log('Searching the issue tracker...')
          if (mode === 'start') {
            yield* (yield* Invocation.ToolCall).output('Searching the issue tracker...')
            yield* Console.log(
              'Paused after committing tool progress. Kill this process, then run resume.',
            )
            return yield* Effect.never
          }
          return 'issue #42'
        }),
      }),
    ),
  )
})

if (import.meta.main)
  BunRuntime.runMain(
    Effect.scoped(
      Effect.gen(function* () {
        const [filename, mode] = yield* Schema.decodeUnknownEffect(Args)(process.argv.slice(2))
        const settled = yield* recover(filename, mode)
        yield* Console.log(yield* Schema.encodeEffect(Schema.fromJsonString(Result))(settled))
      }),
    ).pipe(Effect.provide(BunServices.layer)),
  )
