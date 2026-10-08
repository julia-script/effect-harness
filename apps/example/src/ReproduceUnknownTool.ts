import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as Model from 'effect-harness/Model'
import { Console, Context, Effect, Layer, Stream } from 'effect'
import { LanguageModel, type Response } from 'effect/ai'
import { Command, Flag } from 'effect/cli'
import * as Application from './Application.ts'
import { finish } from './DemoModel.ts'
import * as Uppercase from './Uppercase.ts'

const ref = { provider: 'local', modelId: 'unknown-tool-reproduction' }
const catalogue = Layer.unwrap(
  Effect.gen(function* () {
    const native = yield* LanguageModel.make({
      generateText: () => Effect.succeed([{ type: 'text', text: 'summary' }, finish('stop')]),
      streamText: () =>
        Stream.fromIterable<Response.StreamPartEncoded>([
          {
            type: 'tool-call',
            id: 'unknown-call',
            name: 'upper_case',
            params: { text: 'abcdef' },
            providerExecuted: false,
          },
          finish('tool-calls'),
        ]),
    })
    return Model.layer([
      {
        ref,
        model: native,
        contextWindow: 100000,
        maxOutputTokens: 1000,
        configure: () => Effect.succeed(Context.empty()),
      },
    ])
  }),
)
const tools = Uppercase.layerRegistry.pipe(Layer.provide(Uppercase.layerHandlers))
const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  yield* Console.log('Unknown tool: upper_case; offered tool: uppercase')
  const submission = yield* Conversation.submit(root, 'uppercase hello')
  yield* Console.log(yield* Submission.wait(submission))
})

/** Parse options before acquiring the harness and its database. */
export const command = Command.make(
  'unknown-tool',
  { retry: Flag.Boolean('retry').pipe(Flag.withDefault(false)) },
  ({ retry }) =>
    program.pipe(
      Effect.provide(
        Application.layerWith({
          agent: { model: ref },
          settings: {
            retry: { enabled: retry, maxRetries: 1, baseDelay: 0 },
            compaction: { enabled: false },
          },
        }).pipe(Layer.provide(Layer.merge(catalogue, tools)), Layer.provide(BunServices.layer)),
      ),
    ),
)
if (import.meta.main)
  BunRuntime.runMain(
    Command.run(command, { version: '0.0.0' }).pipe(Effect.provide(BunServices.layer)),
  )
