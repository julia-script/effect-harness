import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

const filename = process.argv[2]
const blocked = process.argv[3] === 'block'
const finish = (reason) => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const ModelLive = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: ({ prompt }) =>
      Effect.succeed(
        prompt.content.some((message) => message.role === 'tool')
          ? [{ type: 'text', text: 'done' }, finish('stop')]
          : [
              {
                type: 'tool-call',
                id: 'work-1',
                name: 'work',
                params: {},
                providerExecuted: false,
              },
              finish('tool-calls'),
            ],
      ),
    streamText: () => Stream.empty,
  }),
)
const tools = Toolkit.make(Tool.make('work', { success: Schema.String, replay: 'safe' }))
const ToolsLive = tools.toLayer({
  work: () =>
    Console.log('entered').pipe(Effect.andThen(blocked ? Effect.never : Effect.succeed('done'))),
})
const HarnessLive = Harness.layerLocal({ tools }).pipe(
  Layer.provide(ToolsLive),
  Layer.provide(ModelLive),
  Layer.provide(Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))),
)
const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const submission = yield* root.pipe(
    Conversation.submit({
      type: 'input',
      content: 'Fix the flaky login test',
      requestId: 'job-42',
    }),
  )
  yield* Console.log(`submitted:${submission.id}`)
  const settled = yield* Submission.wait(submission)
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Submission.SettledSchema))(
    settled,
  )
  yield* Console.log(`settled:${encoded}`)
})
await Effect.runPromise(program.pipe(Effect.provide(HarnessLive)))
