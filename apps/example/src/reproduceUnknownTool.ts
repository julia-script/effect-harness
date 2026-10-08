import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Conversation from 'effect-harness/durable/Conversation'
import * as Identity from 'effect-harness/durable/Identity'
import * as Session from 'effect-harness/durable/Session'
import { Submission } from 'effect-harness/durable/workflow/Submission'
import * as Model from 'effect-harness/Model'
import * as Console from 'effect/Console'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Application from './Application.ts'
import { finish } from './DemoModel.ts'
import * as Uppercase from './Uppercase.ts'

const retry = Bun.argv.includes('--retry')
const ref = { provider: 'local', modelId: 'unknown-tool-reproduction' }

const Catalogue = Layer.unwrap(
  Effect.gen(function* () {
    const native = yield* LanguageModel.make({
      generateText: () => Effect.succeed([{ type: 'text', text: 'summary' }, finish('stop')]),
      streamText: ({ prompt }) => {
        const corrected = prompt.content.some(
          (message) =>
            message.role === 'user' &&
            message.content.some(
              (part) =>
                part.type === 'text' &&
                part.text.includes('Your previous response could not be validated'),
            ),
        )
        return Stream.fromIterable<Response.StreamPartEncoded>(
          corrected
            ? [
                { type: 'text-start', id: 'answer' },
                {
                  type: 'text-delta',
                  id: 'answer',
                  delta: 'Recovered using validation feedback.',
                },
                { type: 'text-end', id: 'answer' },
                finish('stop'),
              ]
            : [
                {
                  type: 'tool-call',
                  id: 'unknown-call',
                  name: 'upper_case', // Only "uppercase" is registered.
                  params: { text: 'abcdef' },
                  providerExecuted: false,
                },
                finish('tool-calls'),
              ],
        )
      },
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

const Tools = Uppercase.layerRegistry.pipe(Layer.provide(Uppercase.layerHandlers))
const Runtime = Application.layerNoDeps.pipe(
  Layer.provide(Layer.mergeAll(Catalogue, Tools)),
  Layer.provideMerge(Application.Configuration),
  Layer.provide(BunServices.layer),
)

const program = Effect.gen(function* () {
  const session = yield* Session.Session
  const configuration = yield* Conversation.Configuration
  yield* configuration.updateSettings({
    retry: { enabled: retry, maxRetries: 1, baseDelayMs: 0 },
  })
  const root = yield* session.root()
  yield* session.transaction(
    Effect.fn('reproduction.selectModel')(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
      agent.model = ref
    }),
  )
  yield* Console.log(`Unknown tool: upper_case; offered tool: uppercase; retry: ${retry}`)
  const result = yield* Submission.execute({
    sessionId: Application.sessionId,
    conversationId: root.id,
    requestId: Identity.RequestId.make('unknown-tool-reproduction'),
    submission: {
      _tag: 'input',
      type: 'input',
      message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'uppercase hello' })] }),
    },
  })
  yield* Conversation.awaitIdle(session, root.id)
  yield* Console.log(result)
  const context = yield* Conversation.context(session, root.id)
  for (const entry of context.entries)
    if (entry.error !== undefined)
      yield* Console.log('Recorded request error:', entry.error.reason._tag)
})

BunRuntime.runMain(program.pipe(Effect.provide(Runtime)))
