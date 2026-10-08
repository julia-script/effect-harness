import { BunRuntime, BunServices } from '@effect/platform-bun'
import { Conversation, Identity, Session } from 'effect-harness/durable'
import { Submission } from 'effect-harness/durable/workflow'
import { Model } from 'effect-harness'
import { Console, Context, Effect, Layer, Stream } from 'effect'
import { LanguageModel, Prompt, type Response } from 'effect/ai'
import { Command, Flag } from 'effect/cli'
import * as Application from './Application.ts'
import { finish } from './DemoModel.ts'
import * as Uppercase from './Uppercase.ts'

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
  Layer.provide(BunServices.layer),
)

const program = Effect.fnUntraced(function* (retry: boolean) {
  const session = yield* Session.Session
  const configuration = yield* Conversation.Configuration
  yield* configuration.updateSettings({
    retry: { enabled: retry, maxRetries: 1, baseDelay: 0 },
  })
  const root = yield* session.root()
  yield* session.transaction(
    Effect.fn('reproduction.selectModel')(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
      agent.model = ref
    }),
  )
  yield* Console.log(`Unknown tool: upper_case; offered tool: uppercase; retry: ${retry}`)
  const result = yield* Submission.Submission.execute({
    sessionId: Application.sessionId,
    conversationId: root.id,
    requestId: Identity.RequestId.make('unknown-tool-reproduction'),
    submission: {
      _tag: 'input',
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

/** Parses retry before acquiring the application and its database. */
export const command = Command.make(
  'unknown-tool',
  {
    retry: Flag.Boolean('retry').pipe(Flag.withDefault(false)),
  },
  ({ retry }) => program(retry).pipe(Effect.provide(Runtime)),
)

if (import.meta.main)
  BunRuntime.runMain(
    Command.run(command, { version: '0.0.0' }).pipe(Effect.provide(BunServices.layer)),
  )
