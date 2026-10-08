import { Conversation, Identity, Record, Session } from 'effect-harness/durable'
import { Submission } from 'effect-harness/durable/workflow'
import { ConfigProvider, Console, Effect, Layer, Schema } from 'effect'
import { Prompt } from 'effect/ai'
import { BunRuntime, BunServices } from '@effect/platform-bun'

import * as Application from './Application.ts'
import * as DemoModel from './DemoModel.ts'
import * as Greeting from './Greeting.ts'

class UnansweredSubmissionError extends Schema.TaggedError<UnansweredSubmissionError>()(
  'UnansweredSubmission',
  { result: Submission.Submission.successSchema },
) {}

// A stable request identity reuses its persisted receipt across process runs.
export const input: typeof Submission.Submission.payloadSchema.Type = {
  sessionId: Application.sessionId,
  conversationId: Record.ROOT_CONVERSATION_ID,
  requestId: Identity.RequestId.make('uppercase-v1'),
  submission: {
    _tag: 'input',
    message: Prompt.userMessage({ content: [Prompt.textPart({ text: 'uppercase hello' })] }),
  },
}

export const program = Effect.gen(function* () {
  const session = yield* Session.Session
  const root = yield* session.root()

  yield* session.transaction(
    Effect.fn('example.configureAgent')(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
      agent.model = DemoModel.ref
    }),
  )

  // Custom and built-in Workflows both use the ordinary native execution API.
  const greeting = yield* Greeting.Greeting.execute({ name: 'Effect' })
  const result = yield* Submission.Submission.execute(input)
  yield* Conversation.awaitIdle(session, root.id)

  if (result._tag !== 'InputDone') return yield* new UnansweredSubmissionError({ result })
  const answer = yield* session
    .entry(result.answer, root.id)
    .pipe(Effect.flatMap(Effect.fromOption))
  const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    answer.entry.model ?? [],
  )
  const text = messages
    .flatMap((message) =>
      message.role === 'assistant'
        ? message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
        : [],
    )
    .join('')

  yield* Console.log(greeting)
  yield* Console.log(text)
  return { greeting, result, text }
})

const Platform = Layer.merge(
  ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true })),
  BunServices.layer,
)
// effect-nit-allow P3-layer-naming-layer-prefix: MainLayer composes the example Application with its selected config provider and platform services; it is application wiring rather than one module's implementation layer.
const MainLayer = Application.layer.pipe(Layer.provide(Platform))

if (import.meta.main) {
  BunRuntime.runMain(
    program.pipe(
      Effect.provide(MainLayer),
      Effect.tap(() => Console.log('native-workflow-example-ok')),
    ),
  )
}
