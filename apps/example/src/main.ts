import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Identity from '@effect-harness/durable/Identity'
import { ROOT_CONVERSATION_ID } from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Console from 'effect/Console'
import * as Data from 'effect/Data'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Application from './Application.ts'
import * as DemoModel from './DemoModel.ts'
import * as Greeting from './Greeting.ts'

class UnansweredSubmission extends Data.TaggedError('UnansweredSubmission')<{
  readonly result: typeof Submission.successSchema.Type
}> {}

// A stable request identity reuses its persisted receipt across process runs.
export const input: typeof Submission.payloadSchema.Type = {
  sessionId: Application.sessionId,
  conversationId: ROOT_CONVERSATION_ID,
  requestId: Identity.RequestId.make('uppercase-v1'),
  submission: {
    _tag: 'input',
    type: 'input',
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
  const result = yield* Submission.execute(input)
  yield* Conversation.awaitIdle(session, root.id)

  if (result._tag !== 'InputDone') return yield* new UnansweredSubmission({ result })
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
const MainLive = Application.layer.pipe(Layer.provide(Platform))

if (import.meta.main) {
  BunRuntime.runMain(
    program.pipe(
      Effect.provide(MainLive),
      Effect.tap(() => Console.log('native-workflow-example-ok')),
    ),
  )
}
