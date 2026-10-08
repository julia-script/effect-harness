import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as Identity from 'effect-harness/Identity'
import * as Record from 'effect-harness/Record'
import { ConfigProvider, Console, Effect, Layer, Option, Schema } from 'effect'
import { Prompt } from 'effect/ai'
import { BunRuntime, BunServices } from '@effect/platform-bun'
import * as Application from './Application.ts'
import * as Greeting from './Greeting.ts'

class UnansweredSubmissionError extends Schema.TaggedError<UnansweredSubmissionError>()(
  'UnansweredSubmission',
  { result: Record.Submission },
) {}

export const requestId = Identity.RequestId.make('uppercase-v1')

export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const definition = yield* Greeting.make
  const greetingId = yield* harness.transaction(
    Effect.fn('example.admitGreeting')(function* (tx) {
      const run = yield* tx.doc(Greeting.RunDoc, { owner: root.id })
      if (run.taskId !== undefined) return run.taskId
      const prepared = yield* definition.prepare({ name: 'Effect' })
      const id = yield* tx.createTask({
        conversationId: root.id,
        kind: definition.name,
        version: definition.version,
        input: prepared.input,
        background: false,
        abortRequested: false,
        state: { status: 'pending', checkpoint: prepared.checkpoint },
      })
      run.taskId = id
      return id
    }),
  )
  // Opening does not start saved work until the host explicitly resumes it.
  yield* harness.resume
  const greetingTask = yield* harness.awaitTask(greetingId)
  const greetingOutcome = yield* Schema.decodeUnknownEffect(Greeting.Outcome)(
    greetingTask.state.outcome,
  )
  if (greetingOutcome.status !== 'completed')
    return yield* Effect.die(`Greeting ended with ${greetingOutcome.status}`)

  const submission = yield* Conversation.submit(root, 'uppercase hello', { requestId })
  const result = yield* Submission.wait(submission)
  yield* Conversation.awaitIdle(root)
  if (result._tag !== 'InputDone') return yield* new UnansweredSubmissionError({ result })
  const snapshot = yield* Conversation.snapshot(root)
  const answer = yield* Effect.fromOption(
    Option.fromUndefinedOr(snapshot.entries.find((entry) => entry.id === result.answer)),
  )
  const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    answer.model ?? [],
  )
  const text = messages
    .flatMap((message) =>
      message.role === 'assistant'
        ? message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
        : [],
    )
    .join('')
  yield* Console.log(greetingOutcome.result)
  yield* Console.log(text)
  return { greeting: greetingOutcome.result, result, text, greetingId }
})

const platform = Layer.merge(
  ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true })),
  BunServices.layer,
)
const runtime = Application.layer.pipe(Layer.provide(platform))

if (import.meta.main)
  BunRuntime.runMain(
    program.pipe(
      Effect.provide(runtime),
      Effect.tap(() => Console.log('embedded-harness-example-ok')),
    ),
  )
