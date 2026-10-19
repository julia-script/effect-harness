import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Application from './Application.js'
import * as Documents from './tour/Documents.js'
import * as Forks from './tour/Forks.js'
import * as Extensions from './tour/Extensions.js'

export const ResultSchema = Schema.Struct({
  imports: Schema.Array(Schema.String),
  answer: Schema.String,
  reusedSubmission: Schema.Boolean,
  forks: Schema.Int,
  documentCalls: Schema.Int,
  extensionStatus: Schema.String,
})

const job = { type: 'input', content: 'hello', requestId: 'portable-42' } as const
const run = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const submission = yield* Conversation.submit(root, job)
  const settled = yield* Submission.wait(submission)
  if (settled._tag !== 'InputDone') return { id: submission.id, answer: 'unanswered' }
  const entries = yield* Stream.runCollect(
    Conversation.entries(root).pipe(
      Stream.filter((entry) => entry.id === settled.answer),
      Stream.take(1),
    ),
  )
  const messages = yield* Schema.decodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))(
    entries[0]?.model ?? [],
  )
  const answer = messages
    .flatMap((message) => (message.role === 'assistant' ? message.content : []))
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('\n')
  return { id: submission.id, answer }
})

/** Memory and the native offline provider need no platform services. */
export const program = Effect.gen(function* () {
  const memory = yield* Layer.build(Storage.layerMemory)
  const live = Application.layer.pipe(Layer.provide(Layer.succeedContext(memory)))
  const first = yield* run.pipe(Effect.provide(live))
  const restarted = yield* run.pipe(Effect.provide(live))
  const forks = yield* Forks.program.pipe(Effect.provide(Application.layerMemory))
  const documents = yield* Documents.program.pipe(Effect.provide(Documents.layer))
  const extension = yield* Extensions.program.pipe(Effect.provide(Extensions.layer))
  return {
    imports: ['Harness', 'Conversation', 'Submission', 'Storage', 'Tool', 'Toolkit'],
    answer: restarted.answer,
    reusedSubmission: first.id === restarted.id,
    forks: forks.length,
    documentCalls: documents.snapshot._tag === 'Some' ? documents.snapshot.value.value.count : 0,
    extensionStatus: extension.status,
  }
})
