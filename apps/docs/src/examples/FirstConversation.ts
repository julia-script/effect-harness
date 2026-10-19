import { Effect, Layer, Schema, Stream } from 'effect'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'

const finish: Response.FinishPartEncoded = {
  type: 'finish',
  reason: 'stop',
  response: undefined,
  usage: {
    inputTokens: { total: 0, uncached: 0, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 0, text: 0, reasoning: undefined },
  },
}
const ModelLive = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([{ type: 'text', text: 'Hello, harness!' }, finish]),
    streamText: () =>
      Stream.fromIterable([
        { type: 'text-start', id: 'hello' },
        { type: 'text-delta', id: 'hello', delta: 'Hello, ' },
        { type: 'text-delta', id: 'hello', delta: 'harness!' },
        { type: 'text-end', id: 'hello' },
        finish,
      ]),
  }),
)
const HarnessLive = Harness.layerLocal().pipe(
  Layer.provide(ModelLive),
  Layer.provide(Storage.layerMemory),
)
const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const input = { type: 'input', content: 'Say hello', requestId: 'hello-1' } as const
  const first = yield* Conversation.submit(root, input)
  const settled = yield* Submission.wait(first)
  if (settled._tag !== 'InputDone') return
  const entries = yield* Conversation.entries(root).pipe(
    Stream.filter((entry) => entry.id === settled.answer),
    Stream.take(1),
    Stream.runCollect,
  )
  const messages = yield* Schema.decodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))(
    entries[0]?.model ?? [],
  )
  const answer = messages
    .flatMap((message) => (message.role === 'assistant' ? message.content : []))
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('')
  const again = yield* Conversation.submit(root, input)
  console.log(answer)
  console.log('Same submission:', first.id === again.id)
})

await Effect.runPromise(program.pipe(Effect.provide(HarnessLive)))
