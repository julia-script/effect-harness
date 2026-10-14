import { Effect, Schema, Stream } from 'effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'

export const readAnswer = Effect.fn('readAnswer')(function* (
  conversation: Conversation.Conversation,
  submission: Submission.Submission,
) {
  const settled = yield* Submission.wait(submission)
  if (settled._tag !== 'InputDone') return { status: settled._tag, text: '' }
  const entries = yield* Conversation.entries(conversation).pipe(
    Stream.filter((entry) => entry.id === settled.answer),
    Stream.take(1),
    Stream.runCollect,
  )
  const messages = yield* Schema.decodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))(
    entries[0]?.model ?? [],
  )
  return {
    status: settled._tag,
    text: messages
      .flatMap((message) => (message.role === 'assistant' ? message.content : []))
      .filter((part) => part.type === 'text')
      .map((part) => part.text)
      .join(''),
  }
})

export const logCommittedEntries = (conversation: Conversation.Conversation) =>
  Conversation.entries(conversation).pipe(
    Stream.runForEach((entry) => Effect.log(entry.id, entry.kind)),
  )
