/** Shared local model and scoped composition for the runnable tour. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import type * as Agent from 'effect-harness/Agent'
import type * as Conversation from 'effect-harness/Conversation'
import * as Executor from 'effect-harness/Executor'
import type * as Extension from 'effect-harness/Extension'
import * as Harness from 'effect-harness/Harness'
import * as Model from 'effect-harness/Model'
import { Persistence } from 'effect-harness/Persistence'
import * as Registry from 'effect-harness/Registry'
import * as Submission from 'effect-harness/Submission'
import type * as Task from 'effect-harness/Task'
import * as Memory from 'effect-harness/storage/Memory'

export class ExampleError extends Schema.TaggedError<ExampleError>()('ExampleError', {
  message: Schema.String,
}) {}

export const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new ExampleError({ message }))

export const ref = { provider: 'tour', modelId: 'local' }
export const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: 5, reasoning: undefined },
  },
  response: undefined,
})

export const answer = (text: string): Stream.Stream<Response.StreamPartEncoded> =>
  Stream.fromIterable<Response.StreamPartEncoded>([
    { type: 'text-start', id: 'answer' },
    { type: 'text-delta', id: 'answer', delta: text },
    { type: 'text-end', id: 'answer' },
    finish('stop'),
  ])

export const lastUserText = (prompt: Prompt.Prompt): string =>
  prompt.content
    .filter((message) => message.role === 'user')
    .at(-1)
    ?.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
    .join('') ?? ''

export type Provider = Parameters<typeof LanguageModel.make>[0]
const provider: Provider = {
  generateText: () =>
    Effect.succeed([{ type: 'text', text: 'Summary of earlier messages.' }, finish('stop')]),
  streamText: ({ prompt }) => answer(`Reply: ${lastUserText(prompt)}`),
}

export interface Options {
  readonly extensions?: ReadonlyArray<Extension.Extension>
  readonly tasks?: ReadonlyArray<Task.BoundDefinition>
  readonly store?: Persistence['Service']
  readonly provider?: Provider
  readonly agent?: Agent.State
  readonly settings?: Agent.Settings.Input
}

/** Each caller owns its Scope; SQLite can replace the default in-memory store. */
export const open = Effect.fn('tour.open')(function* (options: Options = {}) {
  const store = options.store ?? (yield* Memory.make)
  const model = yield* LanguageModel.make(options.provider ?? provider)
  const registry = yield* Registry.make(options.extensions ?? [])
  const catalogue = Model.layer([
    {
      ref,
      model,
      contextWindow: 100000,
      maxOutputTokens: 1000,
      configure: () => Effect.succeed(Context.empty()),
    },
  ])
  const executor = yield* Executor.Executor.pipe(
    Effect.provide(Executor.layer.pipe(Layer.provide(catalogue))),
    Effect.provideService(Registry.Registry, registry),
  )
  const harness = yield* Harness.make({
    agent: { model: ref, ...options.agent },
    settings: {
      retry: { enabled: false },
      compaction: { enabled: false },
      progress: { partialInterval: '0 millis', outputInterval: '0 millis' },
      ...options.settings,
    },
    tasks: options.tasks ?? [],
  }).pipe(
    Effect.provideService(Persistence, store),
    Effect.provideService(Executor.Executor, executor),
  )
  return { harness, registry }
})

/** Read the committed answer rather than a model callback's transient value. */
export const ask = Effect.fn('tour.ask')(function* (
  conversation: Conversation.Conversation,
  input: string,
  options?: Harness.SubmitOptions,
) {
  const submission = yield* conversation.harness.submit(conversation.id, input, options)
  const settled = yield* Submission.wait(submission)
  if (settled._tag !== 'InputDone')
    return yield* new ExampleError({ message: `Submission ended as ${settled._tag}` })
  const entry = yield* conversation.harness.transaction((tx) => tx.entry(settled.answer))
  if (Option.isNone(entry))
    return yield* new ExampleError({ message: 'Committed answer is missing' })
  const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Schema.Array(Prompt.Message)))(
    entry.value.model ?? [],
  )
  const text = messages
    .flatMap((message) =>
      message.role === 'assistant'
        ? message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
        : [],
    )
    .join('')
  return { submission: settled, text }
})
