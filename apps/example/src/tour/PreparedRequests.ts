/** A public memory runtime commits the request identity and final prompt before this local provider runs. */
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Context from 'effect/Context'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Storage from 'effect-harness/Storage'

const program = Effect.scoped(
  Effect.gen(function* () {
    const native = yield* LanguageModel.make({
      generateText: () =>
        Effect.gen(function* () {
          const request = Option.getOrThrow(
            yield* Effect.serviceOption(Prepared.PreparedRequest),
          ).current
          yield* Console.log(
            `Committed request ${request.requestId}, logical request ${request.ordinal}, attempt ${request.attempt}`,
          )
          return [
            { type: 'text' as const, text: 'Prepared before provider I/O.' },
            {
              type: 'finish' as const,
              reason: 'stop' as const,
              usage: {
                inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
                outputTokens: { total: 1, text: 1, reasoning: undefined },
              },
              response: undefined,
            },
          ]
        }),
      streamText: () => Stream.empty,
    })
    const model = Model.make({
      definition: {
        ref: { provider: 'local', modelId: 'example' },
        capabilities: { tools: false, images: false, reasoning: false, structuredOutput: false },
      },
      languageModel: native,
      options: Schema.Struct({}),
      configure: () => Effect.succeed(Context.empty()),
    })
    const runtime = yield* HarnessRuntime.make({ models: [model] })
    const root = yield* runtime.backend.root
    const submission = yield* runtime.backend.submit({
      conversationId: root,
      draft: { type: 'input', content: 'Explain preparation.' },
    })
    yield* Console.log((yield* runtime.backend.wait(submission.id)).status)
  }),
).pipe(Effect.provide(Storage.layerMemory))

await Effect.runPromise(program)
