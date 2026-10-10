/** Explicit deferred selection persists one handle and polls the same prepared request. */
import * as Clock from 'effect/Clock'
import * as Console from 'effect/Console'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'

const program = Effect.scoped(
  Effect.gen(function* () {
    const deferred = yield* Model.makeDeferred<Schema.String, never>({
      handle: Schema.String,
      start: () =>
        Clock.currentTimeMillis.pipe(
          Effect.map((now) => ({
            _tag: 'Deferred',
            handle: 'example-response',
            nextPollAt: now + 10,
          })),
        ),
      poll: (handle) =>
        Effect.succeed({
          _tag: 'Complete',
          response: new LanguageModel.GenerateTextResponse([
            Response.makePart('text', { text: `Completed ${handle}` }),
          ]),
        }),
      cancel: () => Effect.void,
    })
    const languageModel = yield* LanguageModel.make({
      generateText: () => Effect.die('Deferred selection'),
      streamText: () => Stream.die('Deferred selection'),
    })
    const model = Model.make({
      definition: {
        ref: { provider: 'example', modelId: 'deferred' },
        capabilities: { tools: true, reasoning: false, images: false, structuredOutput: false },
      },
      options: Schema.JsonObject,
      languageModel,
      deferred,
      configure: () => Effect.succeed(Context.empty()),
    })
    const settings = yield* Settings.make({ stream: { deferred: { window: '15m' } } })
    const runtime = yield* HarnessRuntime.make({ models: [model] }).pipe(
      Effect.provideService(Settings.Settings, settings),
    )
    const conversationId = yield* runtime.backend.root
    const job = yield* runtime.backend.submit({
      conversationId,
      draft: { type: 'input', content: 'Complete this request in the background.' },
    })
    yield* Console.log((yield* runtime.backend.wait(job.id)).status)
    yield* Console.log(yield* Session.usage(runtime.session, conversationId))
  }),
).pipe(Effect.provide(Storage.layerMemory))

// Closing a runtime stops local polling. Explicit abort also attempts remote cancellation.
// Custom uninterruptible cancellation can delay the owning Scope's close until its work finishes.
await Effect.runPromise(program)
