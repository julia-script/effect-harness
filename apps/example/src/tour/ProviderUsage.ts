import * as Option from 'effect/Option'
import { NodeRuntime } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import { ProviderAffinity } from 'effect-harness/ProviderAffinity'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

// Deterministic, offline: node apps/example/dist/tour/ProviderUsage.js
// Use the harness OpenAI model adapter for automatic prompt_cache_key defaults.
// This custom adapter can read ProviderAffinity in configure and generateText.
const tools = Toolkit.make(Tool.makeResult('paid', { replay: 'safe' }))
const finish = Response.makePart('finish', {
  reason: 'stop',
  usage: new Response.Usage({
    inputTokens: { total: 10, uncached: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 2, text: undefined, reasoning: undefined },
  }),
})
const program = Effect.scoped(
  Effect.gen(function* () {
    const native = yield* LanguageModel.make({
      generateText: ({ prompt }) =>
        Effect.gen(function* () {
          const affinity = yield* Effect.serviceOption(ProviderAffinity)
          if (Option.isSome(affinity))
            yield* Effect.log('Provider request routing hint', affinity.value.id)
          return prompt.content.some((message) => message.role === 'tool')
            ? [Response.makePart('text', { text: 'done' }), finish]
            : [
                Response.makePart('tool-call', {
                  id: 'paid-1',
                  name: 'paid',
                  params: {},
                  providerExecuted: false,
                }),
                finish,
              ]
        }),
      streamText: () => Stream.empty,
    })
    const model = Model.make({
      definition: {
        ref: { provider: 'example', modelId: 'offline' },
        capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
        pricing: {
          inputToken: 0.01,
          outputToken: 0.02,
          currency: 'USD',
          source: 'example-flat-rate-v1',
        },
      },
      languageModel: native,
      options: Schema.Struct({}),
      configure: () => Effect.succeed(Context.empty()),
    })
    const runtime = yield* HarnessRuntime.make({ models: [model], tools }).pipe(
      Effect.provide(
        tools.toLayer({
          paid: () =>
            Effect.succeed({
              content: [],
              spend: { amount: 0.5, currency: 'EUR', source: 'example-invoice' },
            }),
        }),
      ),
    )
    const root = yield* runtime.backend.root
    const submission = yield* runtime.backend.submit({
      conversationId: root,
      draft: { type: 'input', content: 'go' },
    })
    yield* runtime.backend.wait(submission.id)
    const own = yield* Session.usage(runtime.session, root)
    const child = yield* runtime.backend.fork({ conversationId: root })
    const childUsage = yield* Session.usage(runtime.session, child)
    if (
      own.models[0]?.responses !== 2 ||
      own.tools[0]?.results !== 1 ||
      childUsage.models.length !== 0
    )
      return yield* Effect.die('Accounting contract failed')
    yield* Effect.log('Known model/tool subtotals by currency', own.costs)
    yield* Effect.log('Fork starts with no own spend', childUsage)
  }),
).pipe(Effect.provide(Storage.layerMemory))
NodeRuntime.runMain(program)
