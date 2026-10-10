import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import { Usage as RootUsage, ProviderAffinity as RootAffinity } from 'effect-harness'
import * as Conversation from 'effect-harness/Conversation'
import type * as Harness from 'effect-harness/Harness'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type { HarnessError } from 'effect-harness/HarnessError'
import * as Model from 'effect-harness/Model'
import { ProviderAffinity } from 'effect-harness/ProviderAffinity'
import * as Session from 'effect-harness/Session'
import type * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Usage from 'effect-harness/Usage'

declare const native: typeof LanguageModel.LanguageModel.Service
declare const session: Session.Session
declare const conversation: Conversation.Conversation
declare const harness: Harness.HarnessService

test('request-scoped affinity requirements are provided by the runtime', () => {
  const model = Model.make({
    definition: {
      ref: { provider: 'custom', modelId: 'test' },
      capabilities: { tools: false, images: false, reasoning: false, structuredOutput: false },
      pricing: { currency: 'USD', source: 'price-v1', inputToken: 1, outputToken: 2 },
    },
    options: Schema.Struct({}),
    languageModel: native,
    configure: () =>
      Effect.gen(function* () {
        yield* ProviderAffinity
        return Context.empty()
      }),
  })
  expect(HarnessRuntime.make({ models: [model] })).type.toBe<
    Effect.Effect<HarnessRuntime.HarnessRuntimeService, HarnessError, Scope.Scope | Storage.Storage>
  >()
  expect(RootAffinity.ProviderAffinity).type.toBe<typeof ProviderAffinity>()
})
test('public accounting preserves local errors and exposes optional counters', () => {
  expect(Session.usage(session)).type.toBe<Effect.Effect<Usage.Summary, Session.Failure>>()
  expect(Conversation.usage(conversation)).type.toBe<Effect.Effect<Usage.Summary, HarnessError>>()
  expect(harness.usage()).type.toBe<Effect.Effect<Usage.Summary, HarnessError>>()
  expect(RootUsage.aggregate).type.toBe<typeof Usage.aggregate>()
  expect(Usage.aggregate([]).models[0]!.tokens.input).type.toBe<number | undefined>()
})
test('tool spend is opt-in, distinct and schema typed', () => {
  const toolkit = Toolkit.make(
    Tool.makeResult('paid', { success: Schema.Struct({ done: Schema.Boolean }) }),
  )
  expect(
    toolkit.toLayer({
      paid: () =>
        Effect.succeed({
          content: [],
          structuredOutput: { done: true },
          spend: { amount: 1, currency: 'EUR', source: 'invoice' },
        }),
    }),
  ).type.toBe<import('effect/Layer').Layer<Toolkit.Handler<'paid'>>>()
})
