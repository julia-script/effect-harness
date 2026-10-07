import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as AiError from 'effect/ai/AiError'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Anthropic from '@effect-harness/provider-anthropic/Anthropic'
import * as Catalog from '@effect-harness/provider-anthropic/Catalog'
import * as Provider from '@effect-harness/provider-anthropic/Prompt'
import * as OAuth from '@effect-harness/provider-anthropic/OAuth'
import * as Account from '@effect-harness/provider-anthropic/Account'
import type * as Layer from 'effect/Layer'
import type * as HttpClient from 'effect/http/HttpClient'
import type { ModelError } from '@effect-harness/harness/Error'
class Audit extends Context.Service<
  Audit,
  { readonly record: (value: number) => Effect.Effect<void> }
>()('typetest/Audit') {}
const convert = Tool.make('convert', {
  parameters: Schema.Struct({ value: Schema.FiniteFromString }),
  success: Schema.String,
  failure: Schema.Literal('failure'),
  dependencies: [Audit],
})
const toolkit = Toolkit.make(convert)
declare const withHandlers: Toolkit.WithHandler<Toolkit.Tools<typeof toolkit>>
declare const model: Effect.Success<ReturnType<typeof Provider.make>>

test('public native model retains toolkit mode, handler errors and invocation services', () => {
  const handled = model.generateText({ prompt: 'question', toolkit: withHandlers })
  expect(handled).type.toBe<
    Effect.Effect<
      LanguageModel.GenerateTextResponse<Toolkit.Tools<typeof toolkit>, 'opaque'>,
      AiError.AiError | Tool.HandlerError<typeof convert>,
      Audit
    >
  >()
  expect(toolkit).type.toBe<Toolkit.Toolkit<Toolkit.Tools<typeof toolkit>>>()
  const intents = model.generateText({
    prompt: 'question',
    toolkit,
    disableToolCallResolution: true,
  })
  expect(intents).type.toBe<
    Effect.Effect<
      LanguageModel.GenerateTextResponse<Toolkit.Tools<typeof toolkit>, 'encoded'>,
      AiError.AiError
    >
  >()
  expect(intents).type.not.toBeAssignableTo<
    Effect.Effect<
      LanguageModel.GenerateTextResponse<Toolkit.Tools<typeof toolkit>, 'opaque'>,
      AiError.AiError
    >
  >()
  expect(model.generateText).type.not.toBeCallableWith({ prompt: 'question', toolkit: 42 })
})

test('owned construction pins native client outputs and account inputs', () => {
  expect(Provider.make({ model: 'declared' })).type.toBe<
    Effect.Effect<
      typeof LanguageModel.LanguageModel.Service,
      never,
      AnthropicClient.AnthropicClient
    >
  >()
  expect(Provider.layer({ model: 'declared' })).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
      never,
      AnthropicClient.AnthropicClient
    >
  >()
  expect(Account.layer({ model: 'declared', account: 'key' })).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
      AiError.AiError,
      HttpClient.HttpClient | OAuth.OAuth
    >
  >()
  expect(
    Catalog.descriptor({ modelId: 'declared', contextWindow: 200000, maxOutputTokens: 32000 }),
  ).type.toBe<Effect.Effect<Catalog.Descriptor, ModelError, AnthropicClient.AnthropicClient>>()
  expect(Catalog.descriptor).type.not.toBeCallableWith(
    { modelId: 'declared', contextWindow: 1, maxOutputTokens: 1 },
    42,
  )
})

test('both native config override forms retain caller services and foreign errors', () => {
  const request = Effect.flatMap(Audit, () => Effect.fail('foreign' as const))
  expect(Anthropic.withConfigOverride(request, { max_tokens: 1 })).type.toBe<
    Effect.Effect<never, 'foreign', Audit>
  >()
  expect(request.pipe(Anthropic.withConfigOverride({ max_tokens: 1 }))).type.toBe<
    Effect.Effect<never, 'foreign', Audit>
  >()
  expect<[Effect.Success<typeof request>]>().type.toBe<[never]>()
  expect(Anthropic.withConfigOverride).type.toBe<typeof AnthropicLanguageModel.withConfigOverride>()
  expect(Anthropic.withConfigOverride).type.not.toBeCallableWith(request, { max_tokens: 'wrong' })
})
