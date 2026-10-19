import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as AiError from 'effect/ai/AiError'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import type * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import type * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'

import * as Prompt from 'effect-harness/provider-anthropic/Prompt'

import type * as Layer from 'effect/Layer'
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
declare const model: Effect.Success<ReturnType<typeof Prompt.make>>

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

test('owned construction pins native client outputs ', () => {
  expect(Prompt.make({ model: 'declared' })).type.toBe<
    Effect.Effect<
      typeof LanguageModel.LanguageModel.Service,
      never,
      AnthropicClient.AnthropicClient
    >
  >()
  expect(Prompt.layer({ model: 'declared' })).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel | AnthropicClient.AnthropicClient,
      never,
      AnthropicClient.AnthropicClient
    >
  >()
})

test('both native config override forms retain caller services and foreign errors', () => {
  const request = Effect.flatMap(Audit, () => Effect.fail('foreign' as const))
  expect(HarnessAnthropicLanguageModel.withConfigOverride(request, { max_tokens: 1 })).type.toBe<
    Effect.Effect<never, 'foreign', Audit>
  >()
  expect(
    request.pipe(HarnessAnthropicLanguageModel.withConfigOverride({ max_tokens: 1 })),
  ).type.toBe<Effect.Effect<never, 'foreign', Audit>>()
  expect<[Effect.Success<typeof request>]>().type.toBe<[never]>()
  expect(HarnessAnthropicLanguageModel.withConfigOverride).type.toBe<
    typeof AnthropicLanguageModel.withConfigOverride
  >()
  expect(HarnessAnthropicLanguageModel.withConfigOverride).type.not.toBeCallableWith(request, {
    max_tokens: 'wrong',
  })
})

import * as ProviderAnthropic from 'effect-harness/provider-anthropic'

test('root namespaces expose native client ', () => {
  expect(ProviderAnthropic.AnthropicClient.AnthropicClient).type.toBe<
    typeof AnthropicClient.AnthropicClient
  >()
})

test('encoded tool parameters retain the exact indexed shape through concrete and generic handlers', () => {
  const concrete = model.generateText({
    prompt: 'question',
    toolkit,
    disableToolCallResolution: true,
  })
  type Expected = { readonly value: string } | undefined
  expect<Effect.Success<typeof concrete>['toolCalls'][number]['params']>().type.toBe<{
    readonly value: string
  }>()
  const indexed = Effect.map(concrete, (response) => response.toolCalls[0]?.params)
  expect(indexed).type.toBe<Effect.Effect<Expected, AiError.AiError>>()
  const generic = <Tools extends Record<string, Tool.Any>>(input: Toolkit.WithHandler<Tools>) =>
    model.generateText({ prompt: 'question', toolkit: input, disableToolCallResolution: true })
  const instantiated = generic(withHandlers)
  expect(instantiated).type.toBe<
    Effect.Effect<
      LanguageModel.GenerateTextResponse<Toolkit.Tools<typeof toolkit>, 'encoded'>,
      AiError.AiError
    >
  >()
  expect(Effect.map(instantiated, (response) => response.toolCalls[0]?.params)).type.toBe<
    Effect.Effect<Expected, AiError.AiError>
  >()
  expect<Effect.Success<typeof concrete>['toolCalls'][number]['params']>().type.not.toBe<{
    readonly value: number
  }>()
})

import * as HarnessAnthropicLanguageModel from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
