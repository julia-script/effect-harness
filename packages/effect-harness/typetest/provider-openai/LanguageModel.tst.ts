import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as AiError from 'effect/ai/AiError'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'

import * as Catalog from 'effect-harness/provider-openai/Catalog'
import * as ToolResult from 'effect-harness/provider-openai/ToolResult'
import type * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import type * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Layer from 'effect/Layer'
import type * as HttpClient from 'effect/http/HttpClient'
import type { ModelError } from 'effect-harness/ModelError'
import * as Redacted from 'effect/Redacted'
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
declare const model: Effect.Success<ReturnType<typeof OpenAiLanguageModel.make>>

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

test('owned public construction exposes the exact captured native client', () => {
  expect(OpenAiLanguageModel.make({ model: 'declared' })).type.toBe<
    Effect.Effect<typeof LanguageModel.LanguageModel.Service, never, OpenAiClient.OpenAiClient>
  >()
  expect(OpenAiLanguageModel.layer({ model: 'declared' })).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
      never,
      OpenAiClient.OpenAiClient
    >
  >()
  expect(
    OpenAiLanguageModel.layerApiKey({ model: 'declared', apiKey: Redacted.make('secret') }),
  ).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel | OpenAiClient.OpenAiClient,
      never,
      HttpClient.HttpClient
    >
  >()
  expect(
    Catalog.descriptor({ modelId: 'declared', contextWindow: 200000, maxOutputTokens: 32000 }),
  ).type.toBe<Effect.Effect<Catalog.Descriptor, ModelError, OpenAiClient.OpenAiClient>>()
  expect(ToolResult.content).type.not.toBeCallableWith([{ type: 'text', text: 1 }])
  expect(ToolResult.content(parts)).type.toBe<
    Effect.Effect<Array<typeof OpenAiSchema.InputContent.Encoded>, AiError.AiError>
  >()
})
declare const parts: ReadonlyArray<Prompt.UserMessagePart>

test('curried provider combinators retain channels and reject ambiguous empty options', () => {
  const entry = { modelId: 'declared', contextWindow: 200000, maxOutputTokens: 32000 }
  expect(Catalog.descriptor()(entry)).type.toBe<
    Effect.Effect<Catalog.Descriptor, ModelError, OpenAiClient.OpenAiClient>
  >()
  expect(Catalog.descriptor({ provider: 'custom' })(entry)).type.toBe<
    Effect.Effect<Catalog.Descriptor, ModelError, OpenAiClient.OpenAiClient>
  >()
  expect(Catalog.descriptor).type.not.toBeCallableWith({})
  expect(ToolResult.content({ prefixes: [] })(parts)).type.toBe<
    Effect.Effect<Array<typeof OpenAiSchema.InputContent.Encoded>, AiError.AiError>
  >()
  expect(ToolResult.content).type.not.toBeCallableWith(['file_'])
})

import * as ProviderOpenai from 'effect-harness/provider-openai'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'

test('root namespaces expose canonical constructors and the exact native client facade', () => {
  expect(ProviderOpenai.OpenAiLanguageModel.layer).type.toBe<typeof OpenAiLanguageModel.layer>()
  expect(ProviderOpenai.OpenAiClient.OpenAiClient).type.toBe<typeof OpenAiClient.OpenAiClient>()
})
