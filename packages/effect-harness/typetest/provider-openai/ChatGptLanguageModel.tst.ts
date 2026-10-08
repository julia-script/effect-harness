import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Stream from 'effect/Stream'
import * as Schema from 'effect/Schema'
import type * as AiError from 'effect/ai/AiError'
import type * as Response from 'effect/ai/Response'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import type * as ChatGptLanguageModel from 'effect-harness/provider-openai/ChatGptLanguageModel'

class HandlerAudit extends Context.Service<HandlerAudit, { readonly record: string }>()(
  'effect-harness/typetest/provider-openai/ChatGptLanguageModel/HandlerAudit',
) {}
class DecodeAudit extends Context.Service<DecodeAudit, { readonly decoded: string }>()(
  'effect-harness/typetest/provider-openai/ChatGptLanguageModel/DecodeAudit',
) {}
const convert = Tool.make('convert', {
  parameters: Schema.Struct({ value: Schema.FiniteFromString }),
  success: Schema.String,
  failure: Schema.Literal('handler-failed'),
  dependencies: [HandlerAudit],
})
const toolkit = Toolkit.make(convert)
declare const handled: Toolkit.WithHandler<Toolkit.Tools<typeof toolkit>>
declare const unionToolkit: typeof handled | Effect.Effect<typeof handled, never, DecodeAudit>
declare const model: Effect.Success<ReturnType<typeof ChatGptLanguageModel.make>>
declare const schema: Schema.Codec<
  { readonly value: number },
  { readonly value: string },
  DecodeAudit
>

test('account decorators retain the native higher-rank interface and every invocation channel', () => {
  expect<typeof model>().type.toBe<typeof LanguageModel.LanguageModel.Service>()
  expect(model.generateText({ prompt: 'question' })).type.toBe<
    Effect.Effect<LanguageModel.GenerateTextResponse<{}>, AiError.AiError>
  >()
  expect(model.generateText({ prompt: 'question', toolkit: handled })).type.toBe<
    Effect.Effect<
      LanguageModel.GenerateTextResponse<Toolkit.Tools<typeof toolkit>, 'opaque'>,
      AiError.AiError | Tool.HandlerError<typeof convert>,
      HandlerAudit
    >
  >()
  expect(
    model.generateText({ prompt: 'question', toolkit, disableToolCallResolution: true }),
  ).type.toBe<
    Effect.Effect<
      LanguageModel.GenerateTextResponse<Toolkit.Tools<typeof toolkit>, 'encoded'>,
      AiError.AiError
    >
  >()
  expect(model.streamText({ prompt: 'question', toolkit: handled })).type.toBe<
    Stream.Stream<
      Response.StreamPart<Toolkit.Tools<typeof toolkit>, 'opaque'>,
      AiError.AiError | Tool.HandlerError<typeof convert>,
      HandlerAudit
    >
  >()
  const object = model.generateObject({ prompt: 'question', schema, toolkit: handled })
  expect<Effect.Services<typeof object>>().type.toBe<HandlerAudit | DecodeAudit>()
  expect<Effect.Error<typeof object>>().type.toBe<
    AiError.AiError | Tool.HandlerError<typeof convert>
  >()
  const union = model.generateText({ prompt: 'question', toolkit: unionToolkit })
  expect<Effect.Services<typeof union>>().type.toBe<HandlerAudit | DecodeAudit>()
  expect<Effect.Error<typeof union>>().type.toBe<
    AiError.AiError | Tool.HandlerError<typeof convert>
  >()
  expect(model.generateText).type.not.toBeCallableWith({
    prompt: 'question',
    toolkit: Effect.fail('toolkit-failed'),
  })
})
