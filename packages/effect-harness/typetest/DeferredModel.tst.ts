import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import type * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as Model from 'effect-harness/Model'
import * as Identity from 'effect-harness/Identity'
import * as Usage from 'effect-harness/Usage'
import * as DeferredResponse from 'effect-harness/provider-openai/DeferredResponse'

class Decoder extends Context.Service<Decoder, number>()('DeferredModel/types/Decoder') {}
class Encoder extends Context.Service<Encoder, number>()('DeferredModel/types/Encoder') {}
class Owner extends Context.Service<Owner, number>()('DeferredModel/types/Owner') {}
const codec = Schema.String.pipe(
  Schema.decodeTo(Schema.Finite, {
    decode: SchemaGetter.transformEffect((value) => Decoder.pipe(Effect.as(Number(value)))),
    encode: SchemaGetter.transformEffect((value) => Encoder.pipe(Effect.as(String(value)))),
  }),
)
const request = {
  requestId: Identity.RequestId.make('prepared'),
  attempt: Usage.AttemptId.make(0),
  prompt: Prompt.make('hello'),
  toolkit: Toolkit.empty,
}
declare const adapter: Model.Deferred
declare const native: typeof LanguageModel.LanguageModel.Service

test('construction captures callback and both codec service requirements', () => {
  const construction = Model.makeDeferred<typeof codec, Owner>({
    handle: codec,
    start: () => Owner.pipe(Effect.as({ _tag: 'Deferred' as const, handle: 1, nextPollAt: 1000 })),
    poll: () => Effect.succeed({ _tag: 'Pending', nextPollAt: 2000 }),
    cancel: () => Effect.void,
  })
  expect(construction).type.toBe<Effect.Effect<Model.Deferred, never, Owner | Decoder | Encoder>>()
  expect(DeferredResponse.make({ model: 'gpt-4.1' })).type.toBe<
    Effect.Effect<Model.Deferred, Model.ModelError, OpenAiClient.OpenAiClient>
  >()
  expect(adapter.start(request)).type.toBe<
    Effect.Effect<Model.Complete | Model.DeferredHandle<Schema.Json>, Model.DeferredError>
  >()
  expect(adapter.poll({ id: 'a' }, 1000, request)).type.toBe<
    Effect.Effect<Model.Complete | Model.Pending, Model.DeferredError>
  >()
  expect(adapter.cancel({ id: 'a' })).type.toBe<Effect.Effect<void, Model.DeferredError>>()
  expect(adapter.start).type.not.toBeCallableWith({ ...request, attempt: 0 })
  expect(adapter.start).type.not.toBeCallableWith({ ...request, requestId: 'raw' })
})

test('tool parameter encoder stays visible without requiring handlers or result decoding', () => {
  const tool = Tool.make('convert', { parameters: Schema.Struct({ value: codec }) })
  const toolkit = Toolkit.make(tool)
  const prepared = { ...request, toolkit }
  expect(adapter.start(prepared)).type.toBe<
    Effect.Effect<Model.Complete | Model.DeferredHandle<Schema.Json>, Model.DeferredError, Encoder>
  >()
  expect(adapter.poll({ id: 'a' }, 1000, prepared)).type.toBe<
    Effect.Effect<Model.Complete | Model.Pending, Model.DeferredError, Encoder>
  >()
  const model = Model.make({
    definition: {
      ref: { provider: 'custom', modelId: 'model' },
      capabilities: { tools: true, reasoning: false, images: false, structuredOutput: false },
    },
    options: codec,
    languageModel: native,
    deferred: adapter,
    configure: () => Effect.succeed(Context.empty()),
  })
  expect<Model.Requirements<typeof model>>().type.toBe<Decoder | Encoder>()
  expect(model.languageModel).type.toBe<typeof LanguageModel.LanguageModel.Service>()
})
