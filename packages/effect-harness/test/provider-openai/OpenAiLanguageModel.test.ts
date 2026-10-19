import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as HttpClient from 'effect/http/HttpClient'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as ProviderOpenai from 'effect-harness/provider-openai'

describe('OpenAiLanguageModel', () => {
  it.effect('construction exports the captured client without sending requests', () =>
    Effect.gen(function* () {
      let requests = 0
      const client = OpenAiClient.OpenAiClient.of({
        client: HttpClient.make(() => {
          requests++
          return Effect.die('Unexpected construction HTTP work')
        }),
        createResponse: () => Effect.die('Unexpected construction response work'),
        createResponseStream: () => Effect.die('Unexpected construction stream work'),
        createEmbedding: () => Effect.die('Unexpected construction embedding work'),
      })
      const context = yield* Layer.build(
        OpenAiLanguageModel.layer({ model: 'declared' }).pipe(
          Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, client)),
        ),
      )
      assert.strictEqual(Context.get(context, OpenAiClient.OpenAiClient), client)
      assert.isDefined(Context.get(context, LanguageModel.LanguageModel).generateText)
      assert.strictEqual(ProviderOpenai.OpenAiLanguageModel.layer, OpenAiLanguageModel.layer)
      assert.strictEqual(ProviderOpenai.OpenAiClient.OpenAiClient, OpenAiClient.OpenAiClient)
      assert.strictEqual(requests, 0)
    }),
  )
})
