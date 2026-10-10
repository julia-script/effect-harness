import { assert, describe, it } from '@effect/vitest'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import type * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as HttpClient from 'effect/http/HttpClient'
import { ProviderAffinity } from 'effect-harness/ProviderAffinity'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as OpenAiResult from 'effect-harness/provider-openai/ToolResult'

describe('native OpenAI conversation affinity mapping', () => {
  for (const supplied of ['none', 'construction', 'request'] as const) {
    it.effect(`maps affinity with ${supplied} caller defaults`, () =>
      Effect.gen(function* () {
        let payload: typeof OpenAiSchema.CreateResponse.Encoded | undefined
        const native = OpenAiClient.OpenAiClient.of({
          client: HttpClient.make(() => Effect.die('Unexpected HTTP work')),
          createResponse: (input) => {
            payload = input
            return Effect.die('captured request')
          },
          createResponseStream: () => Effect.die('Unexpected stream work'),
          createEmbedding: () => Effect.die('Unexpected embedding work'),
        })
        const model = yield* OpenAiLanguageModel.make({
          model: 'fixture',
          ...(supplied === 'construction'
            ? { config: { prompt_cache_key: 'explicit-construction', temperature: 0.4 } }
            : {}),
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, native))
        const generation = model
          .generateText({ prompt: 'hello' })
          .pipe(Effect.provideService(ProviderAffinity, { id: 'durable-identity' }))
        const result = yield* Effect.exit(
          supplied === 'request'
            ? generation.pipe(
                Effect.provideService(OpenAiLanguageModel.Config, {
                  prompt_cache_key: 'explicit-request',
                  temperature: 0.6,
                }),
              )
            : generation,
        )
        assert.isTrue(Exit.isFailure(result))
        assert.strictEqual(
          payload?.prompt_cache_key,
          supplied === 'none' ? 'durable-identity' : `explicit-${supplied}`,
        )
        if (supplied === 'construction') assert.strictEqual(payload?.temperature, 0.4)
        if (supplied === 'request') assert.strictEqual(payload?.temperature, 0.6)
      }),
    )
  }
  it.effect('unscoped calls stay unchanged and explicit empty keys are preserved', () =>
    Effect.gen(function* () {
      const captured: Array<typeof OpenAiSchema.CreateResponse.Encoded> = []
      const native = OpenAiClient.OpenAiClient.of({
        client: HttpClient.make(() => Effect.die('Unexpected HTTP work')),
        createResponse: (payload) => {
          captured.push(payload)
          return Effect.die('captured')
        },
        createResponseStream: () => Effect.die('Unexpected stream work'),
        createEmbedding: () => Effect.die('Unexpected embedding work'),
      })
      const wrapped = OpenAiResult.client(native)
      yield* Effect.exit(wrapped.createResponse({ model: 'fixture', input: [] }))
      yield* Effect.exit(
        wrapped
          .createResponse({ model: 'fixture', input: [], prompt_cache_key: '' })
          .pipe(Effect.provideService(ProviderAffinity, { id: 'durable' })),
      )
      assert.isUndefined(captured[0]?.prompt_cache_key)
      assert.strictEqual(captured[1]?.prompt_cache_key, '')
    }),
  )
})
