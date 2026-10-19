import { assert, describe, it } from '@effect/vitest'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as Sse from 'effect/encoding/Sse'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as AnthropicResult from 'effect-harness/provider-anthropic/ToolResult'
import * as OpenAiResult from 'effect-harness/provider-openai/ToolResult'

describe('provider client capability preservation', () => {
  for (const inherited of [false, true]) {
    it.effect(
      `preserves ${inherited ? 'inherited' : 'non-enumerable'} OpenAI endpoints and receiver`,
      () =>
        Effect.gen(function* () {
          let calls = 0
          const response = { data: [], model: 'embedding-fixture' }
          const prototype = OpenAiClient.OpenAiClient.of({
            client: HttpClient.make(() => Effect.die('Unexpected HTTP work')),
            createResponse: () => Effect.die('Unexpected response work'),
            createResponseStream: () => Effect.die('Unexpected stream work'),
            createEmbedding() {
              assert.strictEqual(this, captured)
              calls++
              return Effect.succeed(response)
            },
          })
          const captured: OpenAiClient.Service = inherited
            ? Object.create(prototype)
            : Object.defineProperties({}, Object.getOwnPropertyDescriptors(prototype))
          if (!inherited) {
            Object.defineProperty(captured, 'client', { enumerable: false })
            Object.defineProperty(captured, 'createEmbedding', { enumerable: false })
          }
          const wrapped = OpenAiResult.client(captured)
          assert.strictEqual(wrapped.client, captured.client)
          assert.strictEqual(
            yield* wrapped.createEmbedding({ model: 'test', input: 'test' }),
            response,
          )
          assert.strictEqual(calls, 1)
        }),
    )

    it.effect(
      `preserves ${inherited ? 'inherited' : 'non-enumerable'} Anthropic endpoints and receiver`,
      () =>
        Effect.gen(function* () {
          const native = yield* AnthropicClient.make({}).pipe(
            Effect.provideService(
              HttpClient.HttpClient,
              HttpClient.make(() => Effect.die('Unexpected HTTP work')),
            ),
          )
          let calls = 0
          const prototype = AnthropicClient.AnthropicClient.of({
            ...native,
            streamRequest() {
              assert.strictEqual(this, captured)
              calls++
              return () => Stream.empty
            },
          })
          const captured: AnthropicClient.Service = inherited
            ? Object.create(prototype)
            : Object.defineProperties({}, Object.getOwnPropertyDescriptors(prototype))
          if (!inherited) {
            Object.defineProperty(captured, 'client', { enumerable: false })
            Object.defineProperty(captured, 'streamRequest', { enumerable: false })
          }
          const wrapped = AnthropicResult.client(captured)
          assert.strictEqual(wrapped.client, captured.client)
          const result = yield* Stream.runCollect(
            wrapped.streamRequest(Sse.Event)(HttpClientRequest.get('https://fixture.invalid')),
          )
          assert.deepStrictEqual(result, [])
          assert.strictEqual(calls, 1)
        }),
    )
  }
})
