import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Http from '../src/internal/SubscriptionHttp.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Codex from '../src/provider-openai-codex/CodexLanguageModel.js'
import * as Claude from '../src/provider-anthropic/OAuthLanguageModel.js'
import { binding, credentials, sse } from './fixtures/SubscriptionProtocol.js'

for (const provider of ['openai-codex', 'anthropic'] as const) {
  for (const [status, code, message, expected] of [
    [400, 'context_length_exceeded', 'SECRET', provider === 'openai-codex'],
    [
      400,
      'invalid_request_error',
      'prompt is too long: 12345 tokens > 12000 maximum',
      provider === 'anthropic',
    ],
    [400, 'invalid_request_error', 'prompt is too long', provider === 'anthropic'],
    [400, 'invalid_request_error', 'SECRET invalid signature', false],
    [400, 'invalid_request_error', 'max_tokens too large', false],
    [400, 'invalid_request_error', 'prompt is too long: SECRET', false],
    [413, 'context_length_exceeded', 'prompt is too long', false],
    [401, 'context_length_exceeded', 'prompt is too long', false],
    [429, 'insufficient_quota', 'SECRET', false],
    [429, 'rate_limit_error', 'SECRET', false],
  ] as const) {
    it.effect(`${provider}: HTTP ${status} ${code} ${message} overflow=${expected}`, () =>
      Effect.gen(function* () {
        const store = yield* credentials(provider)
        const http = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify({ error: { code, type: code, message } }), { status }),
            ),
          ),
        )
        const native = yield* (
          provider === 'anthropic'
            ? Claude.make({ model: 'claude-sonnet-4-6', binding: { ...binding, provider } })
            : Codex.make({ model: 'gpt-5.4', binding })
        ).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const error = yield* Effect.flip(native.generateText({ prompt: 'hello' }))
        assert.strictEqual(Http.isOverflow(error), expected)
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(AiError.AiError))(error)
        assert.notInclude(JSON.stringify(encoded), 'SECRET')
        assert.notInclude(JSON.stringify(encoded), '12345')
        assert.strictEqual(
          Http.isOverflow(yield* Schema.decodeEffect(Schema.toCodecJson(AiError.AiError))(encoded)),
          expected,
        )
      }),
    )
  }
  it.effect(
    `${provider}: SSE input overflow is classified before provider messages are discarded`,
    () =>
      Effect.gen(function* () {
        const store = yield* credentials(provider)
        const event =
          provider === 'anthropic'
            ? {
                type: 'error',
                error: {
                  type: 'invalid_request_error',
                  message: 'prompt is too long: 12345 tokens > 12000 maximum',
                },
              }
            : {
                type: 'response.failed',
                response: { error: { code: 'context_length_exceeded', message: 'SECRET' } },
              }
        const http = HttpClient.make((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response(sse([event])))),
        )
        const native = yield* (
          provider === 'anthropic'
            ? Claude.make({ model: 'claude-sonnet-4-6', binding: { ...binding, provider } })
            : Codex.make({ model: 'gpt-5.4', binding })
        ).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const error = yield* Effect.flip(
          native.streamText({ prompt: 'hello' }).pipe(Stream.runCollect),
        )
        assert.isTrue(Http.isOverflow(error))
        assert.notInclude(JSON.stringify(error), 'SECRET')
      }),
  )
}
import * as Stream from 'effect/Stream'
