import { assertSome } from '@effect/vitest/utils'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import { vi } from 'vitest'
vi.mock('@effect/ai-anthropic/AnthropicLanguageModel', { spy: true })
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Context from 'effect/Context'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Prompt from 'effect-harness/provider-anthropic/Prompt'
import { assert, describe, it } from '@effect/vitest'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Effect from 'effect/Effect'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Stream from 'effect/Stream'
import * as Anthropic from 'effect-harness/provider-anthropic/Anthropic'

const message = {
  id: 'msg_test',
  type: 'message',
  role: 'assistant',
  model: 'claude-sonnet-4-5',
  content: [{ type: 'text', text: 'Hello Julia' }],
  stop_reason: 'end_turn',
  stop_sequence: null,
  usage: {
    input_tokens: 11,
    output_tokens: 3,
    cache_creation: null,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    service_tier: 'standard',
  },
}

const model = (client: HttpClient.HttpClient) =>
  Anthropic.layer({
    model: 'claude-sonnet-4-5',
    apiKey: Redacted.make('test-key'),
    config: { max_tokens: 1234 },
  }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client)))

describe('Anthropic', () => {
  describe('Anthropic native Effect AI provider', () => {
    it.effect('uses API-key transport and keeps model parts and usage', () =>
      Effect.gen(function* () {
        const client = HttpClient.make((request, url) => {
          assert.strictEqual(url.origin, 'https://api.anthropic.com')
          assert.strictEqual(request.headers['x-api-key'], 'test-key')
          assert.strictEqual(request.headers['anthropic-version'], '2023-06-01')
          assert.strictEqual(request.headers['authorization'], undefined)
          assert.strictEqual(request.headers['user-agent'], undefined)
          assert.strictEqual(request.method, 'POST')
          return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(message)))
        })
        const response = yield* LanguageModel.generateText({
          prompt: 'Hello',
          disableToolCallResolution: true,
        }).pipe(Effect.provide(model(client)))
        assert.strictEqual(response.text, 'Hello Julia')
        assert.strictEqual(response.usage.inputTokens.total, 11)
        assert.strictEqual(response.usage.outputTokens.total, 3)
        assert.strictEqual(response.finishReason, 'stop')
      }),
    )

    it.effect('streams through Effect AI and preserves terminal usage', () =>
      Effect.gen(function* () {
        const events = [
          {
            type: 'message_start',
            message: {
              ...message,
              content: [],
              stop_reason: null,
              usage: { ...message.usage, output_tokens: 0 },
            },
          },
          { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
          {
            type: 'content_block_delta',
            index: 0,
            delta: { type: 'text_delta', text: 'Hello Julia' },
          },
          { type: 'content_block_stop', index: 0 },
          {
            type: 'message_delta',
            delta: { stop_reason: 'end_turn', stop_sequence: null },
            usage: {
              output_tokens: 3,
              input_tokens: 11,
              cache_creation_input_tokens: 0,
              cache_read_input_tokens: 0,
            },
          },
          { type: 'message_stop' },
        ]
        const client = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(
                events
                  .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
                  .join(''),
                { headers: { 'content-type': 'text/event-stream' } },
              ),
            ),
          ),
        )
        const parts = yield* LanguageModel.streamText({
          prompt: 'Hello',
          disableToolCallResolution: true,
        }).pipe(Stream.runCollect, Effect.provide(model(client)))
        const text = parts
          .filter((part) => part.type === 'text-delta')
          .map((part) => part.delta)
          .join('')
        const finish = parts.find((part) => part.type === 'finish')
        assert.strictEqual(text, 'Hello Julia')
        assert.strictEqual(finish?.usage.outputTokens.total, 3)
      }),
    )

    it.effect('maps authentication failure into the native typed AI error', () =>
      Effect.gen(function* () {
        const client = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json(
                { type: 'error', error: { type: 'authentication_error', message: 'Invalid key' } },
                { status: 401 },
              ),
            ),
          ),
        )
        const result = yield* LanguageModel.generateText({ prompt: 'Hello' }).pipe(
          Effect.provide(model(client)),
          Effect.result,
        )
        // The native reason embeds live HttpClient request/response handles and transport
        // diagnostics, whose function identities are not deterministic fixtures. Compare
        // every stable authentication field here, retaining the actual native reason.
        const error = Result.getFailure(result)
        assertSome(
          Option.map(error, (self) => ({
            tag: self._tag,
            reason: self.reason._tag,
            retryable: self.isRetryable,
          })),
          { tag: 'AiError', reason: 'AuthenticationError', retryable: false },
        )
        if (Result.isFailure(result) && result.failure.reason._tag === 'AuthenticationError') {
          assert.deepStrictEqual(
            {
              kind: result.failure.reason.kind,
              status: result.failure.reason.http?.response?.status,
            },
            { kind: 'InvalidKey', status: 401 },
          )
        }
      }),
    )

    it.effect('provides the native client for account model discovery', () =>
      Effect.gen(function* () {
        const client = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({
                data: [
                  {
                    id: 'claude-sonnet-4-5',
                    type: 'model',
                    display_name: 'Sonnet',
                    created_at: '2025-09-29T00:00:00Z',
                  },
                ],
                has_more: false,
                first_id: 'claude-sonnet-4-5',
                last_id: 'claude-sonnet-4-5',
              }),
            ),
          ),
        )
        const available = yield* Effect.gen(function* () {
          const anthropic = yield* AnthropicClient.AnthropicClient
          return yield* anthropic.client.modelsList({})
        }).pipe(Effect.provide(model(client)))
        assert.strictEqual(available.data[0]?.id, 'claude-sonnet-4-5')
      }),
    )
  })

  describe('Anthropic configured provider capability', () => {
    it.effect(
      'Prompt configuration reexports the exact native client with one model construction',
      () =>
        Effect.gen(function* () {
          const http = HttpClient.make((request) =>
            Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(message))),
          )
          const client = yield* AnthropicClient.make({ apiKey: Redacted.make('fake-key') }).pipe(
            Effect.provideService(HttpClient.HttpClient, http),
          )
          const spy = vi.mocked(AnthropicLanguageModel.make)
          spy.mockClear()
          yield* Effect.gen(function* () {
            const context = yield* Layer.build(
              Prompt.layerConfig({ model: Config.String('MODEL') }).pipe(
                Layer.provide(Layer.succeed(AnthropicClient.AnthropicClient, client)),
              ),
            ).pipe(
              Effect.provideService(
                ConfigProvider.ConfigProvider,
                ConfigProvider.fromUnknown({ MODEL: 'configured-model' }),
              ),
            )
            assert.strictEqual(Context.get(context, AnthropicClient.AnthropicClient), client)
            assert.isDefined(Context.get(context, LanguageModel.LanguageModel))
            assert.strictEqual(spy.mock.calls.length, 1)
          })
        }),
    )
    it.effect('default-key configuration retains ANTHROPIC_API_KEY and all model options', () =>
      Effect.gen(function* () {
        let requests = 0
        const http = HttpClient.make((request) => {
          requests++
          assert.strictEqual(request.headers['x-api-key'], 'configured-key')
          assert.strictEqual(request.url, 'https://config.example/v1/messages?beta=true')
          if (request.body._tag !== 'Uint8Array') return Effect.die('Expected final JSON body')
          const payload = JSON.parse(new TextDecoder().decode(request.body.body))
          assert.strictEqual(payload.model, 'configured-model')
          assert.strictEqual(payload.max_tokens, 2000)
          return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(message)))
        })
        const layer = Anthropic.layerDefaultConfig({
          model: Config.String('MODEL'),
          apiUrl: Config.String('API_URL'),
          config: { max_tokens: Config.Int('MAX_TOKENS') },
        }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))
        return yield* Effect.gen(function* () {
          yield* LanguageModel.generateText({ prompt: 'Hello' }).pipe(
            Anthropic.withConfigOverride({ max_tokens: 2000 }),
          )
          assert.isDefined(yield* AnthropicClient.AnthropicClient)
          assert.strictEqual(requests, 1)
        }).pipe(
          Effect.provide(layer),
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromUnknown({
              MODEL: 'configured-model',
              API_URL: 'https://config.example',
              MAX_TOKENS: 1000,
              ANTHROPIC_API_KEY: 'configured-key',
            }),
          ),
        )
      }),
    )
  })
})
