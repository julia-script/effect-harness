import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Providers from '../src/Providers.js'
import * as ProviderStore from '../src/ProviderStore.js'
import * as MemoryCredentials from '../src/provider-auth/MemoryCredentialStore.js'
import * as OpenRouter from '../src/provider-openrouter/Provider.js'
import * as Catalog from '../src/provider-openrouter/Catalog.js'
import { payload } from './fixtures/SubscriptionProtocol.js'

// Fields and units follow OpenRouter's public Models API and reasoning metadata contract.
const entry = {
  id: 'vendor/model',
  name: 'Vendor Model',
  architecture: { input_modalities: ['text', 'image'], output_modalities: ['text'] },
  supported_parameters: ['tools', 'reasoning', 'structured_outputs', 'temperature'],
  context_length: 128000,
  top_provider: { max_completion_tokens: 16000 },
  pricing: { prompt: '0.0000025', completion: '0.00001', input_cache_read: '0.00000125' },
  reasoning: {
    supported_efforts: ['high', 'medium', 'low'],
    default_effort: 'medium',
    mandatory: true,
  },
}
const reply = {
  id: 'chat-1',
  object: 'chat.completion',
  created: 0,
  model: entry.id,
  choices: [{ index: 0, message: { role: 'assistant', content: 'hello' }, finish_reason: 'stop' }],
  usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 },
}

it.effect(
  'public discovery is lazy and independent; inference uses the selected account and effort',
  () =>
    Effect.gen(function* () {
      const manager = yield* Providers.make({ definitions: { openrouter: OpenRouter.definition } })
      const instance = yield* manager.add({ provider: 'openrouter', label: 'Personal', config: {} })
      const calls: string[] = []
      const efforts: unknown[] = []
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          calls.push(request.url)
          if (request.method === 'GET') {
            assert.isUndefined(request.headers.authorization)
            const data = request.url.endsWith('/models')
              ? [
                  entry,
                  { id: 'vendor/second', name: 'Second Model' },
                  { id: 'other/a', name: 'Other' },
                ]
              : entry
            return HttpClientResponse.fromWeb(request, Response.json({ data }))
          }
          assert.equal(request.url, 'https://openrouter.ai/api/v1/chat/completions')
          assert.equal(request.headers.authorization, 'Bearer router-key')
          const body = payload(request)
          efforts.push(body.reasoning)
          assert.isUndefined(body.reasoning_effort)
          return HttpClientResponse.fromWeb(request, Response.json(reply))
        }),
      )
      assert.isEmpty(calls)
      yield* Effect.gen(function* () {
        const first = yield* manager.models.list({
          instanceId: instance.id,
          query: 'vendor',
          limit: 1,
        })
        assert.deepEqual(first, { items: [{ id: entry.id, name: entry.name }], nextCursor: '1' })
        const second = yield* manager.models.list({
          instanceId: instance.id,
          query: 'vendor',
          limit: 1,
          cursor: first.nextCursor!,
        })
        assert.deepEqual(second, { items: [{ id: 'vendor/second', name: 'Second Model' }] })
        const info = yield* manager.models.get({ instanceId: instance.id, modelId: entry.id })
        assert.equal(calls[2], 'https://openrouter.ai/api/v1/model/vendor/model')
        assert.deepEqual(info.capabilities, {
          tools: true,
          images: true,
          reasoning: true,
          structuredOutput: true,
          temperature: true,
        })
        assert.equal(info.contextWindow, 128000)
        assert.equal(info.maxOutputTokens, 16000)
        assert.deepEqual(info.pricing, {
          inputToken: 0.0000025,
          outputToken: 0.00001,
          currency: 'USD',
          source: 'openrouter/models',
        })
        assert.deepEqual(info.reasoning?.effort, {
          values: ['high', 'medium', 'low'],
          default: 'medium',
        })
        assert.equal(
          (yield* Effect.flip(
            manager.models.resolve({ instanceId: instance.id, modelId: entry.id }),
          )).reason,
          'MissingCredential',
        )
        yield* manager.update(instance.id, { secrets: { apiKey: Redacted.make('router-key') } })
        for (const effort of [undefined, 'high']) {
          const model = yield* manager.models.resolve({
            instanceId: instance.id,
            modelId: entry.id,
            ...(effort === undefined ? {} : { options: { reasoning: { effort } } }),
          })
          assert.equal((yield* model.languageModel.generateText({ prompt: 'hello' })).text, 'hello')
        }
        assert.equal(
          (yield* Effect.flip(
            manager.models.resolve({
              instanceId: instance.id,
              modelId: entry.id,
              options: { reasoning: { effort: 'none' } },
            }),
          )).reason,
          'unsupportedOptions',
        )
      }).pipe(Effect.provideService(HttpClient.HttpClient, client))
      assert.deepEqual(efforts, [{ effort: 'medium' }, { effort: 'high' }])
    }).pipe(Effect.provide(Layer.merge(ProviderStore.layerMemory, MemoryCredentials.layer))),
)

it.effect('effort discovery distinguishes null, omitted, mandatory, and disabled defaults', () =>
  Effect.gen(function* () {
    const cases = [
      { reasoning: undefined, expected: undefined },
      { reasoning: { mandatory: false }, expected: undefined },
      {
        reasoning: { supported_efforts: null },
        expected: {
          values: ['max', 'xhigh', 'high', 'medium', 'low', 'minimal', 'none'],
        },
      },
      {
        reasoning: { supported_efforts: ['high', 'none'], mandatory: true },
        expected: { values: ['high'] },
      },
      {
        reasoning: {
          supported_efforts: ['high', 'none'],
          default_effort: 'high',
          default_enabled: false,
        },
        expected: { values: ['high', 'none'] },
      },
      {
        reasoning: { supported_efforts: ['future-effort', 'none'], default_effort: 'none' },
        expected: { values: ['future-effort', 'none'], default: 'none' },
      },
    ] as const
    for (const item of cases) {
      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json({ data: { ...entry, reasoning: item.reasoning } }),
          ),
        ),
      )
      const info = yield* Catalog.get(entry.id).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      )
      assert.deepEqual(info.reasoning?.effort, item.expected)
    }
  }),
)

it.effect(
  'routing aliases retain their request ID and unknown prices are not treated as free',
  () =>
    Effect.gen(function* () {
      const client = HttpClient.make((request) => {
        assert.equal(request.url, 'https://openrouter.ai/api/v1/model/vendor/model%3Anitro')
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json({
              data: { ...entry, pricing: { prompt: '-1', completion: '-1' } },
            }),
          ),
        )
      })
      const info = yield* Catalog.get('vendor/model:nitro').pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      )
      assert.equal(info.id, 'vendor/model:nitro')
      assert.isUndefined(info.pricing)
    }),
)

it.effect('invalid metadata, missing models, and HTTP failures remain explicit', () =>
  Effect.gen(function* () {
    const cases = [
      { status: 404, body: {}, tag: 'ModelCatalogError', reason: 'modelNotFound' },
      { status: 503, body: {}, tag: 'HttpClientError' },
      { status: 200, body: { data: {} }, tag: 'ModelCatalogError', reason: 'invalidMetadata' },
      {
        status: 200,
        body: { data: { ...entry, pricing: { prompt: 'NaN', completion: '0' } } },
        tag: 'ModelCatalogError',
        reason: 'invalidMetadata',
      },
      {
        status: 200,
        body: {
          data: { ...entry, reasoning: { supported_efforts: ['low'], default_effort: 'high' } },
        },
        tag: 'ModelCatalogError',
        reason: 'invalidMetadata',
      },
    ]
    for (const item of cases) {
      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(request, Response.json(item.body, { status: item.status })),
        ),
      )
      const error = yield* Effect.flip(Catalog.get(entry.id)).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
      )
      assert.equal(error._tag, item.tag)
      if (error._tag === 'ModelCatalogError') assert.equal(error.reason, item.reason)
    }
  }),
)
