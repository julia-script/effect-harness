import { assert, describe, it } from '@effect/vitest'
import * as Model from '@effect-harness/harness/Model'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Config from 'effect/Config'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as NativeLanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Catalog from '../src/Catalog.ts'
import * as Anthropic from '../src/Anthropic.ts'

const sessionId = '019a08e0-7c00-7000-8000-000000000001'
const entry: Catalog.Entry = {
  modelId: 'declared-anthropic',
  contextWindow: 200000,
  maxOutputTokens: 32000,
  thinking: { mode: 'adaptive' },
  efforts: ['low', 'medium', 'high'],
  cache: true,
  prices: { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75, cacheWrite1h: 6 },
}
const message = {
  id: 'msg_test',
  type: 'message',
  role: 'assistant',
  model: entry.modelId,
  content: [{ type: 'text', text: 'Hello' }],
  stop_reason: 'end_turn',
  stop_sequence: null,
  usage: {
    input_tokens: 11,
    output_tokens: 3,
    cache_creation: { ephemeral_5m_input_tokens: 2, ephemeral_1h_input_tokens: 4 },
    cache_creation_input_tokens: 6,
    cache_read_input_tokens: 8,
    service_tier: 'standard',
  },
}
const body = (request: HttpClientRequest.HttpClientRequest) => {
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected native JSON body')
  return Schema.decodeUnknownSync(Schema.JsonObject)(
    JSON.parse(new TextDecoder().decode(request.body.body)),
  )
}
const fixture = () => {
  const requests: Array<HttpClientRequest.HttpClientRequest> = []
  const client = HttpClient.make((request) => {
    requests.push(request)
    return Effect.succeed(HttpClientResponse.fromWeb(request, globalThis.Response.json(message)))
  })
  const layer = Catalog.layerApiKey({
    apiKey: Redacted.make('fake-api-key'),
    models: [entry],
    apiUrl: 'https://test.example',
    apiVersion: 'custom-version',
    transformClient: (value) =>
      value.pipe(
        HttpClient.mapRequest((request) =>
          request.pipe(HttpClientRequest.setHeader('x-transform', 'applied')),
        ),
      ),
  }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client)))
  return { requests, client, layer }
}
const resolve = () =>
  Model.Catalog.use((catalog) => catalog.resolve({ provider: 'anthropic', modelId: entry.modelId }))

describe('Anthropic native catalogue', () => {
  it.effect(
    'preserves native transport options and translates pinned model, thinking, caching, UUID7 and maxTokens in actual HTTP body',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const descriptor = yield* resolve()
        const context = yield* descriptor.configure({
          thinking: 'high',
          options: { temperature: 0.2, stop_sequences: ['STOP'], disableParallelToolCalls: true },
          sessionId,
          maxTokens: 12000,
          cache: 'long',
        })
        const response = yield* descriptor.model
          .generateText({ prompt: 'Hello' })
          .pipe(Effect.provideContext(context))
        const request = f.requests[0]
        if (request === undefined) return yield* Effect.die('No captured native request')
        assert.strictEqual(request.url, 'https://test.example/v1/messages?beta=true')
        assert.strictEqual(request.headers['x-api-key'], 'fake-api-key')
        assert.strictEqual(request.headers['anthropic-version'], 'custom-version')
        assert.strictEqual(request.headers['x-transform'], 'applied')
        const sent = body(request)
        assert.strictEqual(sent.model, entry.modelId)
        assert.strictEqual(sent.max_tokens, 12000)
        assert.deepEqual(sent.metadata, { user_id: sessionId })
        assert.deepEqual(sent.thinking, { type: 'adaptive' })
        assert.deepEqual(sent.output_config, { effort: 'high' })
        assert.deepEqual(sent.cache_control, { type: 'ephemeral', ttl: '1h' })
        assert.strictEqual(sent.temperature, 0.2)
        assert.deepEqual(sent.stop_sequences, ['STOP'])
        const finish = response.content.find((part) => part.type === 'finish')
        const usage = descriptor.usage?.(response.usage, finish?.metadata ?? {})
        assert.strictEqual(usage?.cacheWrite1h, 4)
        assert.strictEqual(usage?.totalTokens, 28)
        assert.strictEqual(usage?.cost.known, true)
        assert.approximately(usage?.cost.cacheWrite ?? 0, (2 * 3.75 + 4 * 6) / 1_000_000, 1e-12)
        assert.isDefined(yield* AnthropicClient.AnthropicClient)
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'fixed budgets, short/none cache, and ambient pinning are reflected in the native request',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const descriptor = yield* Catalog.descriptor({
          ...entry,
          thinking: { mode: 'budget', budgets: { low: 1024, high: 8192 } },
        })
        const context = yield* descriptor.configure({
          thinking: 'low',
          options: {},
          sessionId,
          maxTokens: 2000,
          cache: 'short',
        })
        yield* descriptor.model
          .generateText({ prompt: 'Hi' })
          .pipe(
            Effect.provideContext(context),
            Effect.provideService(AnthropicLanguageModel.Config, { model: 'other' }),
          )
        const request = f.requests[0]
        if (request === undefined) return yield* Effect.die('No captured request')
        const sent = body(request)
        assert.strictEqual(sent.model, entry.modelId)
        assert.deepEqual(sent.thinking, { type: 'enabled', budget_tokens: 1024 })
        assert.deepEqual(sent.cache_control, { type: 'ephemeral', ttl: '5m' })
        const off = yield* descriptor.configure({
          thinking: 'off',
          options: {},
          sessionId,
          cache: 'none',
        })
        yield* descriptor.model.generateText({ prompt: 'Hi' }).pipe(Effect.provideContext(off))
        const second = f.requests[1]
        if (second === undefined) return yield* Effect.die('No second request')
        assert.deepEqual(body(second).thinking, { type: 'disabled' })
        assert.strictEqual(body(second).cache_control, null)
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'rejects unsupported native fields, conflicting controls, malformed affinity and over-budget requests before transport',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const descriptor = yield* resolve()
        const negatives: ReadonlyArray<Model.RequestOptions> = [
          { thinking: 'off', options: { model: 'other' } },
          { thinking: 'off', options: { mcp_servers: [] } },
          { thinking: 'off', options: { system: 'replace' } },
          { thinking: 'high', options: { output_config: { effort: 'low' } } },
          { thinking: 'xhigh', options: {} },
          { thinking: 'off', options: { thinking: { type: 'adaptive' } } },
          { thinking: 'off', options: {}, sessionId: 'invalid' },
          { thinking: 'off', options: { max_tokens: 8 }, maxTokens: 9 },
          { thinking: 'off', options: {}, maxTokens: 32001 },
          { thinking: 'off', options: { cache_control: { type: 'ephemeral' } }, cache: 'none' },
          { thinking: 'off', options: { metadata: { user_id: 'other' } }, sessionId },
        ]
        for (const request of negatives)
          assert.strictEqual(
            (yield* descriptor.configure(request).pipe(Effect.flip)).reason._tag,
            'ModelUnsupported',
          )
        assert.strictEqual(f.requests.length, 0)
        const budget = yield* Catalog.descriptor({
          ...entry,
          thinking: { mode: 'budget', budgets: { high: 8192 } },
        })
        assert.strictEqual(
          (yield* budget
            .configure({ thinking: 'high', options: {}, maxTokens: 8000 })
            .pipe(Effect.flip)).reason._tag,
          'ModelUnsupported',
        )
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'Config credentials/URL/version, transformClient and native per-call overrides are preserved',
    () => {
      const f = fixture()
      const layer = Anthropic.layerConfig({
        apiKey: Config.succeed(Redacted.make('config-key')),
        apiUrl: Config.succeed('https://config.example'),
        apiVersion: Config.succeed('configured-version'),
        model: entry.modelId,
        config: { max_tokens: 1000 },
        transformClient: (client) =>
          client.pipe(
            HttpClient.mapRequest((request) =>
              request.pipe(HttpClientRequest.setHeader('x-transform', 'config')),
            ),
          ),
      }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, f.client)))
      return Effect.gen(function* () {
        yield* NativeLanguageModel.generateText({ prompt: 'Hi' }).pipe(
          Anthropic.withConfigOverride({ max_tokens: 2000, temperature: 0.5 }),
        )
        const request = f.requests[0]
        if (request === undefined) return yield* Effect.die('No captured native request')
        assert.strictEqual(request.url, 'https://config.example/v1/messages?beta=true')
        assert.strictEqual(request.headers['x-api-key'], 'config-key')
        assert.strictEqual(request.headers['anthropic-version'], 'configured-version')
        assert.strictEqual(request.headers['x-transform'], 'config')
        assert.strictEqual(body(request).max_tokens, 2000)
        assert.strictEqual(body(request).temperature, 0.5)
      }).pipe(Effect.provide(layer))
    },
  )
  it.effect(
    'missing prices or one-hour price remains unknown; native cache counters stay available',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const descriptor = yield* Catalog.descriptor({ ...entry, prices: undefined })
        const value = new Response.Usage({
          inputTokens: { uncached: 1, total: 8, cacheRead: 2, cacheWrite: 5 },
          outputTokens: { total: 1 },
        })
        assert.strictEqual(descriptor.usage?.(value, {}).cost.known, false)
        const incomplete = yield* Catalog.descriptor({
          ...entry,
          prices: { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 },
        })
        const mapped = incomplete.usage?.(value, {
          anthropic: { usage: { cache_creation: { ephemeral_1h_input_tokens: 3 } } },
        })
        assert.strictEqual(mapped?.cacheWrite1h, 3)
        assert.strictEqual(mapped?.cost.known, false)
        assert.strictEqual(mapped?.cost.totalKnown, false)
        const missingBreakdown = incomplete.usage?.(value, {})
        assert.strictEqual(missingBreakdown?.cost.known, false)
        assert.strictEqual(missingBreakdown?.cost.cacheWrite, 0)
        const badBreakdown = incomplete.usage?.(value, {
          anthropic: { usage: { cache_creation: { ephemeral_1h_input_tokens: 9 } } },
        })
        assert.strictEqual(badBreakdown?.cacheWrite1h, 9)
        assert.strictEqual(badBreakdown?.cost.known, false)
        assert.strictEqual(badBreakdown?.cost.cacheWrite, 0)
        const overflow = yield* Catalog.descriptor({
          ...entry,
          prices: {
            input: Number.MAX_VALUE,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            cacheWrite1h: 0,
          },
        })
        assert.strictEqual(
          overflow.usage?.(
            new Response.Usage({
              inputTokens: { uncached: 2 },
              outputTokens: {},
            }),
            {},
          ).cost.known,
          false,
        )
        assert.strictEqual(
          (yield* Catalog.descriptor({ ...entry, config: { max_tokens: 32001 } }).pipe(Effect.flip))
            .reason._tag,
          'ModelUnsupported',
        )
      }).pipe(Effect.provide(f.layer))
    },
  )
})
