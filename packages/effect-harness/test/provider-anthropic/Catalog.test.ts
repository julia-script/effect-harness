import * as TestSchema from 'effect/testing/TestSchema'
import { vi } from 'vitest'
// effect-nit-allow P8-test-doubles-are-layers: this external npm SDK factory is the adapter construction-count subject; native HTTP/service layer wiring remains real.
vi.mock('@effect/ai-anthropic/AnthropicLanguageModel', { spy: true })
import * as Context from 'effect/Context'
import { assert, describe, it } from '@effect/vitest'
import * as Model from 'effect-harness/Model'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Config from 'effect/Config'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Catalog from 'effect-harness/provider-anthropic/Catalog'

const sessionId = '019a08e0-7c00-7000-8000-000000000001'
const entry: Catalog.Entry = {
  modelId: 'declared-anthropic',
  contextWindow: 200000,
  maxOutputTokens: 32000,
  thinking: { _tag: 'adaptive' },
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
const bodyUnsafe = (request: HttpClientRequest.HttpClientRequest) => {
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

describe('Catalog', { concurrent: false }, () => {
  it.effect('thinking uses canonical tags in domain and encoded catalogues', () =>
    Effect.gen(function* () {
      const self = {
        modelId: 'declared',
        contextWindow: 200000,
        maxOutputTokens: 32000,
        thinking: { _tag: 'budget' as const, budgets: { high: 8192 } },
      }
      const wire = { ...self, thinking: { _tag: 'budget' as const, budgets: { high: 8192 } } }
      const checks = new TestSchema.Asserts(Catalog.Entry)
      yield* checks.decoding().succeedEffect(wire, self)
      yield* checks.encoding().succeedEffect(self, wire)
    }),
  )

  describe('Anthropic native catalogue', () => {
    it.effect(
      'preserves native transport options and translates pinned model, thinking, caching, UUID7 and maxTokens in actual HTTP body',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const descriptor = yield* resolve()
            const context = yield* descriptor.configure({
              thinking: 'high',
              options: {
                temperature: 0.2,
                stop_sequences: ['STOP'],
                disableParallelToolCalls: true,
              },
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
            const sent = bodyUnsafe(request)
            assert.strictEqual(sent.model, entry.modelId)
            assert.strictEqual(sent.max_tokens, 12000)
            assert.deepStrictEqual(sent.metadata, { user_id: sessionId })
            assert.deepStrictEqual(sent.thinking, { type: 'adaptive' })
            assert.deepStrictEqual(sent.output_config, { effort: 'high' })
            assert.deepStrictEqual(sent.cache_control, { type: 'ephemeral', ttl: '1h' })
            assert.strictEqual(sent.temperature, 0.2)
            assert.deepStrictEqual(sent.stop_sequences, ['STOP'])
            const finish = response.content.find((part) => part.type === 'finish')
            const usage = descriptor.usage?.(response.usage, finish?.metadata ?? {})
            assert.strictEqual(usage?.cacheWrite1h, 4)
            assert.strictEqual(usage?.totalTokens, 28)
            assert.strictEqual(usage?.cost.known, true)
            assert.approximately(usage?.cost.cacheWrite ?? 0, (2 * 3.75 + 4 * 6) / 1_000_000, 1e-12)
            assert.isDefined(yield* AnthropicClient.AnthropicClient)
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'fixed budgets, short/none cache, and ambient pinning are reflected in the native request',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const descriptor = yield* Catalog.descriptor({
              ...entry,
              thinking: { _tag: 'budget', budgets: { low: 1024, high: 8192 } },
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
                Effect.provideService(
                  AnthropicLanguageModel.Config,
                  AnthropicLanguageModel.Config.of({ model: 'other' }),
                ),
              )
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('No captured request')
            const sent = bodyUnsafe(request)
            assert.strictEqual(sent.model, entry.modelId)
            assert.deepStrictEqual(sent.thinking, { type: 'enabled', budget_tokens: 1024 })
            assert.deepStrictEqual(sent.cache_control, { type: 'ephemeral', ttl: '5m' })
            const off = yield* descriptor.configure({
              thinking: 'off',
              options: {},
              sessionId,
              cache: 'none',
            })
            yield* descriptor.model.generateText({ prompt: 'Hi' }).pipe(Effect.provideContext(off))
            const second = f.requests[1]
            if (second === undefined) return yield* Effect.die('No second request')
            assert.deepStrictEqual(bodyUnsafe(second).thinking, { type: 'disabled' })
            assert.strictEqual(bodyUnsafe(second).cache_control, null)
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'rejects unsupported native fields, conflicting controls, malformed affinity and over-budget requests before transport',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
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
                'ModelUnsupportedError',
              )
            assert.strictEqual(f.requests.length, 0)
            const budget = yield* Catalog.descriptor({
              ...entry,
              thinking: { _tag: 'budget', budgets: { high: 8192 } },
            })
            assert.strictEqual(
              (yield* budget
                .configure({ thinking: 'high', options: {}, maxTokens: 8000 })
                .pipe(Effect.flip)).reason._tag,
              'ModelUnsupportedError',
            )
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'Config credentials/URL/version, transformClient and native per-call overrides are preserved',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          const layer = HarnessAnthropicLanguageModel.layerApiKeyConfig({
            apiKey: Config.succeed(Redacted.make('config-key')),
            apiUrl: Config.succeed('https://config.example'),
            apiVersion: Config.succeed('configured-version'),
            model: Config.succeed(entry.modelId),
            config: Config.succeed({ max_tokens: 1000 }),
            transformClient: Config.succeed((client: HttpClient.HttpClient) =>
              client.pipe(
                HttpClient.mapRequest((request) =>
                  request.pipe(HttpClientRequest.setHeader('x-transform', 'config')),
                ),
              ),
            ),
          }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, f.client)))
          return yield* Effect.gen(function* () {
            yield* LanguageModel.generateText({ prompt: 'Hi' }).pipe(
              HarnessAnthropicLanguageModel.withConfigOverride({
                max_tokens: 2000,
                temperature: 0.5,
              }),
            )
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('No captured native request')
            assert.strictEqual(request.url, 'https://config.example/v1/messages?beta=true')
            assert.strictEqual(request.headers['x-api-key'], 'config-key')
            assert.strictEqual(request.headers['anthropic-version'], 'configured-version')
            assert.strictEqual(request.headers['x-transform'], 'config')
            assert.strictEqual(bodyUnsafe(request).max_tokens, 2000)
            assert.strictEqual(bodyUnsafe(request).temperature, 0.5)
          }).pipe(Effect.provide(layer))
        }),
    )
    it.effect(
      'missing prices or one-hour price remains unknown; native cache counters stay available',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
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
              (yield* Catalog.descriptor({ ...entry, config: { max_tokens: 32001 } }).pipe(
                Effect.flip,
              )).reason._tag,
              'ModelUnsupportedError',
            )
          }).pipe(Effect.provide(f.layer))
        }),
    )
  })

  describe('catalogue schema admission', () => {
    it.effect(
      'rejects malformed declared prices and limits with original SchemaError cause before client work',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const constructions = vi.mocked(AnthropicLanguageModel.make).mock.calls.length
            const invalid: ReadonlyArray<Catalog.Entry> = [
              { ...entry, modelId: '' },
              { ...entry, contextWindow: Number.MAX_SAFE_INTEGER + 1 },
              { ...entry, maxOutputTokens: 0 },
              { ...entry, maxOutputTokens: entry.contextWindow + 1 },
              { ...entry, prices: { input: 1 } } as Catalog.Entry,
              {
                ...entry,
                prices: { input: 1, output: 1, cacheRead: 0, cacheWrite: Number.POSITIVE_INFINITY },
              },
            ]
            for (const value of invalid) {
              const error = yield* Catalog.descriptor(value).pipe(Effect.flip)
              assert.strictEqual(error.reason._tag, 'ModelUnsupportedError')
              assert.isTrue(Schema.isSchemaError(error.cause))
            }
            assert.strictEqual(
              vi.mocked(AnthropicLanguageModel.make).mock.calls.length,
              constructions,
            )
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
    )
  })

  it.effect('catalogue schemas preserve native nullable options and undefined normalization', () =>
    Effect.gen(function* () {
      const f = fixture()
      return yield* Effect.gen(function* () {
        const model = yield* Catalog.descriptor({
          ...entry,
          config: {
            output_config: { effort: null },
            disableParallelToolCalls: undefined,
            structuredOutputs: undefined,
            strictJsonSchema: undefined,
            midConversationSystemMessages: undefined,
          },
        })
        const decoded = { ...entry, config: { output_config: { effort: null } } }
        yield* new TestSchema.Asserts(Schema.toType(Catalog.Entry))
          .decoding()
          .succeedEffect(decoded, {
            ...entry,
            config: { output_config: { effort: null } },
          })
        assert.strictEqual(decoded.config?.output_config?.effort, null)
        const context = yield* model.configure({
          thinking: 'high',
          options: {
            max_tokens: undefined,
            stop_sequences: undefined,
          } as unknown as Schema.JsonObject,
        })
        const config = Context.get(context, AnthropicLanguageModel.Config)
        assert.strictEqual(config.max_tokens, entry.maxOutputTokens)
        assert.strictEqual(config.output_config?.effort, 'high')
        assert.isFalse(Object.hasOwn(config, 'stop_sequences'))
        assert.strictEqual(f.requests.length, 0)
      }).pipe(Effect.provide(f.layer))
    }),
  )

  it.effect(
    'incomplete price declarations fail schema admission before native model construction',
    () =>
      Effect.gen(function* () {
        const f = fixture()
        return yield* Effect.gen(function* () {
          const constructions = vi.mocked(AnthropicLanguageModel.make).mock.calls.length
          const malformed = { ...entry, prices: { input: 1 } } as Catalog.Entry
          const error = yield* Catalog.descriptor(malformed).pipe(Effect.flip)
          assert.strictEqual(error.reason._tag, 'ModelUnsupportedError')
          assert.isTrue(Schema.isSchemaError(error.cause))
          assert.strictEqual(
            vi.mocked(AnthropicLanguageModel.make).mock.calls.length,
            constructions,
          )
          assert.strictEqual(f.requests.length, 0)
        }).pipe(Effect.provide(f.layer))
      }),
  )

  it.effect(
    'catalogue schema enforces declared thinking budgets and default output relations',
    () =>
      Effect.gen(function* () {
        const f = fixture()
        return yield* Effect.gen(function* () {
          for (const value of [
            { ...entry, thinking: { _tag: 'budget', budgets: { low: 1023 } } },
            { ...entry, thinking: { _tag: 'budget', budgets: { low: entry.maxOutputTokens } } },
            { ...entry, config: { max_tokens: entry.maxOutputTokens + 1 } },
          ] as ReadonlyArray<Catalog.Entry>) {
            const error = yield* Catalog.descriptor(value).pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'ModelUnsupportedError')
            assert.isTrue(Schema.isSchemaError(error.cause))
          }
          assert.strictEqual(f.requests.length, 0)
        }).pipe(Effect.provide(f.layer))
      }),
  )
})

import * as HarnessAnthropicLanguageModel from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
