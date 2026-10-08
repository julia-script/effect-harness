import { vi } from 'vitest'
// effect-nit-allow P8-test-doubles-are-layers: this external npm SDK factory is the adapter construction-count subject; native HTTP/service layer wiring remains real.
vi.mock('@effect/ai-openai/OpenAiLanguageModel', { spy: true })
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as Context from 'effect/Context'
import * as LanguageModel from 'effect/ai/LanguageModel'

import { assert, describe, it } from '@effect/vitest'
import * as Model from 'effect-harness/Model'
import * as Usage from 'effect-harness/Usage'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Response from 'effect/ai/Response'
import * as HttpClient from 'effect/http/HttpClient'
import type * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Catalog from 'effect-harness/provider-openai/Catalog'

const sessionId = '019a08e0-7c00-7000-8000-000000000001'
const entry: Catalog.Entry = {
  modelId: 'declared-model',
  contextWindow: 128000,
  maxOutputTokens: 16000,
  reasoningEfforts: ['none', 'low', 'high'],
  cache: 'prompt-cache-options',
  prices: { input: 2, output: 3, cacheRead: 0.2, cacheWrite: 0 },
}
const response = {
  id: 'response-1',
  model: 'declared-model',
  created_at: 0,
  status: 'completed',
  output: [
    {
      type: 'message',
      id: 'message-1',
      role: 'assistant',
      status: 'completed',
      content: [{ type: 'output_text', text: 'Hello', annotations: [] }],
    },
  ],
  usage: {
    input_tokens: 10,
    output_tokens: 3,
    total_tokens: 13,
    input_tokens_details: { cached_tokens: 2 },
    output_tokens_details: { reasoning_tokens: 1 },
  },
}
const bodyUnsafe = (request: HttpClientRequest.HttpClientRequest) => {
  assert.strictEqual(request.body._tag, 'Uint8Array')
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected native JSON body')
  return Schema.decodeUnknownSync(Schema.JsonObject)(
    JSON.parse(new TextDecoder().decode(request.body.body)),
  )
}
const resolve = () =>
  Model.Catalog.use((catalog) => catalog.resolve({ provider: 'openai', modelId: entry.modelId }))
const fixture = () => {
  const requests: Array<HttpClientRequest.HttpClientRequest> = []
  const client = HttpClient.make((request) => {
    requests.push(request)
    return Effect.succeed(HttpClientResponse.fromWeb(request, globalThis.Response.json(response)))
  })
  const layer = Catalog.layerApiKey({ apiKey: Redacted.make('fake-key'), models: [entry] }).pipe(
    Layer.provide(Layer.succeed(HttpClient.HttpClient, client)),
  )
  return { requests, layer }
}

describe('Catalog', { concurrent: false }, () => {
  it.effect('empty malformed subjects fail semantically before any native model work', () =>
    Effect.gen(function* () {
      // Deliberately model an untyped JavaScript caller that supplies a malformed subject.
      const invalid: unknown = {}
      const result = Catalog.descriptor(invalid as Catalog.Entry)
      assert.isTrue(Effect.isEffect(result))
      const error = yield* result.pipe(
        Effect.provide(
          Layer.succeed(
            OpenAiClient.OpenAiClient,
            OpenAiClient.OpenAiClient.of({
              client: HttpClient.make((request) => Effect.die(`Unexpected request ${request.url}`)),
              createResponse: () => Effect.die('Unexpected response work'),
              createResponseStream: () => Effect.die('Unexpected stream work'),
              createEmbedding: () => Effect.die('Unexpected embedding work'),
            }),
          ),
        ),
        Effect.flip,
      )
      assert.strictEqual(error._tag, 'ModelError')
      assert.strictEqual(error.reason._tag, 'ModelUnsupportedError')
    }),
  )

  describe('OpenAI native catalogue', () => {
    it.effect(
      'pins model/client and translates UUID7, reasoning, cache and token cap into actual native HTTP body',
      () =>
        Effect.gen(function* () {
          const f = yield* Inspection
          const descriptor = yield* resolve()
          const context = yield* descriptor.configure({
            thinking: 'high',
            options: {
              temperature: 0.4,
              text: { verbosity: 'low' },
              reasoning: { summary: 'auto' },
            },
            sessionId,
            maxTokens: 9000,
            cache: 'long',
          })
          const output = yield* descriptor.model
            .generateText({ prompt: 'Hello' })
            .pipe(Effect.provideContext(context))
          assert.strictEqual(output.text, 'Hello')
          const request = f.requests[0]
          assert.isDefined(request)
          if (request === undefined) return yield* Effect.die('No captured request')
          const sent = bodyUnsafe(request)
          assert.strictEqual(sent.model, entry.modelId)
          assert.strictEqual(sent.max_output_tokens, 9000)
          assert.strictEqual(sent.prompt_cache_key, sessionId)
          assert.deepStrictEqual(sent.prompt_cache_options, { mode: 'implicit', ttl: '30m' })
          assert.deepStrictEqual(sent.reasoning, { effort: 'high', summary: 'auto' })
          assert.strictEqual(sent.temperature, 0.4)
          assert.deepStrictEqual(sent.text, { verbosity: 'low', format: { type: 'text' } })
          const mapped = descriptor.usage?.(output.usage, {})
          assert.strictEqual(mapped?.input, 8)
          assert.strictEqual(mapped?.cacheRead, 2)
          assert.strictEqual(mapped?.reasoning, 1)
          assert.strictEqual(mapped?.cost.known, true)
          assert.approximately(
            mapped?.cost.total ?? 0,
            (8 * 2 + 3 * 3 + 2 * 0.2) / 1_000_000,
            1e-12,
          )
          assert.isDefined(yield* OpenAiClient.OpenAiClient)
        }).pipe(Effect.provide(fixtureLayer())),
    )
    it.effect(
      'rejects unknown, unpinned, unsupported and conflicting options before a request',
      () =>
        Effect.gen(function* () {
          const f = yield* Inspection
          const descriptor = yield* resolve()
          const negatives: ReadonlyArray<Model.RequestOptions> = [
            { thinking: 'high', options: { model: 'other' }, sessionId },
            { thinking: 'off', options: { previous_response_id: 'prior' }, sessionId },
            { thinking: 'xhigh', options: {}, sessionId },
            { thinking: 'high', options: { reasoning: { effort: 'low' } }, sessionId },
            { thinking: 'off', options: { text: { format: { type: 'json_object' } } }, sessionId },
            { thinking: 'off', options: {}, sessionId: 'not-uuid' },
            { thinking: 'off', options: {}, maxTokens: 16001 },
            { thinking: 'off', options: { max_output_tokens: 5 }, maxTokens: 6 },
            { thinking: 'off', options: { temperature: 'bad' } },
            { thinking: 'off', options: { prompt_cache_key: 'other' }, sessionId },
            {
              thinking: 'off',
              options: { prompt_cache_options: { mode: 'implicit' } },
              cache: 'none',
            },
          ]
          for (const request of negatives)
            assert.strictEqual(
              (yield* descriptor.configure(request).pipe(Effect.flip)).reason._tag,
              'ModelUnsupportedError',
            )
          assert.strictEqual(f.requests.length, 0)
          assert.strictEqual(
            (yield* Model.Catalog.use((catalog) =>
              catalog.resolve({ provider: 'other', modelId: entry.modelId }),
            ).pipe(Effect.flip)).reason._tag,
            'ModelNoModelError',
          )
        }).pipe(Effect.provide(fixtureLayer())),
    )
    it.effect(
      'request context overrides ambient model configuration and the captured HTTP client remains pinned',
      () =>
        Effect.gen(function* () {
          const f = yield* Inspection
          const descriptor = yield* resolve()
          const context = yield* descriptor.configure({
            thinking: 'off',
            options: {},
            sessionId,
            cache: 'none',
          })
          const unsafe = HttpClient.make(() => Effect.die('Captured client was replaced'))
          yield* descriptor.model
            .generateText({ prompt: 'Hi' })
            .pipe(
              Effect.provideContext(context),
              Effect.provideService(
                OpenAiLanguageModel.Config,
                OpenAiLanguageModel.Config.of({ model: 'other-model' }),
              ),
              Effect.provideService(HttpClient.HttpClient, unsafe),
            )
          const request = f.requests[0]
          if (request === undefined) return yield* Effect.die('No captured request')
          const sent = bodyUnsafe(request)
          assert.strictEqual(sent.model, entry.modelId)
          assert.deepStrictEqual(sent.prompt_cache_options, { mode: 'explicit' })
          assert.strictEqual(sent.prompt_cache_key, undefined)
          assert.deepStrictEqual(sent.reasoning, { effort: 'none' })
        }).pipe(Effect.provide(fixtureLayer())),
    )
    it.effect(
      'unknown pricing/counters remain explicitly unknown and invalid catalogue metadata is rejected',
      () =>
        Effect.gen(function* () {
          const descriptor = yield* Catalog.descriptor({ ...entry, prices: undefined })
          const value = descriptor.usage?.(
            new Response.Usage({ inputTokens: {}, outputTokens: {} }),
            {},
          )
          assert.strictEqual(value?.cost.known, false)
          assert.strictEqual(value?.cost.totalKnown, false)
          const merged = Usage.add(Usage.make(), value ?? Usage.make())
          assert.strictEqual(merged.cost.known, false)
          const partial = yield* Catalog.descriptor({
            ...entry,
            prices: { input: 2, output: 0, cacheRead: 0, cacheWrite: 0 },
          })
          const uncertainInput = partial.usage(
            new Response.Usage({ inputTokens: { total: 10 }, outputTokens: {} }),
            {},
          )
          assert.strictEqual(uncertainInput.cost.known, false)
          assert.strictEqual(uncertainInput.cost.input, 0)
          const overflow = yield* Catalog.descriptor({
            ...entry,
            prices: { input: Number.MAX_VALUE, output: 0, cacheRead: 0, cacheWrite: 0 },
          })
          assert.strictEqual(
            overflow.usage(
              new Response.Usage({ inputTokens: { uncached: 2 }, outputTokens: {} }),
              {},
            ).cost.known,
            false,
          )
          assert.strictEqual(
            (yield* Catalog.descriptor({
              ...entry,
              config: { max_output_tokens: entry.maxOutputTokens + 1 },
            }).pipe(Effect.flip)).reason._tag,
            'ModelUnsupportedError',
          )
          assert.strictEqual(
            (yield* Catalog.descriptor({ ...entry, contextWindow: 0 }).pipe(Effect.flip)).reason
              ._tag,
            'ModelUnsupportedError',
          )
          assert.strictEqual(
            (yield* Catalog.descriptor({
              ...entry,
              prices: { input: -1, output: 0, cacheRead: 0, cacheWrite: 0 },
            }).pipe(Effect.flip)).reason._tag,
            'ModelUnsupportedError',
          )
        }).pipe(Effect.provide(fixtureLayer())),
    )
  })
  function fixtureLayer() {
    // Separate fixture hook lets the test inspect the same request owner.
    const f = fixture()
    return Layer.merge(f.layer, Layer.succeed(Inspection, f))
  }
  class Inspection extends Context.Service<Inspection, ReturnType<typeof fixture>>()(
    'openai-catalog-test',
  ) {}

  describe('OpenAI configured provider capability', () => {
    it.effect('reexports the exact substituted native client and constructs one model', () =>
      Effect.gen(function* () {
        const http = HttpClient.make((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, globalThis.Response.json(response))),
        )
        const client = yield* OpenAiClient.make({ apiKey: Redacted.make('fake-key') }).pipe(
          Effect.provideService(HttpClient.HttpClient, http),
        )
        const spy = vi.mocked(OpenAiLanguageModel.make)
        spy.mockClear()
        yield* Effect.gen(function* () {
          const context = yield* Layer.build(
            HarnessOpenAiLanguageModel.layerConfig({ model: Config.String('MODEL') }).pipe(
              Layer.provide(Layer.succeed(OpenAiClient.OpenAiClient, client)),
            ),
          ).pipe(
            Effect.provideService(
              ConfigProvider.ConfigProvider,
              ConfigProvider.fromUnknown({ MODEL: 'substituted-model' }),
            ),
          )
          assert.strictEqual(Context.get(context, OpenAiClient.OpenAiClient), client)
          assert.isDefined(Context.get(context, LanguageModel.LanguageModel))
          assert.strictEqual(spy.mock.calls.length, 1)
        })
      }),
    )
    it.effect(
      'API-key configuration loads real provider values and preserves per-call overrides',
      () =>
        Effect.gen(function* () {
          const requests: Array<HttpClientRequest.HttpClientRequest> = []
          const http = HttpClient.make((request) => {
            requests.push(request)
            return Effect.succeed(
              HttpClientResponse.fromWeb(request, globalThis.Response.json(response)),
            )
          })
          const layer = HarnessOpenAiLanguageModel.layerApiKeyConfig({
            model: Config.String('MODEL'),
            apiKey: Config.Redacted('API_KEY'),
            apiUrl: Config.String('API_URL'),
          }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))
          return yield* Effect.gen(function* () {
            yield* LanguageModel.generateText({ prompt: 'Hello' }).pipe(
              OpenAiLanguageModel.withConfigOverride({ max_output_tokens: 1000 }),
            )
            const request = requests[0]
            if (request === undefined) return yield* Effect.die('Expected configured request')
            assert.strictEqual(request.headers.authorization, 'Bearer configured-key')
            assert.strictEqual(request.url, 'https://config.example/responses')
            assert.strictEqual(bodyUnsafe(request).model, 'configured-model')
            assert.strictEqual(bodyUnsafe(request).max_output_tokens, 1000)
            assert.isDefined(yield* OpenAiClient.OpenAiClient)
          }).pipe(
            Effect.provide(layer),
            Effect.provideService(
              ConfigProvider.ConfigProvider,
              ConfigProvider.fromUnknown({
                MODEL: 'configured-model',
                API_KEY: 'configured-key',
                API_URL: 'https://config.example',
              }),
            ),
          )
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
            const constructions = vi.mocked(OpenAiLanguageModel.make).mock.calls.length
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
            assert.strictEqual(vi.mocked(OpenAiLanguageModel.make).mock.calls.length, constructions)
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
    )
  })

  it.effect(
    'catalogue schemas preserve undefined defaults, configuration convenience fields and native null rejection',
    () =>
      Effect.gen(function* () {
        const f = fixture()
        return yield* Effect.gen(function* () {
          const model = yield* Catalog.descriptor({
            ...entry,
            config: {
              max_output_tokens: undefined,
              text: { verbosity: undefined },
              strictJsonSchema: undefined,
            },
          })
          const context = yield* model.configure({ thinking: 'off', options: {} })
          const config = Context.get(context, OpenAiLanguageModel.Config)
          assert.strictEqual(config.max_output_tokens, entry.maxOutputTokens)
          assert.deepStrictEqual(config.text, { verbosity: undefined })
          assert.isFalse(
            Schema.is(Catalog.Entry)({ ...entry, config: { max_output_tokens: null } }),
          )
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
          const constructions = vi.mocked(OpenAiLanguageModel.make).mock.calls.length
          const malformed = { ...entry, prices: { input: 1 } } as Catalog.Entry
          const error = yield* Catalog.descriptor(malformed).pipe(Effect.flip)
          assert.strictEqual(error.reason._tag, 'ModelUnsupportedError')
          assert.isTrue(Schema.isSchemaError(error.cause))
          assert.strictEqual(vi.mocked(OpenAiLanguageModel.make).mock.calls.length, constructions)
          assert.strictEqual(f.requests.length, 0)
        }).pipe(Effect.provide(f.layer))
      }),
  )
})

import * as HarnessOpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
