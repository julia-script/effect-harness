import { assert, describe, it } from '@effect/vitest'
import * as Model from '@effect-harness/harness/Model'
import * as Usage from '@effect-harness/harness/Usage'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Response from 'effect/ai/Response'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Catalog from '../src/Catalog.ts'
import { ChatGpt } from '../src/ChatGpt.ts'

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
const body = (request: HttpClientRequest.HttpClientRequest) => {
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

describe('OpenAI native catalogue', () => {
  it.effect(
    'pins model/client and translates UUID7, reasoning, cache and token cap into actual native HTTP body',
    () =>
      Effect.gen(function* () {
        const f = yield* Inspection
        const descriptor = yield* resolve()
        const context = yield* descriptor.configure({
          thinking: 'high',
          options: { temperature: 0.4, text: { verbosity: 'low' }, reasoning: { summary: 'auto' } },
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
        const sent = body(request)
        assert.strictEqual(sent.model, entry.modelId)
        assert.strictEqual(sent.max_output_tokens, 9000)
        assert.strictEqual(sent.prompt_cache_key, sessionId)
        assert.deepEqual(sent.prompt_cache_options, { mode: 'implicit', ttl: '30m' })
        assert.deepEqual(sent.reasoning, { effort: 'high', summary: 'auto' })
        assert.strictEqual(sent.temperature, 0.4)
        assert.deepEqual(sent.text, { verbosity: 'low', format: { type: 'text' } })
        const mapped = descriptor.usage?.(output.usage, {})
        assert.strictEqual(mapped?.input, 8)
        assert.strictEqual(mapped?.cacheRead, 2)
        assert.strictEqual(mapped?.reasoning, 1)
        assert.strictEqual(mapped?.cost.known, true)
        assert.approximately(mapped?.cost.total ?? 0, (8 * 2 + 3 * 3 + 2 * 0.2) / 1_000_000, 1e-12)
        assert.isDefined(yield* OpenAiClient.OpenAiClient)
      }).pipe(Effect.provide(fixtureLayer())),
  )
  it.effect('rejects unknown, unpinned, unsupported and conflicting options before a request', () =>
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
        { thinking: 'off', options: { prompt_cache_options: { mode: 'implicit' } }, cache: 'none' },
      ]
      for (const request of negatives)
        assert.strictEqual(
          (yield* descriptor.configure(request).pipe(Effect.flip)).reason._tag,
          'ModelUnsupported',
        )
      assert.strictEqual(f.requests.length, 0)
      assert.strictEqual(
        (yield* Model.Catalog.use((catalog) =>
          catalog.resolve({ provider: 'other', modelId: entry.modelId }),
        ).pipe(Effect.flip)).reason._tag,
        'ModelNoModel',
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
            Effect.provideService(OpenAiLanguageModel.Config, { model: 'other-model' }),
            Effect.provideService(HttpClient.HttpClient, unsafe),
          )
        const request = f.requests[0]
        if (request === undefined) return yield* Effect.die('No captured request')
        const sent = body(request)
        assert.strictEqual(sent.model, entry.modelId)
        assert.deepEqual(sent.prompt_cache_options, { mode: 'explicit' })
        assert.strictEqual(sent.prompt_cache_key, undefined)
        assert.deepEqual(sent.reasoning, { effort: 'none' })
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
        const merged = Usage.add(Usage.zero(), value ?? Usage.zero())
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
          overflow.usage(new Response.Usage({ inputTokens: { uncached: 2 }, outputTokens: {} }), {})
            .cost.known,
          false,
        )
        assert.strictEqual(
          (yield* Catalog.descriptor({
            ...entry,
            config: { max_output_tokens: entry.maxOutputTokens + 1 },
          }).pipe(Effect.flip)).reason._tag,
          'ModelUnsupported',
        )
        assert.strictEqual(
          (yield* Catalog.descriptor({ ...entry, contextWindow: 0 }).pipe(Effect.flip)).reason._tag,
          'ModelUnsupported',
        )
        assert.strictEqual(
          (yield* Catalog.descriptor({
            ...entry,
            prices: { input: -1, output: 0, cacheRead: 0, cacheWrite: 0 },
          }).pipe(Effect.flip)).reason._tag,
          'ModelUnsupported',
        )
      }).pipe(Effect.provide(fixtureLayer())),
  )
  it.effect(
    'official account catalogue uses fresh native bearer credentials and public streaming store:false Responses',
    () =>
      Effect.gen(function* () {
        let fresh = 0
        const requests: Array<HttpClientRequest.HttpClientRequest> = []
        const auth = Layer.succeed(ChatGpt, {
          begin: () => Effect.die('unexpected login'),
          complete: () => Effect.die('unexpected callback'),
          refresh: () => Effect.die('unexpected refresh'),
          accessToken: () => Effect.sync(() => Redacted.make(`fake-token-${++fresh}`)),
          models: () => Effect.succeed([]),
          signOut: () => Effect.void,
          cancel: () => Effect.void,
        })
        const client = HttpClient.make((request) => {
          requests.push(request)
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new globalThis.Response(
                `data: ${JSON.stringify({ type: 'response.completed', sequence_number: 1, response })}\n\n`,
                { headers: { 'content-type': 'text/event-stream' } },
              ),
            ),
          )
        })
        const layer = Catalog.layerChatGpt({ account: 'test-account', models: [entry] }).pipe(
          Layer.provide(Layer.merge(auth, Layer.succeed(HttpClient.HttpClient, client))),
        )
        yield* Effect.gen(function* () {
          const descriptor = yield* Model.Catalog.use((catalog) =>
            catalog.resolve({ provider: 'openai-chatgpt', modelId: entry.modelId }),
          )
          const context = yield* descriptor.configure({
            thinking: 'low',
            options: {},
            sessionId,
            maxTokens: 500,
          })
          yield* descriptor.model
            .generateText({ prompt: 'Hi' })
            .pipe(Effect.provideContext(context))
          yield* descriptor.model
            .generateText({ prompt: 'Hi again' })
            .pipe(Effect.provideContext(context))
          assert.strictEqual(
            (yield* descriptor
              .configure({ thinking: 'off', options: { store: true }, sessionId })
              .pipe(Effect.flip)).reason._tag,
            'ModelUnsupported',
          )
          assert.isDefined(yield* OpenAiClient.OpenAiClient)
        }).pipe(Effect.provide(layer))
        assert.strictEqual(fresh, 2)
        for (const [index, request] of requests.entries()) {
          assert.strictEqual(request.url, 'https://api.openai.com/v1/responses')
          assert.strictEqual(request.headers.authorization, `Bearer fake-token-${index + 1}`)
          const sent = body(request)
          assert.strictEqual(sent.store, false)
          assert.strictEqual(sent.stream, true)
          assert.strictEqual(sent.max_output_tokens, 500)
          assert.strictEqual(sent.prompt_cache_key, sessionId)
        }
      }),
  )
})
function fixtureLayer() {
  // Separate fixture hook lets the test inspect the same request owner.
  const f = fixture()
  return Layer.merge(f.layer, Layer.succeed(Inspection, f))
}
import * as Context from 'effect/Context'
class Inspection extends Context.Service<Inspection, ReturnType<typeof fixture>>()(
  'openai-catalog-test',
) {}
