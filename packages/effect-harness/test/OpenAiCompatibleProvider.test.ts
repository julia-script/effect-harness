import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as OpenAiSchema from '@effect/ai-openai/Generated'
import * as ToolResult from '../src/ToolResult.js'
import { payload } from './fixtures/SubscriptionProtocol.js'
import * as Context from 'effect/Context'
import * as Native from '@effect/ai-openai-compat/OpenAiLanguageModel'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Providers from '../src/Providers.js'
import * as ProviderStore from '../src/ProviderStore.js'
import * as MemoryCredentials from '../src/provider-auth/MemoryCredentialStore.js'
import * as Gateway from '../src/provider-openai-compatible/Provider.js'
import type * as ModelInfo from '../src/ModelInfo.js'
const info: ModelInfo.ModelInfo = {
  id: 'same-model',
  name: 'Same',
  capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
}

it.effect(
  'shared transport routes independent instances to their own endpoint and credentials',
  () =>
    Effect.gen(function* () {
      const manager = yield* Providers.make({ definitions: { api: Gateway.definition } })
      const first = yield* manager.add({
        provider: 'api',
        label: 'First',
        config: { baseUrl: 'https://first.example/v1', models: [info] },
        secrets: { apiKey: Redacted.make('first-key') },
      })
      const second = yield* manager.add({
        provider: 'api',
        label: 'Second',
        config: { baseUrl: 'https://second.example/v1', models: [info] },
        secrets: { apiKey: Redacted.make('second-key') },
      })
      const anonymous = yield* manager.add({
        provider: 'api',
        label: 'Local',
        config: { baseUrl: 'http://localhost:8080/v1', authentication: 'optional', models: [info] },
      })
      const missing = yield* manager.add({
        provider: 'api',
        label: 'Not logged in',
        config: { baseUrl: 'https://other.example/v1', models: [info] },
      })
      const requests: Array<{ url: string; authorization: string | undefined }> = []
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          requests.push({ url: request.url, authorization: request.headers.authorization })
          return HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({
                id: 'chat-1',
                object: 'chat.completion',
                created: 0,
                model: info.id,
                choices: [
                  {
                    index: 0,
                    message: { role: 'assistant', content: 'hello' },
                    finish_reason: 'stop',
                  },
                ],
                usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 },
              }),
            ),
          )
        }),
      )
      yield* Effect.gen(function* () {
        assert.deepEqual((yield* manager.models.list({ instanceId: missing.id })).items, [
          { id: info.id, name: info.name },
        ])
        assert.equal(
          (yield* Effect.flip(manager.models.resolve({ instanceId: missing.id, modelId: info.id })))
            .reason,
          'MissingCredential',
        )
        for (const instance of [first, second, anonymous]) {
          const model = yield* manager.models.resolve({ instanceId: instance.id, modelId: info.id })
          assert.equal((yield* model.languageModel.generateText({ prompt: 'hello' })).text, 'hello')
        }
      }).pipe(Effect.provideService(HttpClient.HttpClient, http))
      assert.deepEqual(requests, [
        { url: 'https://first.example/v1/chat/completions', authorization: 'Bearer first-key' },
        { url: 'https://second.example/v1/chat/completions', authorization: 'Bearer second-key' },
        { url: 'http://localhost:8080/v1/chat/completions', authorization: undefined },
      ])
    }).pipe(Effect.provide(Layer.merge(ProviderStore.layerMemory, MemoryCredentials.layer))),
)

it.effect(
  'custom metadata definitions infer gateway config and keep list/detail fetching independent',
  () =>
    Effect.gen(function* () {
      const calls: string[] = []
      const definition = Gateway.define({
        version: 1,
        config: Schema.Struct({ region: Schema.NonEmptyString }),
        connection: (config) => ({ baseUrl: `https://${config.region}.example/v1` }),
        models: {
          list: (context) =>
            Effect.sync(() => {
              calls.push(`list:${context.config.region}`)
              return { items: [{ id: info.id, name: info.name }] }
            }),
          get: (context, id) =>
            Effect.sync(() => {
              calls.push(`get:${context.config.region}:${id}`)
              return { ...info, id }
            }),
        },
      })
      const providers = yield* Providers.make({ definitions: { custom: definition } })
      const instance = yield* providers.add({
        provider: 'custom',
        label: 'Custom',
        config: { region: 'eu' },
      })
      assert.deepEqual(calls, [])
      yield* providers.models.get({ instanceId: instance.id, modelId: 'model' })
      assert.deepEqual(calls, ['get:eu:model'])
      yield* providers.models.list({ instanceId: instance.id })
      assert.deepEqual(calls, ['get:eu:model', 'list:eu'])
    }).pipe(Effect.provide(Layer.merge(ProviderStore.layerMemory, MemoryCredentials.layer))),
)

it.effect(
  'gateway settings round-trip through the durable codec and reject undeclared efforts',
  () =>
    Effect.gen(function* () {
      const manager = yield* Providers.make({ definitions: { api: Gateway.definition } })
      const instance = yield* manager.add({
        provider: 'api',
        label: 'Local',
        config: {
          baseUrl: 'http://localhost:8080/v1',
          authentication: 'optional',
          models: [
            {
              ...info,
              capabilities: { ...info.capabilities!, reasoning: true },
              reasoning: {
                effort: { values: ['low', 'high'], default: 'low' },
                thinking: { modes: ['enabled'] },
              },
            },
          ],
        },
      })
      const model = yield* manager.models.resolve({ instanceId: instance.id, modelId: info.id })
      const context = Context.make(Native.Config, {
        temperature: 0.5,
        reasoning: { effort: 'high' },
      })
      const encoded = yield* model.configuration!.encode(context)
      const restored = yield* model.configuration!.restore(encoded)
      assert.deepEqual(Context.getOrUndefined(restored, Native.Config), {
        temperature: 0.5,
        reasoning: { effort: 'high' },
      })
      assert.equal(
        (yield* Effect.flip(
          model.configuration!.encode(
            Context.make(Native.Config, { reasoning: { effort: 'unsupported' } }),
          ),
        ))._tag,
        'ModelError',
      )
      assert.equal(
        (yield* Effect.flip(
          model.configuration!.restore({
            instanceId: instance.id,
            revision: instance.revision,
            value: { temperature: 'invalid' },
          }),
        ))._tag,
        'ModelError',
      )
      assert.equal(
        (yield* Effect.flip(
          manager.models.resolve({
            instanceId: instance.id,
            modelId: info.id,
            options: { reasoning: { thinking: { mode: 'enabled' } } },
          }),
        ))._tag,
        'ModelCatalogError',
      )
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ProviderStore.layerMemory,
          MemoryCredentials.layer,
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make(() => Effect.die('No HTTP expected')),
          ),
        ),
      ),
    ),
)

for (const streaming of [false, true]) {
  for (const format of ['openai', 'openrouter'] as const) {
    it.effect(
      `${format} maps reasoning and tool media on ${streaming ? 'streaming' : 'completion'} requests`,
      () =>
        Effect.gen(function* () {
          const bodies: Schema.JsonObject[] = []
          const client = HttpClient.make((request) =>
            Effect.sync(() => {
              const body = payload(request)
              bodies.push(body)
              const answer = {
                id: 'chat-1',
                object: 'chat.completion',
                created: 0,
                model: 'same-model',
                choices: [
                  {
                    index: 0,
                    message: { role: 'assistant', content: 'hello' },
                    finish_reason: 'stop',
                  },
                ],
              }
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  streaming
                    ? `data: ${JSON.stringify({ ...answer, object: 'chat.completion.chunk', choices: [{ index: 0, delta: { content: 'hello' }, finish_reason: null }] })}\n\ndata: [DONE]\n\n`
                    : JSON.stringify(answer),
                ),
              )
            }),
          )
          const manager = yield* Providers.make({ definitions: { api: Gateway.definition } })
          const instance = yield* manager.add({
            provider: 'api',
            label: 'Gateway',
            config: {
              baseUrl: 'https://gateway.example/v1',
              authentication: 'optional',
              ...(format === 'openai' ? {} : { reasoningFormat: format }),
              models: [
                {
                  ...info,
                  capabilities: { ...info.capabilities!, images: true, reasoning: true },
                  reasoning: { effort: { values: ['low', 'high'], default: 'low' } },
                },
              ],
            },
          })
          const model = yield* manager.models
            .resolve({
              instanceId: instance.id,
              modelId: info.id,
              options: { reasoning: { effort: 'high' } },
            })
            .pipe(Effect.provideService(HttpClient.HttpClient, client))
          const imageResult = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
            _tag: '@effect-harness/ToolContent',
            content: [
              Prompt.makePart('text', { text: 'Screenshot' }),
              Prompt.makePart('file', {
                data: new Uint8Array([137, 80, 78, 71]),
                mediaType: 'image/png',
                options: { openai: { imageDetail: 'high' } },
              }),
            ],
          })
          const prompt = Prompt.make([
            {
              role: 'assistant',
              content: [
                { type: 'tool-call', id: 'image-call', name: 'screenshot', params: {} },
                { type: 'tool-call', id: 'text-call', name: 'status', params: {} },
              ],
            },
            {
              role: 'tool',
              content: [
                {
                  type: 'tool-result',
                  id: 'image-call',
                  name: 'screenshot',
                  isFailure: false,
                  result: imageResult,
                },
                {
                  type: 'tool-result',
                  id: 'text-call',
                  name: 'status',
                  isFailure: false,
                  result: 'ready',
                },
              ],
            },
          ])
          if (streaming) yield* model.languageModel.streamText({ prompt }).pipe(Stream.runCollect)
          else yield* model.languageModel.generateText({ prompt })
          const body = bodies[0]!
          if (format === 'openai') {
            assert.equal(body.reasoning_effort, 'high')
            assert.isUndefined(body.reasoning)
            yield* Schema.decodeUnknownEffect(OpenAiSchema.CreateChatCompletionRequest, {
              onExcessProperty: 'error',
            })(body)
          } else {
            assert.deepEqual(body.reasoning, { effort: 'high' })
            assert.isUndefined(body.reasoning_effort)
          }
          const messages = body.messages as Array<Schema.JsonObject>
          assert.deepEqual(
            messages.map((message) => message.role),
            ['assistant', 'tool', 'tool', 'user'],
          )
          assert.equal(messages[1]!.tool_call_id, 'image-call')
          assert.deepEqual(messages[2], {
            role: 'tool',
            tool_call_id: 'text-call',
            content: 'ready',
          })
          const media = messages[3]!.content as Array<Schema.JsonObject>
          assert.include(JSON.stringify(media), 'image-call')
          assert.deepEqual(media.slice(-2), [
            { type: 'text', text: 'Screenshot' },
            {
              type: 'image_url',
              image_url: { url: 'data:image/png;base64,iVBORw==', detail: 'high' },
            },
          ])
          assert.notInclude(JSON.stringify(body), '@effect-harness/ToolContent')
          const unsupported = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
            _tag: '@effect-harness/ToolContent',
            content: [
              Prompt.makePart('file', { data: new Uint8Array([1]), mediaType: 'application/pdf' }),
            ],
          })
          const failed = yield* Effect.flip(
            model.languageModel.generateText({
              prompt: Prompt.make([
                {
                  role: 'assistant',
                  content: [{ type: 'tool-call', id: 'pdf', name: 'read', params: {} }],
                },
                {
                  role: 'tool',
                  content: [
                    {
                      type: 'tool-result',
                      id: 'pdf',
                      name: 'read',
                      isFailure: false,
                      result: unsupported,
                    },
                  ],
                },
              ]),
            }),
          )
          assert.equal(failed.reason._tag, 'InvalidRequestError')
          assert.equal(bodies.length, 1)
        }).pipe(Effect.provide(Layer.merge(ProviderStore.layerMemory, MemoryCredentials.layer))),
    )
  }
}
