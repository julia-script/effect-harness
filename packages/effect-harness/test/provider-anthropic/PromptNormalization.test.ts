import { assert, describe, it } from '@effect/vitest'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Model from 'effect-harness/Model'
import * as PromptPreparation from 'effect-harness/PromptPreparation'
import * as Config from 'effect/Config'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
// effect-nit-allow P9-namespace-alias-equals-module: effect/ai/Prompt and effect-harness/provider-anthropic/Prompt both bind Prompt; NativePrompt distinguishes the concepts.
import * as NativePrompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'

import * as Catalog from 'effect-harness/provider-anthropic/Catalog'
import * as OAuth from 'effect-harness/provider-anthropic/OAuth'
import * as Prompt from 'effect-harness/provider-anthropic/Prompt'

const base = NativePrompt.systemMessage({
  content: 'base instructions',
  options: { anthropic: { cacheControl: { type: 'ephemeral', ttl: '1h' } } },
})
const later = NativePrompt.systemMessage({ content: 'later plain instructions' })
const hook = NativePrompt.systemMessage({ content: 'hook-added instructions' })
const user = NativePrompt.userMessage({
  content: [
    NativePrompt.textPart({ text: 'first question' }),
    NativePrompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
    NativePrompt.filePart({
      mediaType: 'application/pdf',
      data: new Uint8Array([4, 5]),
      fileName: 'original.pdf',
      options: {
        anthropic: {
          documentTitle: 'document title',
          documentContext: 'original context',
          citations: { enabled: true },
        },
      },
    }),
  ],
})
const assistant = NativePrompt.assistantMessage({
  content: [
    NativePrompt.reasoningPart({
      text: 'saved thinking',
      options: { anthropic: { info: { type: 'thinking', signature: 'signature' } } },
    }),
    NativePrompt.reasoningPart({
      text: '',
      options: { anthropic: { info: { type: 'redacted_thinking', redactedData: 'encrypted' } } },
    }),
    NativePrompt.textPart({ text: 'prior answer' }),
    NativePrompt.toolCallPart({
      id: 'old-call',
      name: 'read',
      params: { path: 'original' },
      providerExecuted: false,
    }),
  ],
})
const result = NativePrompt.toolMessage({
  content: [
    NativePrompt.toolResultPart({
      id: 'old-call',
      name: 'read',
      result: { contents: 'original result' },
      isFailure: false,
      providerExecuted: false,
    }),
  ],
})
const next = NativePrompt.userMessage({ content: [NativePrompt.textPart({ text: 'continue' })] })
const history = NativePrompt.fromMessages([base, user, assistant, later, result, next, hook])
const read = Tool.make('read', {
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(read)
class Audit extends Context.Service<
  Audit,
  { readonly record: (value: number) => Effect.Effect<void> }
>()('test/PromptNormalization/Audit') {}
const convert = Tool.make('convert', {
  parameters: Schema.Struct({ value: Schema.FiniteFromString }),
  success: Schema.String,
  failure: Schema.Literal('tool-failure'),
  dependencies: [Audit],
})
const convertToolkit = Toolkit.make(convert)
const response = {
  id: 'message',
  type: 'message',
  role: 'assistant',
  model: 'declared-model',
  content: [{ type: 'text', text: '{"answer":"ok"}' }],
  stop_reason: 'end_turn',
  stop_sequence: null,
  usage: {
    input_tokens: 17,
    output_tokens: 6,
    cache_creation: null,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    service_tier: 'standard',
  },
}
const frames = [
  {
    type: 'message_start',
    message: {
      ...response,
      content: [],
      stop_reason: null,
      usage: { ...response.usage, output_tokens: 0 },
    },
  },
  { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
  { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'stream answer' } },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn', stop_sequence: null },
    usage: {
      input_tokens: 17,
      output_tokens: 6,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
    },
  },
  { type: 'message_stop' },
]
const bodyUnsafe = (request: HttpClientRequest.HttpClientRequest) => {
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected JSON request')
  return Schema.decodeUnknownSync(Schema.JsonObject)(
    JSON.parse(new TextDecoder().decode(request.body.body)),
  )
}
const options = {
  model: 'declared-model',
  apiUrl: 'https://fixture.example',
  apiVersion: 'fixture-version',
  config: {
    max_tokens: 4321,
    temperature: 0.2,
    thinking: { type: 'disabled' },
    structuredOutputs: true,
    midConversationSystemMessages: false,
  },
  transformClient: (client: HttpClient.HttpClient) =>
    client.pipe(HttpClient.mapRequest(HttpClientRequest.setHeader('x-transform', 'retained'))),
} as const
const fixture = (flow: 'apiKey' | 'account', stream = false) => {
  const requests: HttpClientRequest.HttpClientRequest[] = []
  const http = HttpClient.make((request) => {
    requests.push(request)
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        stream
          ? new Response(
              frames
                .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
                .join(''),
              { headers: { 'content-type': 'text/event-stream' } },
            )
          : Response.json(response),
      ),
    )
  })
  const auth = Layer.succeed(
    OAuth.OAuth,
    OAuth.OAuth.of({
      begin: () => Effect.die('No actual login'),
      complete: () => Effect.die('No actual consent'),
      refresh: () => Effect.die('No actual refresh'),
      signOut: () => Effect.void,
      cancel: () => Effect.void,
      accessToken: () => Effect.succeed(Redacted.make('fixture-bearer')),
    }),
  )
  const dependencies = Layer.merge(Layer.succeed(HttpClient.HttpClient, http), auth)
  const client = (
    flow === 'apiKey'
      ? AnthropicClient.layer({ ...options, apiKey: Redacted.make('fixture-key') })
      : AnthropicAccountClient.layer({ ...options, account: 'fixture-account' })
  ).pipe(Layer.provide(dependencies))
  const layer = (
    flow === 'apiKey'
      ? HarnessAnthropicLanguageModel.layerApiKey({
          ...options,
          apiKey: Redacted.make('fixture-key'),
        })
      : AnthropicAccountLanguageModel.layer({ ...options, account: 'fixture-account' })
  ).pipe(Layer.provide(dependencies))
  return { requests, dependencies, client, layer }
}
const requestAtUnsafe = (
  requests: ReadonlyArray<HttpClientRequest.HttpClientRequest>,
  index = 0,
) => {
  const request = requests[index]
  assert.isDefined(request)
  if (request === undefined) throw new Error('Missing captured request')
  return request
}
const systems = (
  flow: 'apiKey' | 'account',
  texts = ['base instructions', 'later plain instructions', 'hook-added instructions'],
) => [
  ...(flow === 'account' ? [{ type: 'text', text: AnthropicAccountClient.identity }] : []),
  ...texts.map((text) => ({
    type: 'text',
    text,
    cache_control: text === 'base instructions' ? { type: 'ephemeral', ttl: '1h' } : null,
  })),
]
const expectedMessages = (flow: 'apiKey' | 'account') => [
  {
    role: 'user',
    content: [
      { type: 'text', text: 'first question', cache_control: null },
      {
        type: 'image',
        cache_control: null,
        source: { type: 'base64', media_type: 'image/png', data: 'AQID' },
      },
      {
        type: 'document',
        cache_control: null,
        source: { type: 'base64', media_type: 'application/pdf', data: 'BAU=' },
        title: 'document title',
        context: 'original context',
        citations: { enabled: true },
      },
    ],
  },
  {
    role: 'assistant',
    content: [
      { type: 'thinking', thinking: 'saved thinking', signature: 'signature' },
      { type: 'redacted_thinking', data: 'encrypted' },
      { type: 'text', text: 'prior answer' },
      {
        type: 'tool_use',
        id: 'old-call',
        name: flow === 'account' ? 'Read' : 'read',
        input: { path: 'original' },
      },
    ],
  },
  {
    role: 'user',
    content: [
      {
        type: 'tool_result',
        tool_use_id: 'old-call',
        content: '{"contents":"original result"}',
        is_error: false,
        cache_control: null,
      },
      { type: 'text', text: 'continue', cache_control: null },
    ],
  },
]

describe('PromptNormalization', () => {
  describe('faithful native Anthropic system normalization', () => {
    it.effect(
      'native generic tool parameters, errors, dependencies and streamed intents retain their types',
      () =>
        Effect.gen(function* () {
          const observed: number[] = []
          const captured: NativePrompt.Prompt[] = []
          const parts = [
            {
              type: 'tool-call',
              id: 'convert-call',
              name: 'convert',
              params: { value: '7' },
              providerExecuted: false,
            },
          ] as const
          const handlers = convertToolkit.toLayer({
            convert: ({ value }) =>
              Audit.use((audit) => audit.record(value)).pipe(Effect.as('converted')),
          })
          return yield* Effect.gen(function* () {
            const original = yield* LanguageModel.make({
              generateText: (request) => {
                captured.push(request.prompt)
                return Effect.succeed([...parts])
              },
              streamText: (request) => {
                captured.push(request.prompt)
                return Stream.fromIterable(parts)
              },
            })
            const model = original
            const withHandlers = yield* convertToolkit
            const pending = model.generateText({ prompt: history, toolkit: withHandlers })
            const text = yield* pending
            assert.deepStrictEqual(
              text.toolResults.map((part) => part.result),
              ['converted'],
            )
            assert.deepStrictEqual(observed, [7])
            const intents = model.generateText({
              prompt: history,
              toolkit: convertToolkit,
              disableToolCallResolution: true,
            })
            const intent = yield* intents
            const params = intent.toolCalls[0]?.params
            assert.deepStrictEqual(params, { value: '7' })
            const stream = model.streamText({
              prompt: history,
              toolkit: convertToolkit,
              disableToolCallResolution: true,
            })
            const streamed = yield* Stream.runCollect(stream)
            assert.deepStrictEqual(streamed.find((part) => part.type === 'tool-call')?.params, {
              value: '7',
            })
            assert.deepStrictEqual(observed, [7])
            for (const prompt of captured) assert.deepStrictEqual(prompt.content, history.content)
            const generic = <Tools extends Record<string, Tool.Any>>(
              input: Toolkit.WithHandler<Tools>,
            ) =>
              model.generateText({
                prompt: history,
                toolkit: input,
                disableToolCallResolution: true,
              })
            const genericOutput = yield* generic(withHandlers)
            const genericParams = genericOutput.toolCalls[0]?.params
            assert.deepStrictEqual(genericParams, { value: '7' })
          }).pipe(
            Effect.provide(handlers),
            Effect.provideService(
              Audit,
              Audit.of({
                record: (value) =>
                  Effect.sync(() => {
                    observed.push(value)
                  }),
              }),
            ),
          )
        }),
    )

    it('is idempotent and preserves every message object and native options', () => {
      const normalized = Prompt.normalize(history)
      assert.deepStrictEqual(normalized.content, [base, later, hook, user, assistant, result, next])
      for (const message of history.content) assert.isTrue(normalized.content.includes(message))
      assert.strictEqual(normalized.content[0]?.options, base.options)
      assert.deepStrictEqual(Prompt.normalize(normalized), normalized)
      assert.deepStrictEqual(history.content, [base, user, assistant, later, result, next, hook])
      assert.deepStrictEqual(Prompt.normalize(NativePrompt.empty).content, [])
    })

    it.effect(
      'stable native converter preserves every system group without a normalization facade',
      () =>
        Effect.gen(function* () {
          const f = fixture('apiKey')
          return yield* Effect.gen(function* () {
            yield* LanguageModel.generateText({ prompt: history })
            assert.deepStrictEqual(
              bodyUnsafe(requestAtUnsafe(f.requests)).system,
              systems('apiKey'),
            )
          }).pipe(
            Effect.provide(
              AnthropicLanguageModel.layer({
                model: options.model,
                config: { midConversationSystemMessages: false },
              }).pipe(Layer.provide(f.client)),
            ),
          )
        }),
    )

    it.effect('native system history capability follows scoped configuration', () =>
      Effect.gen(function* () {
        const f = fixture('apiKey')
        const inlineHistory = NativePrompt.fromMessages([base, user, later, assistant])
        return yield* Effect.gen(function* () {
          const model = yield* LanguageModel.LanguageModel
          assert.isDefined(model.supportsSystemMessagesInHistory)
          if (model.supportsSystemMessagesInHistory === undefined)
            return yield* Effect.die('Missing native history capability')
          assert.isFalse(yield* model.supportsSystemMessagesInHistory)
          assert.isTrue(
            yield* model.supportsSystemMessagesInHistory.pipe(
              HarnessAnthropicLanguageModel.withConfigOverride({
                midConversationSystemMessages: true,
              }),
            ),
          )
          yield* LanguageModel.generateText({ prompt: inlineHistory }).pipe(
            HarnessAnthropicLanguageModel.withConfigOverride({
              midConversationSystemMessages: true,
            }),
          )
          const sent = bodyUnsafe(requestAtUnsafe(f.requests))
          assert.deepStrictEqual(sent.system, systems('apiKey', ['base instructions']))
          const messages = yield* Schema.decodeUnknownEffect(
            Schema.Array(Schema.Struct({ role: Schema.String })),
          )(sent.messages)
          assert.deepStrictEqual(
            messages.map((message) => message.role),
            ['user', 'system', 'assistant'],
          )
          assert.isFalse(yield* model.supportsSystemMessagesInHistory)
        }).pipe(Effect.provide(f.layer))
      }),
    )

    it.effect(
      'native encoded message and string input overloads still decode without changing user roles',
      () =>
        Effect.gen(function* () {
          const f = fixture('apiKey')
          return yield* Effect.gen(function* () {
            yield* LanguageModel.generateText({
              prompt: [
                { role: 'system', content: 'encoded base' },
                { role: 'user', content: 'encoded question' },
                { role: 'system', content: 'encoded later' },
              ],
            })
            assert.deepStrictEqual(bodyUnsafe(requestAtUnsafe(f.requests)).system, [
              { type: 'text', text: 'encoded base', cache_control: null },
              { type: 'text', text: 'encoded later', cache_control: null },
            ])
            yield* LanguageModel.generateText({ prompt: 'raw question' })
            const sent = bodyUnsafe(requestAtUnsafe(f.requests, 1))
            assert.strictEqual(sent.system, undefined)
            assert.deepStrictEqual(sent.messages, [
              {
                role: 'user',
                content: [{ type: 'text', text: 'raw question', cache_control: null }],
              },
            ])
          }).pipe(Effect.provide(f.layer))
        }),
    )

    it.effect(
      'unsupported native attachment retains its typed input failure before transport',
      () =>
        Effect.gen(function* () {
          const f = fixture('apiKey')
          return yield* Effect.gen(function* () {
            const error = yield* LanguageModel.generateText({
              prompt: NativePrompt.fromMessages([
                base,
                NativePrompt.userMessage({
                  content: [
                    NativePrompt.filePart({ mediaType: 'audio/wav', data: new Uint8Array([1]) }),
                  ],
                }),
                later,
              ]),
            }).pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'InvalidUserInputError')
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
    )

    for (const flow of ['apiKey', 'account'] as const) {
      it.effect(
        `${flow} exported native Layer sends all system blocks and structured history with original options`,
        () =>
          Effect.gen(function* () {
            const f = fixture(flow)
            return yield* Effect.gen(function* () {
              const output = yield* LanguageModel.generateText({
                prompt: history,
                toolkit,
                disableToolCallResolution: true,
              })
              assert.strictEqual(output.finishReason, 'stop')
              assert.strictEqual(output.usage.inputTokens.total, 17)
              const request = requestAtUnsafe(f.requests)
              const sent = bodyUnsafe(request)
              assert.deepStrictEqual(sent.system, systems(flow))
              assert.deepStrictEqual(sent.messages, expectedMessages(flow))
              assert.strictEqual(sent.model, 'declared-model')
              assert.strictEqual(sent.max_tokens, 4321)
              assert.strictEqual(sent.temperature, 0.2)
              assert.strictEqual(request.url, 'https://fixture.example/v1/messages?beta=true')
              assert.strictEqual(request.headers['anthropic-version'], 'fixture-version')
              assert.strictEqual(request.headers['x-transform'], 'retained')
              assert.strictEqual(
                request.headers.authorization,
                flow === 'account' ? 'Bearer fixture-bearer' : undefined,
              )
              assert.strictEqual(
                request.headers['x-api-key'],
                flow === 'apiKey' ? 'fixture-key' : undefined,
              )
            }).pipe(Effect.provide(f.layer))
          }),
      )

      it.effect(`${flow} native streaming preserves late hook systems and finish usage`, () =>
        Effect.gen(function* () {
          const f = fixture(flow, true)
          return yield* Effect.gen(function* () {
            const parts = yield* LanguageModel.streamText({
              prompt: history,
              toolkit,
              disableToolCallResolution: true,
            }).pipe(Stream.runCollect)
            assert.strictEqual(
              parts.find((part) => part.type === 'finish')?.usage.outputTokens.total,
              6,
            )
            assert.deepStrictEqual(bodyUnsafe(requestAtUnsafe(f.requests)).system, systems(flow))
            assert.deepStrictEqual(
              bodyUnsafe(requestAtUnsafe(f.requests)).messages,
              expectedMessages(flow),
            )
            assert.strictEqual(bodyUnsafe(requestAtUnsafe(f.requests)).stream, true)
          }).pipe(Effect.provide(f.layer))
        }),
      )

      it.effect(
        `${flow} native catalogue applies hook normalization, managed section removal/order and request pinning`,
        () =>
          Effect.gen(function* () {
            const f = fixture(flow)
            const layer = Catalog.layer({
              models: [
                {
                  modelId: 'declared-model',
                  contextWindow: 200000,
                  maxOutputTokens: 32000,
                  cache: true,
                  config: { midConversationSystemMessages: false },
                },
              ],
            }).pipe(Layer.provide(f.client))
            return yield* Effect.gen(function* () {
              const descriptor = yield* Model.Catalog.use((catalog) =>
                catalog.resolve({ provider: 'anthropic', modelId: 'declared-model' }),
              )
              const patch = NativePrompt.systemMessage({
                content: 'obsolete encoded section patch',
              })
              const sections = PromptPreparation.replaySections([
                { sections: { first: 'obsolete first', gone: 'removed', second: 'second' } },
                { sections: { first: null, gone: null } },
                { sections: { first: 'current first' } },
              ])
              const projected = PromptPreparation.toPrompt(
                [base, user, patch, assistant, result, next],
                sections,
                { managedSystemMessages: [patch] },
              )
              const afterHooks = NativePrompt.fromMessages([...projected.content, hook])
              assert.isUndefined(descriptor.normalizePrompt)
              const normalized = afterHooks
              const context = yield* descriptor.configure({
                thinking: 'off',
                options: { temperature: 0.4 },
                sessionId: '019a08e0-7c00-7000-8000-000000000001',
                maxTokens: 6000,
                cache: 'long',
              })
              yield* descriptor.model
                .generateText({ prompt: normalized, toolkit, disableToolCallResolution: true })
                .pipe(
                  Effect.provideContext(context),
                  HarnessAnthropicLanguageModel.withConfigOverride({
                    model: 'must-not-override',
                    max_tokens: 999,
                  }),
                )
              const sent = bodyUnsafe(requestAtUnsafe(f.requests))
              // Account's identity receives request cache settings; each saved block keeps its original options.
              const expectedSystem = systems(flow, [
                'base instructions',
                'second\n\ncurrent first',
                'hook-added instructions',
              ])
              if (flow === 'account')
                expectedSystem.splice(0, 1, {
                  type: 'text',
                  text: AnthropicAccountClient.identity,
                  cache_control: { type: 'ephemeral', ttl: '1h' },
                })
              assert.deepStrictEqual(sent.system, expectedSystem)
              assert.deepStrictEqual(sent.messages, expectedMessages(flow))
              assert.strictEqual(sent.model, 'declared-model')
              assert.strictEqual(sent.max_tokens, 6000)
              assert.strictEqual(sent.temperature, 0.4)
              assert.deepStrictEqual(sent.metadata, {
                user_id: '019a08e0-7c00-7000-8000-000000000001',
              })
            }).pipe(Effect.provide(layer))
          }),
      )

      it.effect(
        `${flow} native structured object generation retains schema validation and system history`,
        () =>
          Effect.gen(function* () {
            const f = fixture(flow)
            return yield* Effect.gen(function* () {
              const output = yield* LanguageModel.generateObject({
                prompt: history,
                schema: Schema.Struct({ answer: Schema.String }),
              })
              assert.deepStrictEqual(output.value, { answer: 'ok' })
              assert.deepStrictEqual(bodyUnsafe(requestAtUnsafe(f.requests)).system, systems(flow))
              assert.deepStrictEqual(
                bodyUnsafe(requestAtUnsafe(f.requests)).messages,
                expectedMessages(flow),
              )
              assert.isDefined(bodyUnsafe(requestAtUnsafe(f.requests)).output_config)
            }).pipe(Effect.provide(f.layer))
          }),
      )
    }

    it.effect(
      'API-key Config Layer and native per-request override remain available after normalization',
      () =>
        Effect.gen(function* () {
          const f = fixture('apiKey')
          const layer = HarnessAnthropicLanguageModel.layerApiKeyConfig({
            model: Config.succeed(options.model),
            config: Config.succeed(options.config),
            transformClient: Config.succeed(options.transformClient),
            apiKey: Config.succeed(Redacted.make('config-key')),
            apiUrl: Config.succeed(options.apiUrl),
            apiVersion: Config.succeed(options.apiVersion),
          }).pipe(Layer.provide(f.dependencies))
          return yield* Effect.gen(function* () {
            yield* LanguageModel.generateText({ prompt: history }).pipe(
              AnthropicLanguageModel.withConfigOverride({
                model: 'native-override',
                max_tokens: 7654,
                temperature: 0.5,
              }),
            )
            const request = requestAtUnsafe(f.requests)
            const sent = bodyUnsafe(request)
            assert.deepStrictEqual(sent.system, systems('apiKey'))
            assert.strictEqual(sent.model, 'native-override')
            assert.strictEqual(sent.max_tokens, 7654)
            assert.strictEqual(sent.temperature, 0.5)
            assert.strictEqual(request.headers['x-api-key'], 'config-key')
            const client = yield* AnthropicClient.AnthropicClient
            assert.isFunction(client.createMessage)
          }).pipe(Effect.provide(layer))
        }),
    )
  })
})

import * as AnthropicAccountClient from 'effect-harness/provider-anthropic/AnthropicAccountClient'
import * as HarnessAnthropicLanguageModel from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
import * as AnthropicAccountLanguageModel from 'effect-harness/provider-anthropic/AnthropicAccountLanguageModel'
