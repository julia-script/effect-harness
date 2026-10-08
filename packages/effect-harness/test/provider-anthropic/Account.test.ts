import { assert, describe, it } from '@effect/vitest'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import {
  AuthExpiredError,
  AuthNetworkError,
  AuthPermissionError,
  AuthStorageError,
  AuthTokenError,
  AuthError,
} from 'effect-harness/auth/Credential'
import * as Model from 'effect-harness/Model'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Account from 'effect-harness/provider-anthropic/Account'
import * as OAuth from 'effect-harness/provider-anthropic/OAuth'
import * as Catalog from 'effect-harness/provider-anthropic/Catalog'

const message = {
  id: 'msg_test',
  type: 'message',
  role: 'assistant',
  model: 'declared-model',
  content: [{ type: 'tool_use', id: 'call_new', name: 'Read', input: { path: 'answer' } }],
  stop_reason: 'tool_use',
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
const read = Tool.make('read', {
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
})
const toolkit = Toolkit.make(read)
const history = Prompt.fromMessages([
  Prompt.makeMessage('system', { content: 'Saved instructions' }),
  Prompt.makeMessage('user', {
    content: [
      Prompt.textPart({ text: 'First question' }),
      Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
    ],
  }),
  Prompt.makeMessage('assistant', {
    content: [
      Prompt.reasoningPart({
        text: 'Saved reasoning',
        options: { anthropic: { info: { type: 'thinking', signature: 'saved-signature' } } },
      }),
      Prompt.reasoningPart({
        text: '',
        options: {
          anthropic: { info: { type: 'redacted_thinking', redactedData: 'encrypted-thinking' } },
        },
      }),
      Prompt.textPart({ text: 'Prior response' }),
      Prompt.toolCallPart({
        id: 'call_old',
        name: 'read',
        params: { path: 'old', nested: { type: 'tool_use', name: 'read' } },
        providerExecuted: false,
      }),
    ],
  }),
  Prompt.makeMessage('tool', {
    content: [
      Prompt.toolResultPart({
        id: 'call_old',
        name: 'read',
        result: { ok: true },
        isFailure: false,
        providerExecuted: false,
      }),
    ],
  }),
  Prompt.makeMessage('user', {
    content: [Prompt.textPart({ text: 'Continue from saved context' })],
  }),
])
const body = (request: HttpClientRequest.HttpClientRequest) => {
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected JSON body')
  return Schema.decodeUnknownSync(Schema.JsonObject)(
    JSON.parse(new TextDecoder().decode(request.body.body)),
  )
}
const fixture = (stream = false, denied: boolean | AuthError = false) => {
  const requests: Array<HttpClientRequest.HttpClientRequest> = []
  let credentials = 0
  const auth = Layer.succeed(OAuth.OAuth, {
    begin: () => Effect.die('No login permitted'),
    complete: () => Effect.die('No callback permitted'),
    refresh: () => Effect.die('No refresh permitted'),
    signOut: () => Effect.void,
    cancel: () => Effect.void,
    accessToken: () =>
      Effect.suspend(() => {
        credentials++
        return denied
          ? Effect.fail(
              denied instanceof AuthError
                ? denied
                : new AuthError({
                    reason: new AuthExpiredError({ message: 'Credential refresh failed' }),
                  }),
            )
          : Effect.succeed(Redacted.make(`private-token-${credentials}`))
      }),
  })
  const frames = [
    {
      type: 'message_start',
      message: {
        ...message,
        content: [],
        stop_reason: null,
        usage: { ...message.usage, output_tokens: 0 },
      },
    },
    {
      type: 'content_block_start',
      index: 0,
      content_block: { type: 'tool_use', id: 'call_new', name: 'Read', input: {} },
    },
    {
      type: 'content_block_delta',
      index: 0,
      delta: { type: 'input_json_delta', partial_json: '{"path":"answer"}' },
    },
    { type: 'content_block_stop', index: 0 },
    {
      type: 'message_delta',
      delta: { stop_reason: 'tool_use', stop_sequence: null },
      usage: {
        output_tokens: 3,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        input_tokens: 11,
        service_tier: 'standard',
      },
    },
    { type: 'message_stop' },
  ]
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
          : Response.json(message),
      ),
    )
  })
  const client = Account.layerClient({
    account: 'my-account',
    apiUrl: 'https://test.example',
    apiVersion: 'custom-version',
    transformClient: (value) =>
      value.pipe(
        HttpClient.mapRequest((request) =>
          request.pipe(
            HttpClientRequest.setHeader('x-transform', 'preserved'),
            HttpClientRequest.setHeader('x-api-key', 'must-be-removed'),
          ),
        ),
      ),
  }).pipe(Layer.provide(Layer.merge(auth, Layer.succeed(HttpClient.HttpClient, http))))
  const layer = AnthropicLanguageModel.layer({
    model: 'declared-model',
    config: {
      max_tokens: 4000,
      thinking: { type: 'enabled', budget_tokens: 1024 },
      cache_control: { type: 'ephemeral', ttl: '1h' },
    },
  }).pipe(Layer.provideMerge(client))
  return { requests, auth, http, client, layer, credentials: () => credentials }
}

describe('Account', () => {
  describe('direct Pi-compatible Anthropic account transport', () => {
    it.effect(
      'native saved multi-turn roles/images/thinking/tool results remain structured and canonical names reverse to the native Toolkit',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          let executions = 0
          return yield* Effect.gen(function* () {
            const response = yield* LanguageModel.generateText({
              prompt: history,
              toolkit,
              disableToolCallResolution: true,
            })
            assert.strictEqual(executions, 0)
            assert.strictEqual(response.toolCalls[0]?.name, 'read')
            assert.deepStrictEqual(response.toolCalls[0]?.params, { path: 'answer' })
            assert.strictEqual(response.usage.inputTokens.total, 11)
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('Missing request')
            assert.strictEqual(request.url, 'https://test.example/v1/messages?beta=true')
            assert.strictEqual(request.headers.authorization, 'Bearer private-token-1')
            assert.strictEqual(request.headers['x-api-key'], undefined)
            assert.strictEqual(request.headers['x-app'], 'cli')
            assert.strictEqual(request.headers['user-agent'], `claude-cli/${Account.cliVersion}`)
            assert.strictEqual(request.headers['anthropic-dangerous-direct-browser-access'], 'true')
            assert.strictEqual(request.headers['anthropic-version'], 'custom-version')
            assert.strictEqual(request.headers['x-transform'], 'preserved')
            for (const beta of Account.betas)
              assert.include(request.headers['anthropic-beta'], beta)
            assert.include(request.headers['anthropic-beta'], 'interleaved-thinking-2025-05-14')
            const sent = body(request)
            assert.deepStrictEqual(sent.system, [
              {
                type: 'text',
                text: Account.identity,
                cache_control: { type: 'ephemeral', ttl: '1h' },
              },
              { type: 'text', text: 'Saved instructions', cache_control: null },
            ])
            assert.deepStrictEqual(sent.thinking, { type: 'enabled', budget_tokens: 1024 })
            assert.deepStrictEqual(sent.messages, [
              {
                role: 'user',
                content: [
                  { type: 'text', text: 'First question', cache_control: null },
                  {
                    type: 'image',
                    cache_control: null,
                    source: { type: 'base64', media_type: 'image/png', data: 'AQID' },
                  },
                ],
              },
              {
                role: 'assistant',
                content: [
                  { type: 'thinking', thinking: 'Saved reasoning', signature: 'saved-signature' },
                  { type: 'redacted_thinking', data: 'encrypted-thinking' },
                  { type: 'text', text: 'Prior response' },
                  {
                    type: 'tool_use',
                    id: 'call_old',
                    name: 'Read',
                    input: { path: 'old', nested: { type: 'tool_use', name: 'read' } },
                  },
                ],
              },
              {
                role: 'user',
                content: [
                  {
                    type: 'tool_result',
                    tool_use_id: 'call_old',
                    content: JSON.stringify({ ok: true }),
                    is_error: false,
                    cache_control: null,
                  },
                  { type: 'text', text: 'Continue from saved context', cache_control: null },
                ],
              },
            ])
            assert.strictEqual(f.credentials(), 1)
          }).pipe(
            Effect.provide(
              Layer.merge(
                f.layer,
                toolkit.toLayer({
                  read: () =>
                    Effect.sync(() => {
                      executions++
                      return 'must-not-execute'
                    }),
                }),
              ),
            ),
          )
        }),
    )
    it.effect(
      'streamed partial tool-use aliases are reversed before native Effect AI schema/tool intent processing',
      () =>
        Effect.gen(function* () {
          const f = fixture(true)
          return yield* Effect.gen(function* () {
            const parts = yield* LanguageModel.streamText({
              prompt: history,
              toolkit,
              disableToolCallResolution: true,
            }).pipe(Stream.runCollect)
            const tool = parts.find((part) => part.type === 'tool-call')
            assert.strictEqual(tool?.name, 'read')
            assert.deepStrictEqual(tool?.params, { path: 'answer' })
            const start = parts.find((part) => part.type === 'tool-params-start')
            assert.strictEqual(start?.name, 'read')
            const finish = parts.find((part) => part.type === 'finish')
            assert.strictEqual(finish?.reason, 'tool-calls')
            assert.strictEqual(finish?.usage.outputTokens.total, 3)
            assert.strictEqual(body(f.requests[0]!).stream, true)
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'public native Catalogue composes with account client and preserves selected model/thinking/cache/session controls',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          const catalog = Catalog.layer({
            models: [
              {
                modelId: 'declared-model',
                contextWindow: 200000,
                maxOutputTokens: 32000,
                thinking: { _tag: 'adaptive' },
                efforts: ['high'],
                cache: true,
              },
            ],
          }).pipe(Layer.provide(f.client))
          return yield* Effect.gen(function* () {
            const descriptor = yield* Model.Catalog.use((catalog) =>
              catalog.resolve({ provider: 'anthropic', modelId: 'declared-model' }),
            )
            const context = yield* descriptor.configure({
              thinking: 'high',
              options: {},
              sessionId: '019a08e0-7c00-7000-8000-000000000001',
              maxTokens: 6000,
              cache: 'long',
            })
            yield* descriptor.model
              .generateText({ prompt: history, toolkit, disableToolCallResolution: true })
              .pipe(Effect.provideContext(context))
            const sent = body(f.requests[0]!)
            assert.strictEqual(sent.model, 'declared-model')
            assert.strictEqual(sent.max_tokens, 6000)
            assert.deepStrictEqual(sent.thinking, { type: 'adaptive' })
            assert.deepStrictEqual(sent.output_config, { effort: 'high' })
            assert.deepStrictEqual(sent.metadata, {
              user_id: '019a08e0-7c00-7000-8000-000000000001',
            })
          }).pipe(Effect.provide(catalog))
        }),
    )
    it.effect('typed credential failure occurs before inference and is redacted', () =>
      Effect.gen(function* () {
        const f = fixture(false, true)
        return yield* Effect.gen(function* () {
          const error = yield* LanguageModel.generateText({ prompt: 'Hello' }).pipe(Effect.flip)
          assert.strictEqual(error.reason._tag, 'AuthenticationError')
          assert.isFalse(JSON.stringify(error).includes('private-token'))
          assert.strictEqual(f.requests.length, 0)
        }).pipe(Effect.provide(f.layer))
      }),
    )
    it.effect(
      'canonical alias collisions reject before transport while forced choices preserve custom schema and caller beta headers',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const client = yield* AnthropicClient.AnthropicClient
            const payload = {
              model: 'declared-model',
              max_tokens: 2000,
              messages: [{ role: 'user', content: 'Hello' }],
              tools: [
                { name: 'read', input_schema: { type: 'object' } },
                { name: 'Read', input_schema: { type: 'object' } },
              ],
            } as const
            const error = yield* client.createMessage({ payload }).pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'InvalidRequestError')
            assert.strictEqual(f.requests.length, 0)
            yield* client.createMessage({
              payload: {
                ...payload,
                tools: [payload.tools[0]],
                tool_choice: { type: 'tool', name: 'read' },
              },
              params: { 'anthropic-beta': 'caller-beta,oauth-2025-04-20' },
            })
            assert.deepStrictEqual(body(f.requests[0]!).tool_choice, { type: 'tool', name: 'Read' })
            assert.strictEqual(
              f.requests[0]?.headers['anthropic-beta'],
              'claude-code-20250219,oauth-2025-04-20,caller-beta',
            )
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'generated native SDK client stays available with refreshed bearer auth per HTTP request',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const client = yield* AnthropicClient.AnthropicClient
            for (let index = 0; index < 2; index++)
              yield* client.client.betaMessagesPost({
                payload: {
                  model: 'declared-model',
                  max_tokens: 2000,
                  messages: [{ role: 'user', content: 'SDK injection' }],
                },
              })
            assert.strictEqual(f.credentials(), 2)
            assert.strictEqual(f.requests[0]?.headers.authorization, 'Bearer private-token-1')
            assert.strictEqual(f.requests[1]?.headers.authorization, 'Bearer private-token-2')
            assert.strictEqual(f.requests[0]?.headers['x-api-key'], undefined)
            assert.strictEqual(f.requests[0]?.headers['x-app'], 'cli')
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'raw generated native account client retains its caught AuthError transport cause',
      () =>
        Effect.gen(function* () {
          const original = new AuthError({
            reason: new AuthNetworkError({
              message: 'Sanitized credential failure',
              cause: new Error('private-token-diagnostic'),
            }),
          })
          const f = fixture(false, original)
          return yield* Effect.gen(function* () {
            const client = yield* AnthropicClient.AnthropicClient
            const error = yield* client.client
              .betaMessagesPost({
                payload: {
                  model: 'declared-model',
                  max_tokens: 2000,
                  messages: [{ role: 'user', content: 'Hello' }],
                },
              })
              .pipe(Effect.flip)
            if (!HttpClientError.isHttpClientError(error)) {
              return yield* Effect.die('Expected raw HTTP client failure')
            }
            assert.strictEqual(error.reason._tag, 'TransportError')
            assert.strictEqual(error.reason.cause, original)
            assert.isFalse(JSON.stringify(error).includes('private-token-diagnostic'))
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
    )

    it.effect(
      'native provider-built tool names retain their schema literals; canonicalization applies to custom client tools',
      () =>
        Effect.gen(function* () {
          const f = fixture()
          return yield* Effect.gen(function* () {
            const client = yield* AnthropicClient.AnthropicClient
            yield* client.createMessage({
              payload: {
                model: 'declared-model',
                max_tokens: 2000,
                messages: [{ role: 'user', content: 'Hello' }],
                tools: [{ type: 'bash_20250124', name: 'bash' }],
                tool_choice: { type: 'tool', name: 'bash' },
              },
            })
            assert.deepStrictEqual(body(f.requests[0]!).tools, [
              { type: 'bash_20250124', name: 'bash' },
            ])
            assert.deepStrictEqual(body(f.requests[0]!).tool_choice, { type: 'tool', name: 'bash' })
          }).pipe(Effect.provide(f.layer))
        }),
    )
    it.effect(
      'transient token network, rate limit and server faults remain native retryable reasons before inference',
      () =>
        Effect.forEach(
          [
            new AuthError({
              reason: new AuthNetworkError({ message: 'Token endpoint unavailable' }),
            }),
            new AuthError({
              reason: new AuthTokenError({ message: 'Grant temporarily rejected', status: 429 }),
            }),
            new AuthError({
              reason: new AuthTokenError({ message: 'Grant temporarily rejected', status: 503 }),
            }),
          ],
          (failure) => {
            const f = fixture(false, failure)
            return Effect.gen(function* () {
              const error = yield* LanguageModel.generateText({ prompt: 'Hello' }).pipe(Effect.flip)
              assert.isTrue(error.isRetryable)
              assert.strictEqual(f.requests.length, 0)
              assert.isFalse(JSON.stringify(error).includes('private-token'))
            }).pipe(Effect.provide(f.layer))
          },
        ),
    )
    it.effect(
      'permission and uncertain storage failures remain permanent despite server status',
      () =>
        Effect.forEach(
          [
            new AuthPermissionError({ message: '503 please retry', status: 503 }),
            new AuthStorageError({ message: '503 please retry', status: 503 }),
            new AuthTokenError({ message: 'Invalid grant', status: 401 }),
            new AuthTokenError({ message: 'Unknown status', status: 600 }),
          ],
          (reason) => {
            const f = fixture(false, new AuthError({ reason }))
            return Effect.gen(function* () {
              const error = yield* LanguageModel.generateText({ prompt: 'Hello' }).pipe(Effect.flip)
              assert.strictEqual(error.reason._tag, 'AuthenticationError')
              assert.isFalse(error.isRetryable)
              assert.strictEqual(f.requests.length, 0)
            }).pipe(Effect.provide(f.layer))
          },
        ),
    )
  })

  it.effect(
    'generated nested tool references rename while opaque tool input protocol-shaped values remain untouched',
    () =>
      Effect.gen(function* () {
        const f = fixture()
        const opaque = {
          type: 'tool_use',
          name: 'read',
          input: { type: 'tool_reference', tool_name: 'read' },
          content: [{ type: 'tool_reference', tool_name: 'read' }],
        }
        return yield* Effect.gen(function* () {
          const client = yield* AnthropicClient.AnthropicClient
          yield* client.createMessage({
            payload: {
              model: 'declared-model',
              max_tokens: 2000,
              tools: [{ name: 'read', input_schema: { type: 'object' } }],
              messages: [
                {
                  role: 'assistant',
                  content: [{ type: 'tool_use', id: 'call', name: 'read', input: opaque }],
                },
                {
                  role: 'user',
                  content: [
                    {
                      type: 'tool_result',
                      tool_use_id: 'call',
                      content: [
                        { type: 'tool_reference', tool_name: 'read' },
                        { type: 'text', text: 'read' },
                      ],
                    },
                  ],
                },
              ],
            },
          })
          const sent = body(f.requests[0]!)
          assert.deepStrictEqual(sent.messages, [
            {
              role: 'assistant',
              content: [{ type: 'tool_use', id: 'call', name: 'Read', input: opaque }],
            },
            {
              role: 'user',
              content: [
                {
                  type: 'tool_result',
                  tool_use_id: 'call',
                  content: [
                    { type: 'tool_reference', tool_name: 'Read' },
                    { type: 'text', text: 'read' },
                  ],
                },
              ],
            },
          ])
        }).pipe(Effect.provide(f.layer))
      }),
  )
})
