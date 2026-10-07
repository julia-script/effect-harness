import { assert, describe, it } from '@effect/vitest'
import * as ToolResult from '@effect-harness/harness/ToolResult'
import * as Model from '@effect-harness/harness/Model'
import * as Usage from '@effect-harness/harness/Usage'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Account from '@effect-harness/provider-anthropic/Account'
import * as Anthropic from '@effect-harness/provider-anthropic/Anthropic'
import * as Catalog from '@effect-harness/provider-anthropic/Catalog'
import * as OAuth from '@effect-harness/provider-anthropic/OAuth'

const response = {
  id: 'response',
  type: 'message',
  role: 'assistant',
  model: 'fixture',
  content: [{ type: 'text', text: '{"answer":"ok"}' }],
  stop_reason: 'end_turn',
  stop_sequence: null,
  usage: {
    input_tokens: 12,
    output_tokens: 3,
    cache_creation: null,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    service_tier: 'standard',
  },
}
const frames = [
  { type: 'message_start', message: { ...response, content: [], stop_reason: null } },
  { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } },
  { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'ok' } },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn', stop_sequence: null },
    usage: {
      input_tokens: 12,
      output_tokens: 3,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
    },
  },
  { type: 'message_stop' },
]
const body = (request: HttpClientRequest.HttpClientRequest) => {
  assert.strictEqual(request.body._tag, 'Uint8Array')
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected native JSON body')
  return JSON.parse(new TextDecoder().decode(request.body.body)) as {
    messages: Array<{
      role: string
      content: Array<{
        type: string
        tool_use_id?: string
        content?: unknown
        is_error?: boolean
        cache_control?: unknown
      }>
    }>
  }
}
const fixture = (flow: 'apiKey' | 'account', streaming = false) => {
  const requests: HttpClientRequest.HttpClientRequest[] = []
  const http = HttpClient.make((request) => {
    requests.push(request)
    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        streaming
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
      begin: () => Effect.die('No live consent'),
      complete: () => Effect.die('No live consent'),
      refresh: () => Effect.die('No live refresh'),
      signOut: () => Effect.void,
      cancel: () => Effect.void,
      accessToken: () => Effect.succeed(Redacted.make('account-token')),
    }),
  )
  const dependencies = Layer.merge(Layer.succeed(HttpClient.HttpClient, http), auth)
  const options = {
    model: 'fixture',
    apiUrl: 'https://fixture.invalid',
    config: { max_tokens: 1234 },
  }
  const layer = (
    flow === 'apiKey'
      ? Anthropic.layer({ ...options, apiKey: Redacted.make('api-key') })
      : Account.layer({ ...options, account: 'account' })
  ).pipe(Layer.provide(dependencies))
  return { requests, layer }
}
const history = (result: Schema.Json) =>
  Prompt.fromMessages([
    Prompt.userMessage({ content: [Prompt.textPart({ text: 'initial' })] }),
    Prompt.assistantMessage({
      content: [
        Prompt.toolCallPart({ id: 'call-1', name: 'read', params: {}, providerExecuted: false }),
      ],
    }),
    Prompt.toolMessage({
      content: [
        Prompt.toolResultPart({
          id: 'call-1',
          name: 'read',
          result,
          isFailure: true,
          providerExecuted: false,
          options: { anthropic: { cacheControl: { type: 'ephemeral', ttl: '1h' } } },
        }),
      ],
    }),
    Prompt.userMessage({ content: [Prompt.textPart({ text: 'continue' })] }),
  ])
const mixed = () =>
  ToolResult.encode({
    content: [
      Prompt.textPart({
        text: 'first',
        options: { anthropic: { cacheControl: { type: 'ephemeral', ttl: '5m' } } },
      }),
      Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) }),
      Prompt.textPart({ text: 'middle' }),
      Prompt.filePart({
        mediaType: 'application/pdf',
        data: new Uint8Array([4, 5]),
        fileName: 'file.pdf',
        options: {
          anthropic: {
            documentTitle: 'Title',
            documentContext: 'Context',
            citations: { enabled: true },
            cacheControl: { type: 'ephemeral', ttl: '1h' },
          },
        },
      }),
      Prompt.filePart({ mediaType: 'text/plain', data: 'plain document' }),
      Prompt.textPart({ text: 'last' }),
    ],
    details: { secret: 'private-details' },
    usage: { ...Usage.zero(), input: 987654321 },
    control: { addTools: ['private-control'] },
    diagnostics: [{ kind: 'truncated', message: 'visible warning', severity: 'warning' }],
  })
const toolResult = (request: HttpClientRequest.HttpClientRequest) => {
  const found = body(request)
    .messages.flatMap((message) => message.content)
    .find((block) => block.type === 'tool_result')
  assert.isDefined(found)
  if (found === undefined) throw new Error('Missing native tool result')
  return found
}

describe('ToolResult', () => {
  describe('Anthropic canonical tool media', () => {
    for (const flow of ['apiKey', 'account'] as const) {
      it.effect(`${flow} preserves URL and data-URI image/PDF sources`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const result = yield* ToolResult.encode({
              content: [
                Prompt.filePart({
                  mediaType: 'image/jpeg',
                  data: new URL('https://files.invalid/image.jpg'),
                }),
                Prompt.filePart({
                  mediaType: 'application/pdf',
                  data: 'https://files.invalid/file.pdf',
                }),
                Prompt.filePart({ mediaType: 'image/*', data: 'data:image/jpeg;base64,AQID' }),
                Prompt.filePart({
                  mediaType: 'application/pdf',
                  data: 'data:application/pdf;base64,BAU=',
                }),
              ],
            })
            yield* (yield* LanguageModel.LanguageModel).generateText({ prompt: history(result) })
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('Missing request')
            assert.deepStrictEqual(toolResult(request).content, [
              {
                type: 'image',
                source: { type: 'url', url: 'https://files.invalid/image.jpg' },
                cache_control: null,
              },
              {
                type: 'document',
                source: { type: 'url', url: 'https://files.invalid/file.pdf' },
                title: null,
                cache_control: null,
              },
              {
                type: 'image',
                source: { type: 'base64', media_type: 'image/jpeg', data: 'AQID' },
                cache_control: null,
              },
              {
                type: 'document',
                source: { type: 'base64', media_type: 'application/pdf', data: 'BAU=' },
                title: null,
                cache_control: null,
              },
            ])
          }).pipe(Effect.provide(f.layer))
        }),
      )
      for (const mode of ['text', 'stream', 'object'] as const) {
        it.effect(`${flow} ${mode} preserves mixed media/options inside native tool_result`, () =>
          Effect.gen(function* () {
            const f = fixture(flow, mode === 'stream')
            return yield* Effect.gen(function* () {
              const prompt = history(yield* mixed())
              const model = yield* LanguageModel.LanguageModel
              if (mode === 'stream') yield* model.streamText({ prompt }).pipe(Stream.runDrain)
              else if (mode === 'object')
                assert.deepStrictEqual(
                  (yield* model.generateObject({
                    prompt,
                    schema: Schema.Struct({ answer: Schema.String }),
                  })).value,
                  { answer: 'ok' },
                )
              else
                assert.strictEqual((yield* model.generateText({ prompt })).text, '{"answer":"ok"}')
              const request = f.requests[0]
              assert.isDefined(request)
              if (request === undefined) return yield* Effect.die('Missing request')
              const result = toolResult(request)
              assert.strictEqual(result.tool_use_id, 'call-1')
              assert.strictEqual(result.is_error, true)
              assert.deepStrictEqual(result.cache_control, { type: 'ephemeral', ttl: '1h' })
              assert.deepStrictEqual(result.content, [
                { type: 'text', text: 'first', cache_control: { type: 'ephemeral', ttl: '5m' } },
                {
                  type: 'image',
                  source: { type: 'base64', media_type: 'image/png', data: 'AQID' },
                  cache_control: null,
                },
                { type: 'text', text: 'middle', cache_control: null },
                {
                  type: 'document',
                  source: { type: 'base64', media_type: 'application/pdf', data: 'BAU=' },
                  title: 'Title',
                  context: 'Context',
                  citations: { enabled: true },
                  cache_control: { type: 'ephemeral', ttl: '1h' },
                },
                {
                  type: 'document',
                  source: { type: 'text', media_type: 'text/plain', data: 'plain document' },
                  title: null,
                  cache_control: null,
                },
                { type: 'text', text: 'last', cache_control: null },
                {
                  type: 'text',
                  text: '<harness>\n[warning] visible warning\n</harness>',
                  cache_control: null,
                },
              ])
              const encoded = JSON.stringify(body(request))
              assert.notInclude(encoded, 'private-details')
              assert.notInclude(encoded, 'private-control')
              assert.notInclude(encoded, '987654321')
              assert.notInclude(encoded, '@effect-harness/ToolContent')
              assert.include(request.headers['anthropic-beta'] ?? '', 'pdfs-2024-09-25')
              if (flow === 'account') {
                assert.strictEqual(request.headers['authorization'], 'Bearer account-token')
                assert.include(request.headers['anthropic-beta'] ?? '', 'oauth-2025-04-20')
              } else assert.strictEqual(request.headers['x-api-key'], 'api-key')
            }).pipe(Effect.provide(f.layer))
          }),
        )
      }
      it.effect(`${flow} leaves ordinary JSON and invalid lookalike markers native`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const model = yield* LanguageModel.LanguageModel
            for (const result of [
              { value: ['ordinary'] },
              { _tag: '@effect-harness/ToolContent', content: 'invalid' },
            ]) {
              yield* model.generateText({ prompt: history(result) })
              const request = f.requests.at(-1)
              if (request === undefined) return yield* Effect.die('Missing request')
              assert.strictEqual(toolResult(request).content, JSON.stringify(result))
            }
          }).pipe(Effect.provide(f.layer))
        }),
      )
      it.effect(`${flow} rejects unsupported media before HTTP`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const result = yield* ToolResult.encode({
              content: [Prompt.filePart({ mediaType: 'audio/wav', data: new Uint8Array([1]) })],
            })
            const model = yield* LanguageModel.LanguageModel
            const error = yield* model.generateText({ prompt: history(result) }).pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'InvalidUserInputError')
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
      )
      it.effect(`${flow} rejects invalid native provider options before HTTP`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const malformed: Schema.Json = {
              _tag: '@effect-harness/ToolContent',
              content: [
                {
                  type: 'text',
                  text: 'visible',
                  options: { anthropic: { cacheControl: { type: 'ephemeral', ttl: 'invalid' } } },
                },
              ],
            }
            const error = yield* (yield* LanguageModel.LanguageModel)
              .generateText({ prompt: history(malformed) })
              .pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'InvalidUserInputError')
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
      )
    }
    it.effect('catalogue captures the same canonical media client adapter', () =>
      Effect.gen(function* () {
        const f = fixture('apiKey')
        return yield* Effect.gen(function* () {
          const descriptor = yield* (yield* Model.Catalog).resolve({
            provider: 'anthropic',
            modelId: 'fixture',
          })
          yield* descriptor.model.generateText({ prompt: history(yield* mixed()) })
          const request = f.requests[0]
          if (request === undefined) return yield* Effect.die('Missing request')
          assert.isArray(toolResult(request).content)
        }).pipe(
          Effect.provide(
            Catalog.layer({
              models: [{ modelId: 'fixture', contextWindow: 10000, maxOutputTokens: 1000 }],
            }).pipe(Layer.provide(f.layer)),
          ),
        )
      }),
    )
  })
})
