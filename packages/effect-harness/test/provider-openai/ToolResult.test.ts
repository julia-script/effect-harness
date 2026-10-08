// effect-review-allow P9-namespace-alias-equals-module: effect-harness/provider-openai/ToolResult and effect-harness/ToolResult share a basename; ProviderToolResult distinguishes the provider boundary.
import * as ToolResult from 'effect-harness/provider-openai/ToolResult'
import { assert, describe, it } from '@effect/vitest'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
// effect-review-allow P9-namespace-alias-equals-module: effect-harness/ToolResult and packages/effect-harness/test/provider-openai/ToolResult.test.ts both bind ToolResult; Canonical distinguishes the concepts.
import * as Canonical from 'effect-harness/ToolResult'
import * as Model from 'effect-harness/Model'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
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
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Catalog from 'effect-harness/provider-openai/Catalog'
import * as ChatGpt from 'effect-harness/provider-openai/ChatGpt'
// effect-review-allow P9-namespace-alias-equals-module: effect-harness/provider-openai/LanguageModel and effect/ai/LanguageModel both bind LanguageModel; Provider distinguishes the concepts.
import * as Provider from 'effect-harness/provider-openai/LanguageModel'

const response = {
  id: 'response',
  model: 'fixture',
  created_at: 0,
  output: [
    {
      type: 'message',
      id: 'message',
      role: 'assistant',
      status: 'completed',
      content: [{ type: 'output_text', text: '{"answer":"ok"}', annotations: [] }],
    },
  ],
  usage: {
    input_tokens: 12,
    output_tokens: 3,
    total_tokens: 15,
    input_tokens_details: { cached_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 0 },
  },
}
const body = (request: HttpClientRequest.HttpClientRequest) => {
  assert.strictEqual(request.body._tag, 'Uint8Array')
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected native JSON body')
  return JSON.parse(new TextDecoder().decode(request.body.body)) as {
    input: Array<{
      type?: string
      role?: string
      content?: unknown
      call_id?: string
      status?: string
      output?: unknown
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
        flow === 'account' || streaming
          ? new Response(
              `data: ${JSON.stringify({ type: 'response.completed', sequence_number: 1, response })}\n\n`,
              { headers: { 'content-type': 'text/event-stream' } },
            )
          : Response.json(response),
      ),
    )
  })
  const auth = Layer.succeed(
    ChatGpt.ChatGpt,
    ChatGpt.ChatGpt.of({
      begin: () => Effect.die('No live consent'),
      complete: () => Effect.die('No live consent'),
      refresh: () => Effect.die('No live refresh'),
      signOut: () => Effect.void,
      cancel: () => Effect.void,
      models: () => Effect.succeed([]),
      accessToken: () => Effect.succeed(Redacted.make('account-token')),
    }),
  )
  const dependencies = Layer.merge(Layer.succeed(HttpClient.HttpClient, http), auth)
  const options = { model: 'fixture', config: { fileIdPrefixes: ['file-'] } }
  const layer = (
    flow === 'apiKey'
      ? Provider.layerApiKey({
          ...options,
          apiKey: Redacted.make('api-key'),
          apiUrl: 'https://fixture.invalid',
        })
      : Provider.layerChatGpt({ ...options, account: 'account' })
  ).pipe(Layer.provide(dependencies))
  return { requests, layer, dependencies }
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
          options: { openai: { status: 'completed' } },
        }),
      ],
    }),
    Prompt.userMessage({ content: [Prompt.textPart({ text: 'continue' })] }),
  ])
const mixed = () =>
  Canonical.encode({
    content: [
      Prompt.textPart({
        text: 'first',
        options: { openai: { promptCacheBreakpoint: { mode: 'explicit' } } },
      }),
      Prompt.filePart({
        mediaType: 'image/png',
        data: new Uint8Array([1, 2, 3]),
        options: { openai: { imageDetail: 'high' } },
      }),
      Prompt.textPart({ text: 'middle' }),
      Prompt.filePart({
        mediaType: 'application/pdf',
        data: new Uint8Array([4, 5]),
        fileName: 'file.pdf',
      }),
      Prompt.filePart({
        mediaType: 'application/pdf',
        data: new URL('https://files.invalid/document.pdf'),
      }),
      Prompt.filePart({
        mediaType: 'image/jpeg',
        data: 'file-image',
        options: { openai: { imageDetail: 'low' } },
      }),
      Prompt.filePart({ mediaType: 'application/pdf', data: 'file-document' }),
      Prompt.textPart({ text: 'last' }),
    ],
    details: { secret: 'private-details' },
    usage: { ...Usage.zero(), input: 987654321 },
    control: { addTools: ['private-control'] },
    diagnostics: [{ kind: 'truncated', message: 'visible warning', severity: 'warning' }],
  })
const output = (request: HttpClientRequest.HttpClientRequest) => {
  const item = body(request).input.find((item) => item.type === 'function_call_output')
  assert.isDefined(item)
  if (item === undefined) throw new Error('Missing native function output')
  return item
}

describe('ToolResult', () => {
  it.effect('curried content keeps empty subjects distinct from empty prefix options', () =>
    Effect.gen(function* () {
      assert.deepStrictEqual(yield* ToolResult.content([]), [])
      assert.deepStrictEqual(yield* ToolResult.content()([]), [])
      assert.deepStrictEqual(yield* ToolResult.content({ prefixes: [] })([]), [])
      const self = [Prompt.filePart({ mediaType: 'image/png', data: 'file_uploaded' })]
      assert.deepStrictEqual(yield* ToolResult.content({ prefixes: ['file_'] })(self), [
        { type: 'input_image', detail: 'auto', file_id: 'file_uploaded' },
      ])
      assert.deepStrictEqual(yield* ToolResult.content(self, ['file_']), [
        { type: 'input_image', detail: 'auto', file_id: 'file_uploaded' },
      ])
    }),
  )

  describe('OpenAI canonical tool media', () => {
    it.effect('retains native toolkit schema/mode/error/service generics', () =>
      Effect.gen(function* () {
        const f = fixture('apiKey')
        class Audit extends Context.Service<
          Audit,
          { readonly record: (value: number) => Effect.Effect<void> }
        >()('test/ToolMedia/Audit') {}
        const convert = Tool.make('convert', {
          parameters: Schema.Struct({ value: Schema.FiniteFromString }),
          success: Schema.String,
          failure: Schema.Literal('failure'),
          dependencies: [Audit],
        })
        const toolkit = Toolkit.make(convert)
        const handlers = toolkit.toLayer({
          convert: ({ value }) =>
            Audit.use((audit) => audit.record(value)).pipe(Effect.as('converted')),
        })
        return yield* Effect.gen(function* () {
          const model = yield* LanguageModel.LanguageModel
          const prompt = history(yield* mixed())
          const withHandlers = yield* toolkit
          const generated = model.generateText({ prompt, toolkit: withHandlers })
          yield* generated.pipe(Effect.provideService(Audit, { record: () => Effect.void }))
          const encoded = model.generateText({ prompt, toolkit, disableToolCallResolution: true })
          yield* encoded
          const request = f.requests[0]
          if (request === undefined) return yield* Effect.die('Missing request')
          assert.isArray(output(request).output)
        }).pipe(Effect.provide(Layer.merge(f.layer, handlers)))
      }),
    )
    for (const flow of ['apiKey', 'account'] as const) {
      it.effect(`${flow} applies native dynamic file-ID configuration to tool media`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const result = yield* Canonical.encode({
              content: [
                Prompt.filePart({ mediaType: 'application/pdf', data: 'asset-document' }),
                Prompt.filePart({ mediaType: 'image/jpeg', data: 'file-image' }),
              ],
            })
            yield* (yield* LanguageModel.LanguageModel)
              .generateText({ prompt: history(result) })
              .pipe(
                Effect.provideService(OpenAiLanguageModel.Config, { fileIdPrefixes: ['asset-'] }),
              )
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('Missing request')
            assert.deepStrictEqual(output(request).output, [
              { type: 'input_file', file_id: 'asset-document' },
              {
                type: 'input_image',
                image_url: 'data:image/jpeg;base64,file-image',
                detail: 'auto',
              },
            ])
          }).pipe(Effect.provide(f.layer))
        }),
      )
      it.effect(`${flow} preserves HTTP/data-URI sources and empty canonical content`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const model = yield* LanguageModel.LanguageModel
            const result = yield* Canonical.encode({
              content: [
                Prompt.filePart({ mediaType: 'image/*', data: 'https://files.invalid/image.jpg' }),
                Prompt.filePart({
                  mediaType: 'application/pdf',
                  data: 'data:application/pdf;base64,BAU=',
                }),
              ],
            })
            yield* model.generateText({ prompt: history(result) })
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('Missing request')
            assert.deepStrictEqual(output(request).output, [
              { type: 'input_image', image_url: 'https://files.invalid/image.jpg', detail: 'auto' },
              {
                type: 'input_file',
                filename: 'part-1.pdf',
                file_data: 'data:application/pdf;base64,BAU=',
              },
            ])
            yield* model.generateText({ prompt: history(yield* Canonical.encode({ content: [] })) })
            const empty = f.requests[1]
            if (empty === undefined) return yield* Effect.die('Missing empty request')
            assert.deepStrictEqual(output(empty).output, [])
          }).pipe(Effect.provide(f.layer))
        }),
      )
      for (const mode of ['text', 'stream', 'object'] as const) {
        it.effect(
          `${flow} ${mode} preserves mixed media/options inside native function output`,
          () =>
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
                  assert.strictEqual(
                    (yield* model.generateText({ prompt })).text,
                    '{"answer":"ok"}',
                  )
                const request = f.requests[0]
                if (request === undefined) return yield* Effect.die('Missing request')
                const item = output(request)
                assert.strictEqual(item.call_id, 'call-1')
                assert.strictEqual(item.status, 'completed')
                assert.deepStrictEqual(item.output, [
                  {
                    type: 'input_text',
                    text: 'first',
                    prompt_cache_breakpoint: { mode: 'explicit' },
                  },
                  { type: 'input_image', image_url: 'data:image/png;base64,AQID', detail: 'high' },
                  { type: 'input_text', text: 'middle' },
                  {
                    type: 'input_file',
                    filename: 'file.pdf',
                    file_data: 'data:application/pdf;base64,BAU=',
                  },
                  { type: 'input_file', file_url: 'https://files.invalid/document.pdf' },
                  { type: 'input_image', file_id: 'file-image', detail: 'low' },
                  { type: 'input_file', file_id: 'file-document' },
                  { type: 'input_text', text: 'last' },
                  { type: 'input_text', text: '<harness>\n[warning] visible warning\n</harness>' },
                ])
                assert.deepStrictEqual(
                  body(request).input.map((item) => item.type ?? item.role),
                  ['user', 'function_call', 'function_call_output', 'user'],
                )
                const encoded = JSON.stringify(body(request))
                assert.notInclude(encoded, 'private-details')
                assert.notInclude(encoded, 'private-control')
                assert.notInclude(encoded, '987654321')
                assert.notInclude(encoded, '@effect-harness/ToolContent')
                assert.strictEqual(
                  request.headers['authorization'],
                  `Bearer ${flow === 'apiKey' ? 'api-key' : 'account-token'}`,
                )
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
              assert.strictEqual(output(request).output, JSON.stringify(result))
            }
          }).pipe(Effect.provide(f.layer))
        }),
      )
      it.effect(`${flow} rejects unsupported media before HTTP`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          return yield* Effect.gen(function* () {
            const result = yield* Canonical.encode({
              content: [Prompt.filePart({ mediaType: 'audio/wav', data: new Uint8Array([1]) })],
            })
            const model = yield* LanguageModel.LanguageModel
            const error = yield* model.generateText({ prompt: history(result) }).pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'InvalidRequestError')
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
                  type: 'file',
                  mediaType: 'image/png',
                  data: 'AQID',
                  options: { openai: { imageDetail: 'invalid' } },
                },
              ],
            }
            const error = yield* (yield* LanguageModel.LanguageModel)
              .generateText({ prompt: history(malformed) })
              .pipe(Effect.flip)
            assert.strictEqual(error.reason._tag, 'InvalidRequestError')
            assert.strictEqual(f.requests.length, 0)
          }).pipe(Effect.provide(f.layer))
        }),
      )
      it.effect(`${flow} catalogue retains media translation and declared file prefixes`, () =>
        Effect.gen(function* () {
          const f = fixture(flow)
          const catalog = (
            flow === 'apiKey'
              ? Catalog.layerApiKey({
                  apiKey: Redacted.make('api-key'),
                  models: [
                    {
                      modelId: 'fixture',
                      contextWindow: 10000,
                      maxOutputTokens: 1000,
                      config: { fileIdPrefixes: ['file-'] },
                    },
                  ],
                })
              : Catalog.layerChatGpt({
                  account: 'account',
                  models: [
                    {
                      modelId: 'fixture',
                      contextWindow: 10000,
                      maxOutputTokens: 1000,
                      config: { fileIdPrefixes: ['file-'] },
                    },
                  ],
                })
          ).pipe(Layer.provide(f.dependencies))
          return yield* Effect.gen(function* () {
            const descriptor = yield* (yield* Model.Catalog).resolve({
              provider: flow === 'apiKey' ? 'openai' : 'openai-chatgpt',
              modelId: 'fixture',
            })
            yield* descriptor.model.generateText({ prompt: history(yield* mixed()) })
            const request = f.requests[0]
            if (request === undefined) return yield* Effect.die('Missing request')
            assert.isArray(output(request).output)
            assert.include(JSON.stringify(output(request).output), '"file_id":"file-image"')
          }).pipe(Effect.provide(catalog))
        }),
      )
    }
  })
})
