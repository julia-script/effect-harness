import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as OpenAi from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as Anthropic from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
import * as Storage from 'effect-harness/Storage'
import * as Record from 'effect-harness/Record'
import * as Wire from 'effect-harness/PromptWire'
import * as ToolResult from 'effect-harness/ToolResult'

for (const provider of ['openai', 'anthropic'] as const)
  it.live(`${provider}: actual native HTTP payload is lossless after closed JSONL restore`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-provider-' })
        const layer = Storage.layerJsonl({ filePath: `${directory}/journal` })
        const bytes = new Uint8Array([0, 255, 127, 128, 1, 254])
        const file = Prompt.filePart({
          mediaType: 'image/png',
          data: bytes,
          fileName: 'exact-name.png',
          options: {
            openai: { imageDetail: 'high' },
            anthropic: { cacheControl: { type: 'ephemeral' } },
          },
        })
        const url = Prompt.filePart({
          mediaType: 'image/jpeg',
          data: new URL('https://never-fetch.invalid/image?part=1'),
        })
        const result = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.Envelope))({
          _tag: '@effect-harness/ToolContent',
          content: [Prompt.textPart({ text: 'caption' }), file],
        })
        const prompt = Prompt.fromMessages([
          Prompt.userMessage({ content: [Prompt.textPart({ text: 'inspect' }), file, url] }),
          Prompt.assistantMessage({
            content: [
              Prompt.toolCallPart({
                id: 'call-image',
                name: 'image',
                providerExecuted: false,
                params: { marker: { _tag: '@effect-harness/Bytes', data: 'opaque' } },
              }),
            ],
          }),
          Prompt.toolMessage({
            content: [
              Prompt.toolResultPart({
                id: 'call-image',
                name: 'image',
                result,
                providerExecuted: false,
                isFailure: true,
              }),
            ],
          }),
        ])
        const wire = yield* Schema.encodeEffect(Schema.toCodecJson(Wire.Prompt))(prompt)
        yield* Effect.gen(function* () {
          yield* (yield* Storage.Storage).commit([
            { _tag: 'conversation', value: { id: Record.ROOT_CONVERSATION_ID } },
            {
              _tag: 'document.create',
              record: {
                id: Record.DocumentId.make(2),
                kind: 'harness.request',
                scope: { _tag: 'session' },
              },
              content: { _tag: 'base', version: 1, value: { request: { prompt: wire } } },
            },
          ])
        }).pipe(Effect.provide(layer))
        const restored = yield* Effect.gen(function* () {
          const value = Option.getOrThrow(
            yield* (yield* Storage.Storage).document(Record.DocumentId.make(2)),
          ).value
          return (yield* Schema.decodeUnknownEffect(
            Schema.Struct({ request: Schema.Struct({ prompt: Schema.toCodecJson(Wire.Prompt) }) }),
          )(value)).request.prompt
        }).pipe(Effect.provide(layer))
        assert.deepEqual(restored.content, prompt.content)
        const payloads: Array<Schema.Json> = []
        const http = HttpClient.make((request) =>
          Effect.sync(() => {
            if (request.body._tag !== 'Uint8Array') throw new Error('Expected native JSON request')
            payloads.push(JSON.parse(new TextDecoder().decode(request.body.body)) as Schema.Json)
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  error: {
                    type: 'invalid_request_error',
                    message: 'Synthetic transport stops after request capture',
                  },
                }),
                { status: 400, headers: { 'content-type': 'application/json' } },
              ),
            )
          }),
        )
        const model =
          provider === 'openai'
            ? yield* OpenAi.make({ model: 'gpt-4.1' }).pipe(
                Effect.provideService(
                  OpenAiClient.OpenAiClient,
                  yield* OpenAiClient.make({}).pipe(
                    Effect.provideService(HttpClient.HttpClient, http),
                  ),
                ),
              )
            : yield* Anthropic.make({ model: 'claude-sonnet-4-5' }).pipe(
                Effect.provideService(
                  AnthropicClient.AnthropicClient,
                  yield* AnthropicClient.make({}).pipe(
                    Effect.provideService(HttpClient.HttpClient, http),
                  ),
                ),
              )
        yield* model.generateText({ prompt }).pipe(Effect.result)
        yield* model.generateText({ prompt: restored }).pipe(Effect.result)
        assert.strictEqual(payloads.length, 2)
        assert.deepEqual(payloads[1], payloads[0])
        const json = JSON.stringify(payloads[1])
        assert.include(json, 'AP9/gAH+')
        assert.include(json, 'https://never-fetch.invalid/image?part=1')
        assert.include(json, 'call-image')
        assert.include(json, 'caption')
        if (provider === 'anthropic') assert.include(json, '"is_error":true')
        else assert.include(json, '"detail":"high"')
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
