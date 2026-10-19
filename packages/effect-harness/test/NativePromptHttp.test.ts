import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as OpenAi from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as Anthropic from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Record from 'effect-harness/Record'
import * as Storage from 'effect-harness/Storage'

for (const provider of ['openai', 'anthropic'] as const) {
  it.live(
    `${provider}: native HTTP serialization preserves accepted image bytes, URLs and options on cold retry`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
          const storage = Storage.layerJsonl({ filePath: `${directory}/http.jsonl` })
          const entered = yield* Deferred.make<void>()
          const requests: Schema.JsonObject[] = []
          const transport = HttpClient.make((request) =>
            Effect.gen(function* () {
              if (request.body._tag !== 'Uint8Array')
                return yield* Effect.die('Expected native JSON body')
              requests.push(
                yield* Schema.decodeEffect(Schema.fromJsonString(Schema.JsonObject))(
                  new TextDecoder().decode(request.body.body),
                ).pipe(Effect.orDie),
              )
              if (requests.length === 1) {
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never
              }
              return HttpClientResponse.fromWeb(
                request,
                new globalThis.Response(
                  JSON.stringify({
                    type: 'error',
                    error: {
                      type: 'invalid_request_error',
                      message: 'Intentional test rejection after wire capture',
                      code: 'invalid_request_error',
                    },
                  }),
                  { status: 400, headers: { 'content-type': 'application/json' } },
                ),
              )
            }),
          )
          const native: Layer.Layer<LanguageModel.LanguageModel, never, HttpClient.HttpClient> =
            provider === 'openai'
              ? OpenAi.layerApiKey({
                  model: 'fixture',
                  apiKey: Redacted.make('fixture'),
                  config: { temperature: 0.25, max_output_tokens: 321 },
                })
              : Anthropic.layerApiKey({
                  model: 'fixture',
                  apiKey: Redacted.make('fixture'),
                  config: { temperature: 0.25, max_tokens: 321 },
                })
          const open = Effect.gen(function* () {
            const store = yield* Layer.build(Layer.fresh(storage))
            const model = yield* Layer.build(
              native.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, transport))),
            )
            return yield* HarnessRuntime.make().pipe(
              Effect.provideContext(Context.merge(store, model)),
            )
          })
          const owner = yield* Scope.make()
          const first = yield* open.pipe(Effect.provideService(Scope.Scope, owner))
          yield* first.backend.root
          const job = yield* first.backend.submit({
            conversationId: Record.ROOT_CONVERSATION_ID,
            draft: {
              type: 'input',
              content: [
                Prompt.filePart({
                  mediaType: 'image/png',
                  data: new Uint8Array([1, 2, 3]),
                  options: {
                    openai: { imageDetail: 'high' },
                    anthropic: { cacheControl: { type: 'ephemeral' } },
                  },
                }),
                Prompt.filePart({
                  mediaType: 'image/png',
                  data: new URL('https://example.com/screen.png'),
                  options: { openai: { imageDetail: 'low' } },
                }),
                Prompt.filePart({ mediaType: 'image/png', data: 'BAUG' }),
                Prompt.textPart({ text: 'Caption after images.' }),
              ],
            },
          })
          yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
          yield* Scope.close(owner, Exit.void)
          const second = yield* open
          assert.strictEqual(
            (yield* second.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))).status,
            'unanswered',
          )
          assert.lengthOf(requests, 2)
          assert.deepEqual(requests[1], requests[0])
          const request = requests[0]!
          assert.strictEqual(request.temperature, 0.25)
          assert.strictEqual(request.stream, true)
          if (provider === 'openai') {
            assert.strictEqual(request.max_output_tokens, 321)
            assert.deepEqual(request.input, [
              { role: 'system', content: [{ type: 'input_text', text: '' }] },
              {
                role: 'user',
                content: [
                  { type: 'input_image', image_url: 'data:image/png;base64,AQID', detail: 'high' },
                  {
                    type: 'input_image',
                    image_url: 'https://example.com/screen.png',
                    detail: 'low',
                  },
                  { type: 'input_image', image_url: 'data:image/png;base64,BAUG', detail: 'auto' },
                  { type: 'input_text', text: 'Caption after images.' },
                ],
              },
            ])
          } else {
            assert.strictEqual(request.max_tokens, 321)
            assert.deepEqual(request.messages, [
              {
                role: 'user',
                content: [
                  {
                    type: 'image',
                    source: { type: 'base64', media_type: 'image/png', data: 'AQID' },
                    cache_control: { type: 'ephemeral' },
                  },
                  {
                    type: 'image',
                    cache_control: null,
                    source: { type: 'url', url: 'https://example.com/screen.png' },
                  },
                  {
                    type: 'image',
                    cache_control: null,
                    source: { type: 'base64', media_type: 'image/png', data: 'BAUG' },
                  },
                  { type: 'text', text: 'Caption after images.', cache_control: null },
                ],
              },
            ])
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}
