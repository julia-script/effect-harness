import { assert, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Claude from '../src/provider-anthropic/OAuthModel.js'
import * as Native from '../src/provider-anthropic/OAuthLanguageModel.js'
import { binding, credentials, claudeResponse, payload } from './fixtures/SubscriptionProtocol.js'

it.effect(
  'Claude restores mismatch handling after prefix/tool changes and fences changed policy capabilities',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('anthropic')
      const bodies: Schema.JsonObject[] = []
      const headers: string[] = []
      const model = 'claude-sonnet-5-5'
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          bodies.push(payload(request))
          headers.push(request.headers['anthropic-beta']!)
          return HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({
                ...claudeResponse,
                model,
                content: [
                  { type: 'thinking', thinking: 'reasoning', signature: 'signed-thinking' },
                  { type: 'redacted_thinking', data: 'opaque-thinking' },
                  ...claudeResponse.content,
                ],
              }),
            ),
          )
        }),
      )
      const make = (thinkingModes: ReadonlyArray<'adaptive' | 'enabled'>) =>
        Claude.make({
          model,
          binding: { ...binding, provider: 'anthropic' },
          thinkingModes,
          config: { thinking: { type: 'adaptive' } },
          definition: {
            ref: { provider: 'anthropic', modelId: model },
            capabilities: { tools: true, images: false, reasoning: true, structuredOutput: false },
          },
        }).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(HttpClient.HttpClient, http),
        )
      const original = yield* make(['adaptive'])
      const response = yield* original.languageModel.generateText({ prompt: 'Original prefix' })
      const encoded = yield* original.configuration!.encode(
        Context.make(Native.Config, { thinking: { type: 'adaptive' } }),
      )
      const reopened = yield* make(['adaptive'])
      const restored = yield* reopened.configuration!.restore(JSON.parse(JSON.stringify(encoded)))
      const messages = yield* Schema.decodeEffect(Schema.toCodecJson(Prompt.Prompt))(
        yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.Prompt))(
          Prompt.fromResponseParts(response.content),
        ),
      )
      yield* reopened.languageModel
        .generateText({
          prompt: Prompt.fromMessages([
            Prompt.userMessage({ content: [Prompt.textPart({ text: 'Rewritten prefix' })] }),
            ...messages.content,
            Prompt.userMessage({ content: [Prompt.textPart({ text: 'Continue' })] }),
          ]),
          toolkit: Toolkit.make(
            Tool.make('added', { parameters: Schema.Struct({}), success: Schema.String }),
          ),
          disableToolCallResolution: true,
        })
        .pipe(Effect.provideContext(restored))
      assert.include(JSON.stringify(bodies[1]), 'signed-thinking')
      assert.include(JSON.stringify(bodies[1]), 'opaque-thinking')
      assert.include(JSON.stringify(bodies[1]?.tools), 'added')
      for (const [index, body] of bodies.entries()) {
        assert.deepEqual(body.thinking, {
          type: 'adaptive',
          display: 'summarized',
          block_binding: { prefix_mismatch_behavior: 'drop_block' },
        })
        assert.include(headers[index]!, 'thinking-binding-controls-2026-08-01')
      }
      const changed = yield* make(['enabled'])
      assert.strictEqual(
        (yield* Effect.flip(changed.configuration!.restore(encoded))).reason._tag,
        'ModelUnsupportedError',
      )
      const disabled = yield* Effect.flip(
        reopened.languageModel
          .generateText({ prompt: 'hello' })
          .pipe(Effect.provideService(Native.Config, { thinking: { type: 'disabled' } })),
      )
      assert.strictEqual(disabled.reason._tag, 'InvalidRequestError')
      assert.strictEqual(bodies.length, 2)
    }),
)
