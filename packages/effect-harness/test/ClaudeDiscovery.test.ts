import { assert, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import type * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Provider from '../src/Provider.js'
import * as Claude from '../src/provider-anthropic/Provider.js'
import * as Catalog from '../src/provider-anthropic/Catalog.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as MemoryCredentialStore from '../src/provider-auth/MemoryCredentialStore.js'
import { binding, credentials, claudeResponse, payload } from './fixtures/SubscriptionProtocol.js'

for (const mode of ['adaptive', 'enabled'] as const) {
  it.effect(
    `newly discovered Claude models support ${mode} thinking and replay without a family allowlist`,
    () =>
      Effect.gen(function* () {
        const modelId = 'claude-future-model'
        const bodies: Schema.JsonObject[] = []
        const headers: string[] = []
        const store = yield* credentials('anthropic')
        const client = HttpClient.make((request) =>
          Effect.sync(() => {
            if (request.method === 'GET')
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  JSON.stringify({
                    id: modelId,
                    display_name: 'Future model',
                    max_tokens: 8192,
                    capabilities: {
                      image_input: { supported: true },
                      structured_outputs: { supported: true },
                      thinking: { supported: true, types: { [mode]: { supported: true } } },
                    },
                  }),
                ),
              )
            bodies.push(payload(request))
            headers.push(request.headers['anthropic-beta']!)
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  ...claudeResponse,
                  model: modelId,
                  content: [
                    { type: 'thinking', thinking: 'reasoning', signature: 'future-signature' },
                    ...claudeResponse.content,
                  ],
                }),
              ),
            )
          }),
        )
        const context = Context.make(Store.CredentialStore, store).pipe(
          Context.add(HttpClient.HttpClient, client),
        )
        const options = { id: 'claude', binding: { ...binding, provider: 'anthropic' } } as const
        const provider = Claude.make(options)
        const selection = {
          modelId,
          options: {
            reasoning: {
              thinking: { mode, ...(mode === 'enabled' ? { budgetTokens: 1024 } : {}) },
            },
          },
        }
        const original = yield* Provider.resolve(provider, selection).pipe(
          Effect.provideContext(context),
        )
        const response = yield* original.languageModel.generateText({ prompt: 'hello' })
        const encoded = yield* original.configuration!.encode(Context.empty())
        const reopened = yield* Provider.resolve(provider, selection).pipe(
          Effect.provideContext(context),
        )
        const restored = yield* reopened.configuration!.restore(encoded)
        yield* reopened.languageModel
          .generateText({
            prompt: Prompt.concat(
              Prompt.concat(Prompt.make('hello'), Prompt.fromResponseParts(response.content)),
              'continue',
            ),
          })
          .pipe(Effect.provideContext(restored))
        assert.equal(bodies.length, 2)
        for (const body of bodies)
          assert.deepEqual(body.thinking, {
            type: mode,
            ...(mode === 'enabled' ? { budget_tokens: 1024 } : {}),
          })
        assert.include(JSON.stringify(bodies[1]?.messages), 'future-signature')
        for (const header of headers) assert.notInclude(header, 'thinking-binding-controls')
        if (mode === 'adaptive') {
          const protectedProvider = Claude.make({
            ...options,
            thinkingPolicy: (info) => {
              assert.equal(info.id, modelId)
              return 'server-drop'
            },
          })
          const protectedModel = yield* Provider.resolve(protectedProvider, selection).pipe(
            Effect.provideContext(context),
          )
          assert.equal(
            (yield* Effect.flip(protectedModel.configuration!.restore(encoded))).reason._tag,
            'ModelUnsupportedError',
          )
          yield* protectedModel.languageModel.generateText({ prompt: 'hello' })
          assert.deepEqual(bodies[2]?.thinking, {
            type: 'adaptive',
            block_binding: { prefix_mismatch_behavior: 'drop_block' },
          })
          assert.include(headers[2]!, 'thinking-binding-controls-2026-08-01')
        }
      }),
  )
}

it.effect(
  'OAuth discovery refreshes credentials, pages lazily, and never falls back after HTTP failure',
  () =>
    Effect.gen(function* () {
      const store = yield* MemoryCredentialStore.make
      const account = { ...binding, provider: 'anthropic' } as const
      yield* Store.save({
        type: 'oauth',
        binding: account,
        accessToken: Redacted.make('expired'),
        refreshToken: Redacted.make('refresh'),
        expiresAt: 0,
      }).pipe(Effect.provideService(Store.CredentialStore, store))
      const paths: string[] = []
      const tokens: Array<string | undefined> = []
      let fail = false
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          paths.push(request.url)
          if (request.url.endsWith('/oauth/token'))
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  access_token: 'fresh',
                  refresh_token: 'rotated',
                  expires_in: 3600,
                }),
              ),
            )
          tokens.push(request.headers.authorization)
          if (fail)
            return HttpClientResponse.fromWeb(
              request,
              new Response('SECRET-remote-body', { status: 500 }),
            )
          const after = request.urlParams.params.some(
            ([key, value]) => key === 'after_id' && value === 'first',
          )
          return HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({
                data: [{ id: after ? 'second' : 'first', display_name: 'Model' }],
                last_id: after ? 'second' : 'first',
                has_more: !after,
              }),
            ),
          )
        }),
      )
      const provider = Claude.make({ id: 'claude', binding: account })
      const context = Context.make(Store.CredentialStore, store).pipe(
        Context.add(HttpClient.HttpClient, client),
      )
      const first = Provider.list(provider, { limit: 1 })
      assert.deepEqual(paths, [])
      const page = yield* first.pipe(Effect.provideContext(context))
      assert.equal(page.nextCursor, 'first')
      assert.equal(paths.length, 2)
      const next = yield* Provider.list(provider, { cursor: page.nextCursor!, limit: 1 }).pipe(
        Effect.provideContext(context),
      )
      assert.deepEqual(next, { items: [{ id: 'second', name: 'Model' }] })
      assert.deepEqual(tokens, ['Bearer fresh', 'Bearer fresh'])
      assert.equal(paths.filter((path) => path.endsWith('/oauth/token')).length, 1)
      fail = true
      const error = yield* Effect.flip(Provider.list(provider).pipe(Effect.provideContext(context)))
      assert.equal(error._tag, 'AiError')
      assert.notInclude(JSON.stringify(error), 'SECRET')
      const wrong = Catalog.oauth({ binding: { ...account, provider: 'openai-codex' } })
      const before = paths.length
      yield* Effect.flip(wrong.list({}).pipe(Effect.provideContext(context)))
      assert.equal(paths.length, before)
    }),
)
