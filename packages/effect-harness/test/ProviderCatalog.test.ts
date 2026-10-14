import { assert, it } from '@effect/vitest'
import * as TestClock from 'effect/testing/TestClock'
import * as CodexCatalog from '../src/provider-openai-codex/Catalog.js'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import type * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Catalog from '../src/ModelCatalog.js'
import * as Provider from '../src/Provider.js'
import * as ModelInfo from '../src/ModelInfo.js'
import * as Codex from '../src/provider-openai-codex/Provider.js'
import * as Claude from '../src/provider-anthropic/Provider.js'
import * as ClaudeCatalog from '../src/provider-anthropic/Catalog.js'
import * as Store from '../src/provider-auth/CredentialStore.js'
import {
  binding,
  credentials,
  codexToken,
  codexEvents,
  claudeResponse,
  payload,
  sse,
} from './fixtures/SubscriptionProtocol.js'

const codexEntry = {
  slug: 'gpt-5.4',
  display_name: 'GPT 5.4',
  visibility: 'list',
  supported_reasoning_levels: [{ effort: 'low' }, { effort: 'high' }, { effort: 'future-effort' }],
  default_reasoning_level: 'low',
  context_window: 272000,
  input_modalities: ['text', 'image'],
}

it.effect(
  'Codex discovers lazily with account auth, filters adapter efforts, and restores a pinned selection',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('openai-codex')
      const gets: string[] = []
      const bodies: Schema.JsonObject[] = []
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          if (request.method === 'GET') {
            gets.push(request.url)
            assert.equal(request.headers.authorization, `Bearer ${codexToken}`)
            assert.equal(request.headers['chatgpt-account-id'], 'account-1')
            assert.deepEqual(request.urlParams.params, [['client_version', '0.99.0']])
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  models: [codexEntry, { ...codexEntry, slug: 'hidden', visibility: 'hide' }],
                }),
              ),
            )
          }
          bodies.push(payload(request))
          return HttpClientResponse.fromWeb(request, new Response(sse(codexEvents)))
        }),
      )
      const context = Context.make(Store.CredentialStore, store).pipe(
        Context.add(HttpClient.HttpClient, client),
      )
      const provider = Codex.make({ id: 'codex-personal', binding, clientVersion: '0.99.0' })
      const catalog = Catalog.make({ providers: [provider] })
      const resolve = Catalog.resolve(catalog, {
        provider: 'codex-personal',
        modelId: 'gpt-5.4',
        options: { reasoning: { effort: 'high' } },
      })
      assert.deepEqual(gets, [])
      assert.deepEqual(
        (yield* Catalog.list(catalog, { provider: 'codex-personal' }).pipe(
          Effect.provideContext(context),
        )).items,
        [{ id: 'gpt-5.4', name: 'GPT 5.4' }],
      )
      assert.equal(bodies.length, 0)
      const info = yield* Provider.get(provider, 'gpt-5.4').pipe(Effect.provideContext(context))
      assert.deepEqual(info.reasoning?.effort, { values: ['low', 'high'], default: 'low' })
      const original = yield* resolve.pipe(Effect.provideContext(context))
      const codec = original.configuration
      assert.isDefined(codec)
      if (codec === undefined) return yield* Effect.die('Missing subscription configuration')
      const pinned = yield* codec.encode(Context.empty())
      yield* original.languageModel.generateText({ prompt: 'hello' })
      const reopened = yield* Provider.resolve(provider, {
        modelId: 'gpt-5.4',
        options: { reasoning: { effort: 'low' } },
      }).pipe(Effect.provideContext(context))
      const restored = yield* (
        reopened.configuration?.restore(pinned) ?? Effect.die('Missing configuration')
      )
      yield* reopened.languageModel
        .generateText({ prompt: 'continue' })
        .pipe(Effect.provideContext(restored))
      assert.deepEqual(
        bodies.map((body) => body.reasoning),
        [
          { effort: 'high', summary: 'auto' },
          { effort: 'high', summary: 'auto' },
        ],
      )
      const before = bodies.length
      assert.equal(
        (yield* Effect.flip(
          Provider.resolve(provider, {
            modelId: 'gpt-5.4',
            options: { reasoning: { effort: 'future-effort' } },
          }).pipe(Effect.provideContext(context)),
        ))._tag,
        'ModelCatalogError',
      )
      assert.equal(bodies.length, before)
    }),
)

it.effect(
  'Claude OAuth lists bundled models without services and creates durable adaptive effort settings',
  () =>
    Effect.gen(function* () {
      const provider = Claude.make({
        id: 'claude-personal',
        binding: { ...binding, provider: 'anthropic' },
      })
      const page = yield* Provider.list(provider)
      assert.isTrue(page.items.some((entry) => entry.id === 'claude-opus-5-5'))
      for (const entry of page.items) {
        const info = yield* Provider.get(provider, entry.id)
        assert.deepEqual(info.reasoning?.thinking?.modes, ['adaptive'])
        assert.include(info.reasoning?.effort?.values ?? [], 'max')
      }
      const bodies: Schema.JsonObject[] = []
      const store = yield* credentials('anthropic')
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          bodies.push(payload(request))
          return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(claudeResponse)))
        }),
      )
      const context = Context.make(Store.CredentialStore, store).pipe(
        Context.add(HttpClient.HttpClient, client),
      )
      const original = yield* Provider.resolve(provider, {
        modelId: 'claude-opus-5-5',
        options: { reasoning: { effort: 'high' } },
      }).pipe(Effect.provideContext(context))
      const pinned = yield* (
        original.configuration?.encode(Context.empty()) ?? Effect.die('Missing configuration')
      )
      yield* original.languageModel.generateText({ prompt: 'hello' })
      const reopened = yield* Provider.resolve(provider, { modelId: 'claude-opus-5-5' }).pipe(
        Effect.provideContext(context),
      )
      const restored = yield* (
        reopened.configuration?.restore(pinned) ?? Effect.die('Missing configuration')
      )
      yield* reopened.languageModel
        .generateText({ prompt: 'continue' })
        .pipe(Effect.provideContext(restored))
      assert.deepEqual(
        bodies.map((body) => body.output_config),
        [{ effort: 'high' }, { effort: 'high' }],
      )
      for (const body of bodies)
        assert.deepEqual(body.thinking, {
          type: 'adaptive',
          block_binding: { prefix_mismatch_behavior: 'drop_block' },
        })
      assert.equal(
        (yield* Effect.flip(
          Provider.resolve(provider, {
            modelId: 'claude-opus-5-5',
            options: { reasoning: { thinking: { mode: 'disabled' } } },
          }).pipe(Effect.provideContext(context)),
        ))._tag,
        'ModelCatalogError',
      )
      assert.equal(
        (yield* Effect.flip(Provider.get(provider, 'unknown-model'))).reason,
        'modelNotFound',
      )
    }),
)

it.effect(
  'Claude API-key discovery requests a single page or one ID and normalizes capability flags',
  () =>
    Effect.gen(function* () {
      const paths: string[] = []
      const entry = {
        id: 'model-a',
        display_name: 'Model A',
        max_input_tokens: 200000,
        max_tokens: 8192,
        capabilities: {
          image_input: { supported: true },
          structured_outputs: { supported: false },
          effort: {
            supported: true,
            low: { supported: true },
            high: { supported: true },
            max: { supported: false },
          },
          thinking: {
            supported: true,
            types: {
              adaptive: { supported: true },
              enabled: { supported: true },
              disabled: { supported: true },
            },
          },
        },
      }
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          paths.push(request.url)
          assert.equal(request.headers['x-api-key'], 'test-key')
          assert.equal(request.headers.authorization, undefined)
          if (request.url.endsWith('/models')) {
            assert.deepEqual(request.urlParams.params, [
              ['after_id', 'previous'],
              ['limit', '2'],
            ])
            return HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify({ data: [entry], has_more: true, last_id: 'model-a' })),
            )
          }
          return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(entry)))
        }),
      )
      const source = ClaudeCatalog.apiKey({ apiKey: Redacted.make('test-key') })
      const list = source.list({ cursor: 'previous', limit: 2 })
      assert.deepEqual(paths, [])
      assert.deepEqual(yield* list.pipe(Effect.provideService(HttpClient.HttpClient, client)), {
        items: [{ id: 'model-a', name: 'Model A' }],
        nextCursor: 'model-a',
      })
      const info = yield* source
        .get('model-a')
        .pipe(Effect.provideService(HttpClient.HttpClient, client))
      assert.deepEqual(paths, [
        'https://api.anthropic.com/v1/models',
        'https://api.anthropic.com/v1/models/model-a',
      ])
      assert.deepEqual(info.reasoning?.effort?.values, ['low', 'high'])
      assert.deepEqual(info.reasoning?.thinking?.budgetTokens, { min: 1024, max: 8191 })
      yield* ModelInfo.validateOptions(info, {
        reasoning: { thinking: { mode: 'enabled', budgetTokens: 2048 } },
      })
    }),
)

it.effect(
  'discovery failures are sanitized and never fall back to invented account availability',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('openai-codex')
      const provider = Codex.make({ id: 'codex', binding, clientVersion: '0.99.0' })
      for (const status of [401, 500]) {
        const http = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response('SECRET-remote-body', { status })),
          ),
        )
        const error = yield* Effect.flip(
          Provider.list(provider).pipe(
            Effect.provideService(HttpClient.HttpClient, http),
            Effect.provideService(Store.CredentialStore, store),
          ),
        )
        assert.equal(error._tag, 'AiError')
        assert.notInclude(JSON.stringify(error), 'SECRET')
      }
    }),
)

it.effect(
  'Codex snapshot caching shares list and get, retries failures, and expires or invalidates together',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('openai-codex')
      let requests = 0
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          requests++
          return HttpClientResponse.fromWeb(
            request,
            requests === 1
              ? new Response('unavailable', { status: 503 })
              : new Response(
                  JSON.stringify({ models: [codexEntry, { ...codexEntry, slug: 'second' }] }),
                ),
          )
        }),
      )
      const source = yield* CodexCatalog.cached(
        { binding, clientVersion: '0.99.0' },
        { timeToLive: '10 seconds' },
      ).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(HttpClient.HttpClient, client),
      )
      assert.equal(requests, 0)
      yield* Effect.flip(source.get('gpt-5.4'))
      assert.equal(requests, 1)
      const results = yield* Effect.all(
        [source.list({}), source.get('gpt-5.4'), source.get('second')],
        { concurrency: 'unbounded' },
      )
      assert.equal(requests, 2)
      assert.deepEqual(
        results[0].items.map((item) => item.id),
        ['gpt-5.4', 'second'],
      )
      assert.equal(results[1].id, 'gpt-5.4')
      assert.equal(results[2].id, 'second')
      yield* source.invalidate
      yield* source.get('second')
      assert.equal(requests, 3)
      yield* TestClock.adjust('11 seconds')
      yield* source.list({})
      assert.equal(requests, 4)
    }),
)
