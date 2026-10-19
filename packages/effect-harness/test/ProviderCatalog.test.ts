import { managed } from './fixtures/ManagedProvider.js'
import { assert, it } from '@effect/vitest'
import * as TestClock from 'effect/testing/TestClock'
import * as CodexCatalog from '../src/provider-openai-codex/Catalog.js'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import type * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
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

const claudeEntry = {
  id: 'claude-opus-5-5',
  display_name: 'Claude Opus 5.5',
  max_input_tokens: 1_000_000,
  max_tokens: 128_000,
  capabilities: {
    image_input: { supported: true },
    structured_outputs: { supported: true },
    effort: {
      supported: true,
      low: { supported: true },
      high: { supported: true },
      max: { supported: true },
    },
    thinking: {
      supported: true,
      types: { adaptive: { supported: true }, disabled: { supported: false } },
    },
  },
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
      const provider = yield* managed(Codex.definition, { clientVersion: '0.99.0' }, binding).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      const resolve = provider.resolve({
        modelId: 'gpt-5.4',
        options: { reasoning: { effort: 'high' } },
      })
      assert.deepEqual(gets, [])
      assert.deepEqual((yield* provider.list().pipe(Effect.provideContext(context))).items, [
        { id: 'gpt-5.4', name: 'GPT 5.4' },
      ])
      assert.equal(bodies.length, 0)
      const info = yield* provider.get('gpt-5.4').pipe(Effect.provideContext(context))
      assert.deepEqual(info.reasoning?.effort, { values: ['low', 'high'], default: 'low' })
      const original = yield* resolve.pipe(Effect.provideContext(context))
      const codec = original.configuration
      assert.isDefined(codec)
      if (codec === undefined) return yield* Effect.die('Missing subscription configuration')
      const pinned = yield* codec.encode(Context.empty())
      yield* original.languageModel.generateText({ prompt: 'hello' })
      const reopened = yield* provider
        .resolve({
          modelId: 'gpt-5.4',
          options: { reasoning: { effort: 'low' } },
        })
        .pipe(Effect.provideContext(context))
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
          provider
            .resolve({
              modelId: 'gpt-5.4',
              options: { reasoning: { effort: 'future-effort' } },
            })
            .pipe(Effect.provideContext(context)),
        ))._tag,
        'ModelCatalogError',
      )
      assert.equal(bodies.length, before)
    }),
)

it.effect(
  'Claude OAuth discovers lazily with bearer auth and creates durable adaptive effort settings',
  () =>
    Effect.gen(function* () {
      const paths: string[] = []
      const bodies: Schema.JsonObject[] = []
      const store = yield* credentials('anthropic')
      const provider = yield* managed(Claude.definition, {}, binding).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          if (request.method === 'GET') {
            paths.push(request.url)
            assert.equal(request.headers.authorization, 'Bearer SECRET-access')
            assert.equal(request.headers['x-api-key'], undefined)
            assert.equal(request.headers['anthropic-beta'], 'oauth-2025-04-20')
            const body = request.url.endsWith('/models')
              ? { data: [claudeEntry], has_more: true, last_id: claudeEntry.id }
              : claudeEntry
            return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(body)))
          }
          bodies.push(payload(request))
          return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(claudeResponse)))
        }),
      )
      const context = Context.make(Store.CredentialStore, store).pipe(
        Context.add(HttpClient.HttpClient, client),
      )
      const listing = provider.list({ limit: 1 })
      assert.deepEqual(paths, [])
      const page = yield* listing.pipe(Effect.provideContext(context))
      assert.deepEqual(page, {
        items: [{ id: claudeEntry.id, name: claudeEntry.display_name }],
        nextCursor: claudeEntry.id,
      })
      const info = yield* provider.get(claudeEntry.id).pipe(Effect.provideContext(context))
      assert.deepEqual(info.reasoning?.effort?.values, ['low', 'high', 'max'])
      assert.equal(info.capabilities?.structuredOutput, false)
      assert.deepEqual(paths, [
        'https://api.anthropic.com/v1/models',
        'https://api.anthropic.com/v1/models/claude-opus-5-5',
      ])
      const original = yield* provider
        .resolve({
          modelId: 'claude-opus-5-5',
          options: { reasoning: { effort: 'high' } },
        })
        .pipe(Effect.provideContext(context))
      const pinned = yield* (
        original.configuration?.encode(Context.empty()) ?? Effect.die('Missing configuration')
      )
      yield* original.languageModel.generateText({ prompt: 'hello' })
      const reopened = yield* provider
        .resolve({ modelId: 'claude-opus-5-5' })
        .pipe(Effect.provideContext(context))
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
          display: 'summarized',
          block_binding: { prefix_mismatch_behavior: 'drop_block' },
        })
      assert.equal(
        (yield* Effect.flip(
          provider
            .resolve({
              modelId: 'claude-opus-5-5',
              options: { reasoning: { thinking: { mode: 'disabled' } } },
            })
            .pipe(Effect.provideContext(context)),
        ))._tag,
        'ModelCatalogError',
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
      const provider = yield* managed(Codex.definition, { clientVersion: '0.99.0' }, binding).pipe(
        Effect.provideService(Store.CredentialStore, store),
      )
      for (const status of [401, 500]) {
        const http = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response('SECRET-remote-body', { status })),
          ),
        )
        const error = yield* Effect.flip(
          provider
            .list()
            .pipe(
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

for (const provider of ['codex', 'claude'] as const) {
  for (const capability of ['temperature', 'longCache'] as const) {
    if (provider === 'codex' && capability === 'longCache') continue
    it.effect(`${provider} uses per-model ${capability} support and pins it for recovery`, () => {
      const bodies: Schema.JsonObject[] = []
      const client = HttpClient.make((request) =>
        Effect.sync(() => {
          bodies.push(payload(request))
          return HttpClientResponse.fromWeb(
            request,
            new Response(provider === 'codex' ? sse(codexEvents) : JSON.stringify(claudeResponse)),
          )
        }),
      )
      return Effect.gen(function* () {
        let supported = true
        const models = {
          list: () => Effect.succeed({ items: [] }),
          get: () =>
            Effect.succeed({
              id: 'custom-model',
              name: 'Custom model',
              capabilities: {
                tools: true,
                images: false,
                reasoning: false,
                structuredOutput: false,
                temperature: capability === 'temperature' && supported,
                longCache: capability === 'longCache' && supported,
              },
            }),
        }
        const configured =
          capability === 'temperature' ? { temperature: 0.5 } : { cache: 'long' as const }
        const selection =
          provider === 'codex'
            ? yield* managed(
                Codex.define(models),
                { clientVersion: '1.0', config: { temperature: 0.5 } },
                binding,
              )
            : yield* managed(Claude.define(models), { config: configured }, binding)
        const model = yield* selection.resolve({ modelId: 'custom-model' })
        const prompt = [{ role: 'user' as const, content: 'hello' }]
        yield* model.languageModel.generateText({ prompt })
        if (capability === 'temperature') assert.equal(bodies[0]!.temperature, 0.5)
        else assert.include(JSON.stringify(bodies[0]), '"ttl":"1h"')
        const snapshot = yield* model.configuration!.encode(Context.empty())
        supported = false
        const unsupported = yield* selection.resolve({ modelId: 'custom-model' })
        assert.equal(
          (yield* Effect.flip(unsupported.configuration!.restore(snapshot))).reason._tag,
          'ModelUnsupportedError',
        )
        assert.equal(
          (yield* Effect.flip(unsupported.languageModel.generateText({ prompt }))).reason._tag,
          'InvalidRequestError',
        )
        assert.equal(bodies.length, 1)
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(
          Layer.effect(
            Store.CredentialStore,
            credentials(provider === 'codex' ? 'openai-codex' : 'anthropic'),
          ),
        ),
      )
    })
  }
}
