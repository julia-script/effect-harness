import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Credential from '../src/provider-auth/Credential.js'
import * as Codex from '../src/provider-openai-codex/CodexLanguageModel.js'
import * as Claude from '../src/provider-anthropic/OAuthLanguageModel.js'
import { ProviderAffinity } from '../src/ProviderAffinity.js'
import {
  binding,
  credentials,
  codexResponse,
  claudeResponse,
  payload,
  sse,
} from './fixtures/SubscriptionProtocol.js'

it.effect(
  'Codex maps supported options, disables all affinity fields, and retains opaque reasoning across serialized history',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('openai-codex')
      const bodies: unknown[] = []
      const response = {
        ...codexResponse,
        output: [
          {
            id: 'reason-1',
            type: 'reasoning',
            summary: [],
            encrypted_content: 'opaque-continuity',
          },
          ...codexResponse.output,
        ],
      }
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          const body = payload(request)
          bodies.push(body)
          assert.isUndefined(body.prompt_cache_key)
          assert.isUndefined(request.headers['session-id'])
          assert.isUndefined(request.headers['x-client-request-id'])
          assert.deepEqual(body.reasoning, { effort: 'high', summary: 'concise' })
          assert.strictEqual(body.temperature, 0.4)
          assert.strictEqual(body.service_tier, 'priority')
          assert.deepEqual(body.text, { verbosity: 'high' })
          return HttpClientResponse.fromWeb(
            request,
            new Response(sse([{ type: 'response.completed', response }])),
          )
        }),
      )
      const model = yield* Codex.make({
        model: 'gpt-5.4',
        binding,
        reasoningEfforts: ['high'],
        supportsTemperature: true,
        config: {
          cache: 'none',
          cacheKey: 'ignored',
          reasoningEffort: 'high',
          reasoningSummary: 'concise',
          temperature: 0.4,
          serviceTier: 'priority',
          verbosity: 'high',
        },
      }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(HttpClient.HttpClient, http),
      )
      const first = yield* model
        .generateText({ prompt: 'hello' })
        .pipe(Effect.provideService(ProviderAffinity, { id: 'conversation' }))
      assert.strictEqual(first.reasoning.length, 1)
      const codec = Schema.toCodecJson(Prompt.Prompt)
      const encoded = yield* Schema.encodeEffect(codec)(Prompt.fromResponseParts(first.content))
      const history = yield* Schema.decodeEffect(codec)(JSON.parse(JSON.stringify(encoded)))
      yield* model.generateText({ prompt: Prompt.concat(history, 'continue') })
      assert.include(JSON.stringify(bodies[1]), 'opaque-continuity')
      assert.notInclude(JSON.stringify(bodies[1]), 'item_reference')
    }),
)

it.effect(
  'Claude applies thinking/cache controls, retains signatures, and resolves updated credentials on the same model',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('anthropic')
      const bodies: unknown[] = []
      const headers: string[] = []
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          const body = payload(request)
          bodies.push(body)
          headers.push(request.headers.authorization!)
          assert.deepEqual(body.thinking, { type: 'adaptive', display: 'omitted' })
          assert.deepEqual(body.output_config, { effort: 'high' })
          assert.strictEqual(body.max_tokens, 8192)
          assert.notInclude(JSON.stringify(body), 'cache_control')
          assert.deepEqual(body.metadata, { user_id: 'caller-attribution' })
          return HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({
                ...claudeResponse,
                content: [
                  { type: 'thinking', thinking: '', signature: 'continuity-signature' },
                  { type: 'redacted_thinking', data: 'redacted-continuity' },
                  ...claudeResponse.content,
                ],
              }),
            ),
          )
        }),
      )
      const model = yield* Claude.make({
        model: 'claude-sonnet-4-6',
        binding: { ...binding, provider: 'anthropic' },
        thinkingModes: ['adaptive'],
        efforts: ['high'],
        config: {
          thinking: { type: 'adaptive' },
          thinkingDisplay: 'omitted',
          effort: 'high',
          maxTokens: 8192,
          cache: 'none',
          userId: 'caller-attribution',
        },
      }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(HttpClient.HttpClient, http),
      )
      const first = yield* model.generateText({ prompt: 'hello' })
      assert.strictEqual(first.reasoning.length, 2)
      yield* store.modify(Credential.key({ ...binding, provider: 'anthropic' }), (current) =>
        Effect.succeed([
          undefined,
          current?.type === 'oauth'
            ? { ...current, accessToken: Redacted.make('SECRET-updated') }
            : current,
        ] as const),
      )
      yield* model.generateText({
        prompt: Prompt.concat(Prompt.fromResponseParts(first.content), 'continue'),
      })
      assert.deepEqual(headers, ['Bearer SECRET-access', 'Bearer SECRET-updated'])
      assert.include(JSON.stringify(bodies[1]), 'continuity-signature')
      assert.include(JSON.stringify(bodies[1]), 'redacted-continuity')
    }),
)

const quotaEvents = {
  'response.failed': {
    type: 'response.failed',
    response: {
      ...codexResponse,
      error: { code: 'usage_limit_reached', message: 'SECRET-message' },
    },
  },
  flat: { type: 'error', code: 'usage_limit_reached', message: 'SECRET-message', param: null },
  'flat without param': { type: 'error', code: 'usage_limit_reached', message: 'SECRET-message' },
  nested: { type: 'error', error: { code: 'insufficient_quota', message: 'SECRET-message' } },
}
for (const [shape, event] of Object.entries(quotaEvents))
  it.effect(`subscription quota exhaustion in ${shape} is not retryable`, () =>
    Effect.gen(function* () {
      const store = yield* credentials('openai-codex')
      const model = yield* Codex.make({ model: 'gpt-5.4', binding }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.succeed(HttpClientResponse.fromWeb(request, new Response(sse([event])))),
          ),
        ),
      )
      const error = yield* Effect.flip(model.generateText({ prompt: 'hello' }))
      assert.strictEqual(error.reason._tag, 'QuotaExhaustedError')
      assert.isFalse(error.reason.isRetryable)
      assert.notInclude(JSON.stringify(error), 'SECRET')
    }),
  )

it.effect('Claude cache opt-out preserves application schemas and historical tool inputs', () =>
  Effect.gen(function* () {
    const store = yield* credentials('anthropic')
    let body: Record<string, unknown> = {}
    const model = yield* Claude.make({
      model: 'claude-sonnet-4-6',
      binding: { ...binding, provider: 'anthropic' },
      config: { cache: 'none' },
    }).pipe(
      Effect.provideService(Store.CredentialStore, store),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.sync(() => {
            body = payload(request)
            return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(claudeResponse)))
          }),
        ),
      ),
    )
    yield* model.generateText({
      prompt: Prompt.make([
        {
          role: 'user',
          content: [
            {
              type: 'text',
              text: 'configure',
              options: { anthropic: { cacheControl: { type: 'ephemeral' } } },
            },
          ],
        },
        {
          role: 'assistant',
          content: [
            {
              type: 'tool-call',
              id: 'call-1',
              name: 'configure',
              params: { cache_control: 'application-value' },
            },
          ],
        },
        {
          role: 'tool',
          content: [
            {
              type: 'tool-result',
              id: 'call-1',
              name: 'configure',
              result: 'done',
              isFailure: false,
            },
          ],
        },
      ]),
      toolkit: Toolkit.make(
        Tool.make('configure', {
          parameters: Schema.Struct({ cache_control: Schema.String }),
          success: Schema.String,
        }),
      ),
      disableToolCallResolution: true,
    })
    assert.include(JSON.stringify(body), '"cache_control":{"type":"string"}')
    assert.include(JSON.stringify(body), '"cache_control":"application-value"')
    assert.notInclude(JSON.stringify(body), '"type":"ephemeral"')
  }),
)

it.effect('Claude gates temperature and xhigh effort against explicit model declarations', () =>
  Effect.gen(function* () {
    const store = yield* credentials('anthropic')
    const bodies: Record<string, unknown>[] = []
    const make = (options: Partial<Claude.Options>) =>
      Claude.make({
        model: 'claude-sonnet-4-6',
        binding: { ...binding, provider: 'anthropic' },
        ...options,
      }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.sync(() => {
              bodies.push(payload(request))
              return HttpClientResponse.fromWeb(
                request,
                new Response(JSON.stringify(claudeResponse)),
              )
            }),
          ),
        ),
      )
    const unsupported = yield* make({ config: { temperature: 0.4 } })
    const error = yield* Effect.flip(unsupported.generateText({ prompt: 'hello' }))
    assert.strictEqual(error.reason._tag, 'InvalidRequestError')
    assert.strictEqual(bodies.length, 0)
    const supported = yield* make({ supportsTemperature: true, config: { temperature: 0.4 } })
    yield* supported.generateText({ prompt: 'hello' })
    assert.strictEqual(bodies[0]?.temperature, 0.4)
    const reasoning = yield* make({
      thinkingModes: ['adaptive'],
      efforts: ['xhigh'],
      config: { thinking: { type: 'adaptive' }, effort: 'xhigh' },
    })
    yield* reasoning.generateText({ prompt: 'hello' })
    assert.deepEqual(bodies[1]?.output_config, { effort: 'xhigh' })
    const undeclared = yield* make({
      thinkingModes: ['adaptive'],
      config: { thinking: { type: 'adaptive' }, effort: 'xhigh' },
    })
    assert.strictEqual(
      (yield* Effect.flip(undeclared.generateText({ prompt: 'hello' }))).reason._tag,
      'InvalidRequestError',
    )
    assert.strictEqual(bodies.length, 2)
  }),
)
