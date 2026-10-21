/** Subscription inference, discovery, and persisted refresh over the public provider. */
import { expect, test } from 'bun:test'
import { Effect, Redacted } from 'effect'
import { CredentialStore } from '../../credentials/index.ts'
import { AnthropicOAuth } from '../index.ts'
import { ask, events, existing, run, token } from './oauthSupport.ts'
test('subscription inference sends Claude headers and prefix, normalizes tool names, and restores streamed names', () =>
  run(
    () => events,
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, existing)
        const answer = yield* ask(api)
        expect(answer.content).toMatchObject([
          { type: 'toolCall', name: 'read', arguments: { path: 'file.txt' } },
        ])
        expect(api.requests[0]).toMatchObject({
          path: '/v1/messages',
          search: '?beta=true',
          headers: {
            authorization: 'Bearer access-old',
            'anthropic-version': '2023-06-01',
            'x-app': 'cli',
            'user-agent': 'claude-cli/2.1.280',
            'anthropic-dangerous-direct-browser-access': 'true',
          },
          body: {
            system: [
              { type: 'text', text: "You are Claude Code, Anthropic's official CLI for Claude." },
              { type: 'text', text: 'Keep my instructions.' },
            ],
            tools: [{ name: 'Read', strict: false }],
            thinking: {
              type: 'adaptive',
              display: 'summarized',
              block_binding: { prefix_mismatch_behavior: 'drop_block' },
            },
            cache_control: { type: 'ephemeral', ttl: '1h' },
          },
        })
        expect(api.requests[0]?.headers).not.toHaveProperty('x-api-key')
        expect(api.requests[0]?.headers['anthropic-beta']).toContain('oauth-2025-04-20')
        expect(api.requests[0]?.headers['anthropic-beta']).toContain('claude-code-20250219')
        expect(api.requests[0]?.headers['anthropic-beta']).toContain(
          'thinking-binding-controls-2026-08-01',
        )
      }),
  ))

test('concurrent discovery refreshes once, persists token rotation, and retains account identity', () =>
  run(
    (request) =>
      request.path === '/v1/oauth/token'
        ? { json: token }
        : {
            json: {
              data: [
                { id: 'claude-sonnet-5-5', display_name: 'Claude', max_input_tokens: 900_000 },
              ],
              has_more: false,
              last_id: 'claude-sonnet-5-5',
            },
          },
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, { ...existing, expires: 0 })
        const provider = AnthropicOAuth.provider({ apiUrl: api.url, authUrl: api.url })
        const lists = yield* Effect.all([provider.listModels, provider.listModels], {
          concurrency: 'unbounded',
        })
        expect(lists[0]?.[0]).toMatchObject({ id: 'claude-sonnet-5-5', contextWindow: 900_000 })
        expect(api.requests.filter((request) => request.path === '/v1/oauth/token')).toHaveLength(1)
        expect(api.requests[0]?.body).toMatchObject({
          grant_type: 'refresh_token',
          refresh_token: 'refresh-old',
        })
        const saved = yield* store.get(AnthropicOAuth.name)
        if (saved?._tag !== 'OAuth') return yield* Effect.die('Missing refreshed credential')
        expect(saved.identity).toEqual(existing.identity)
        expect(saved.revision).toBe(existing.revision)
        expect(saved.extra).toEqual(existing.extra)
        expect(Redacted.value(saved.refresh)).toBe('refresh-new')
        for (const request of api.requests.filter((recorded) => recorded.path === '/v1/models')) {
          expect(request.headers.authorization).toBe('Bearer access-new')
          expect(request.headers).not.toHaveProperty('x-api-key')
        }
        return yield* Effect.void
      }),
  ))

test('discovery rejects a repeated page cursor instead of looping indefinitely', () =>
  run(
    () => ({ json: { data: [], has_more: true, last_id: 'repeated' } }),
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, existing)
        const result = yield* Effect.result(AnthropicOAuth.provider({ apiUrl: api.url }).listModels)
        expect(result).toMatchObject({ _tag: 'Failure', failure: { reason: { _tag: 'Rejected' } } })
        expect(api.requests).toHaveLength(2)
      }),
  ))

test('failed token refresh preserves stored credentials and never sends inference', () =>
  run(
    () => ({ status: 400, json: { error: 'invalid_grant', secret: 'must-not-be-in-error' } }),
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        const expired = { ...existing, expires: 0 }
        yield* store.set(AnthropicOAuth.name, expired)
        const result = yield* Effect.result(ask(api))
        expect(result).toMatchObject({ _tag: 'Failure' })
        expect(JSON.stringify(result)).not.toContain('must-not-be-in-error')
        expect(yield* store.get(AnthropicOAuth.name)).toEqual(expired)
        expect(api.requests.map((request) => request.path)).toEqual(['/v1/oauth/token'])
      }),
  ))

test('refresh rejects a different known account without replacing credentials or sending inference', () =>
  run(
    () => ({ json: { ...token, account: { uuid: 'different-account' } } }),
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        const expired = { ...existing, expires: 0 }
        yield* store.set(AnthropicOAuth.name, expired)
        const result = yield* Effect.result(ask(api))
        expect(result).toMatchObject({ _tag: 'Failure' })
        expect(yield* store.get(AnthropicOAuth.name)).toEqual(expired)
        expect(api.requests.map((request) => request.path)).toEqual(['/v1/oauth/token'])
      }),
  ))

test('the latest default model enables adaptive binding without an explicit reasoning effort', () =>
  run(
    () => events,
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, existing)
        const answer = yield* ask(api, 'claude-sonnet-5-5', {})
        expect(answer.stopReason).toBe('toolUse')
        expect(api.requests[0]?.body).toMatchObject({
          model: 'claude-sonnet-5-5',
          thinking: {
            type: 'adaptive',
            display: 'summarized',
            block_binding: { prefix_mismatch_behavior: 'drop_block' },
          },
        })
        expect(api.requests[0]?.headers['anthropic-beta']).toContain(
          'thinking-binding-controls-2026-08-01',
        )
        expect(api.requests[0]?.body).not.toHaveProperty('output_config')
      }),
  ))

test('an older budget model retains summarized display alongside its enabled thinking budget', () =>
  run(
    () => events,
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, existing)
        yield* ask(api, 'claude-opus-4-5', { reasoning: 'medium', maxTokens: 4096 })
        expect(api.requests[0]?.body).toMatchObject({
          model: 'claude-opus-4-5',
          max_tokens: 4096,
          thinking: { type: 'enabled', budget_tokens: 3072, display: 'summarized' },
        })
        expect(api.requests[0]?.body).not.toHaveProperty('thinking.block_binding')
        expect(api.requests[0]?.headers['anthropic-beta']).not.toContain(
          'thinking-binding-controls-2026-08-01',
        )
      }),
  ))

test('discovered adaptive models send effort rather than a budget outside the static catalog', () =>
  run(
    () => events,
    (api) =>
      Effect.gen(function* () {
        const store = yield* CredentialStore.CredentialStore
        yield* store.set(AnthropicOAuth.name, existing)
        yield* ask(
          api,
          'discovered-adaptive',
          { reasoning: 'high', temperature: 0.5 },
          {
            models: [{ id: 'discovered-adaptive', reasoning: true }],
            adaptiveModels: ['discovered-adaptive'],
          },
        )
        expect(api.requests[0]?.body).toMatchObject({
          model: 'discovered-adaptive',
          thinking: { type: 'adaptive', display: 'summarized' },
          output_config: { effort: 'high' },
        })
        expect(api.requests[0]?.body).not.toHaveProperty('thinking.budget_tokens')
        expect(api.requests[0]?.body).not.toHaveProperty('temperature')
        // Tool schemas stay non-strict for adaptive models too.
        expect(api.requests[0]?.body).toMatchObject({ tools: [{ name: 'Read', strict: false }] })
      }),
  ))
