/** PKCE authorization and token exchange over a local HTTP server. */
import { expect, test } from 'bun:test'
import { Effect, Redacted } from 'effect'
import { Credentials } from '../../credentials/index.ts'
import { AnthropicOAuth } from '../index.ts'
import { existing, run, token } from './oauthSupport.ts'

test('PKCE binds the callback and stores credentials only after the JSON exchange', () =>
  run(
    () => ({ json: token }),
    (api) =>
      Effect.gen(function* () {
        const authorization = yield* AnthropicOAuth.begin({ name: 'work' })
        const url = new URL(authorization.url)
        expect(url.origin + url.pathname).toBe('https://claude.ai/oauth/authorize')
        expect(url.searchParams.get('state')).toBe(Redacted.value(authorization.verifier))
        expect(url.searchParams.get('code_challenge_method')).toBe('S256')
        const digest = yield* Effect.promise(() =>
          crypto.subtle.digest(
            'SHA-256',
            new TextEncoder().encode(Redacted.value(authorization.verifier)),
          ),
        )
        expect(url.searchParams.get('code_challenge')).toBe(
          Buffer.from(digest).toString('base64url'),
        )
        yield* AnthropicOAuth.complete(authorization, `code-one#${authorization.state}`, {
          authUrl: api.url,
        })
        expect(api.requests[0]).toMatchObject({
          path: '/v1/oauth/token',
          body: {
            grant_type: 'authorization_code',
            client_id: '9d1c250a-e61b-44d9-88ed-5944d1962f5e',
            code: 'code-one',
            code_verifier: authorization.state,
            state: authorization.state,
            redirect_uri: 'https://platform.claude.com/oauth/code/callback',
          },
        })
        expect(api.requests[0]?.headers['content-type']).toContain('application/json')
        const store = yield* Credentials.Credentials
        const saved = yield* store.get('work')
        expect(saved?.type).toBe('oauth')
        if (saved?.type !== 'oauth') return yield* Effect.die('Missing saved tokens')
        expect(Redacted.value(saved.access)).toBe('access-new')
        expect(Redacted.value(saved.refresh)).toBe('refresh-new')
        expect(typeof saved.extra?.identity).toBe('string')
        expect(saved.expires).toBeGreaterThan(Date.now())
        return yield* Effect.void
      }),
  ))

test.each([
  'bare-code',
  'code#wrong',
  'code#wrong#extra',
  '?code=x&state=wrong',
  '?error=denied&code=x&state=wrong',
])('rejects invalid completion %s without exchanging or replacing credentials', (input) =>
  run(
    () => ({ json: token }),
    (api) =>
      Effect.gen(function* () {
        const store = yield* Credentials.Credentials
        yield* store.set('work', existing)
        const authorization = yield* AnthropicOAuth.begin({ name: 'work' })
        const result = yield* Effect.result(
          AnthropicOAuth.complete(authorization, input, { authUrl: api.url }),
        )
        expect(result).toMatchObject({ _tag: 'Failure', failure: { reason: 'InvalidState' } })
        expect(api.requests).toHaveLength(0)
        expect(yield* store.get('work')).toEqual(existing)
      }),
  ),
)

test('valid callback URLs are accepted, but duplicate parameters and other redirect origins are not', () =>
  run(
    () => ({ json: token }),
    (api) =>
      Effect.gen(function* () {
        const authorization = yield* AnthropicOAuth.begin()
        const query = `code=ok&state=${authorization.state}`
        for (const input of [
          `${authorization.redirectUri}?${query}&state=${authorization.state}`,
          `${authorization.redirectUri}?${query}&code=second`,
          `https://attacker.invalid/callback?${query}`,
          `${authorization.redirectUri}?${query}&error=denied`,
        ]) {
          const result = yield* Effect.result(
            AnthropicOAuth.complete(authorization, input, { authUrl: api.url }),
          )
          expect(result).toMatchObject({ _tag: 'Failure', failure: { reason: 'InvalidState' } })
        }
        expect(api.requests).toHaveLength(0)
        yield* AnthropicOAuth.complete(authorization, `${authorization.redirectUri}?${query}`, {
          authUrl: api.url,
        })
        expect(api.requests).toHaveLength(1)
      }),
  ))

test.each([0, -1, 1e100])('invalid expiry %s never replaces an existing credential', (expires_in) =>
  run(
    () => ({ json: { ...token, expires_in } }),
    (api) =>
      Effect.gen(function* () {
        const store = yield* Credentials.Credentials
        yield* store.set('work', existing)
        const authorization = yield* AnthropicOAuth.begin({ name: 'work' })
        const result = yield* Effect.result(
          AnthropicOAuth.complete(authorization, `code#${authorization.state}`, {
            authUrl: api.url,
          }),
        )
        expect(result).toMatchObject({ _tag: 'Failure', failure: { reason: 'InvalidResponse' } })
        expect(yield* store.get('work')).toEqual(existing)
      }),
  ),
)

test('expired authorizations do not reach the token endpoint', () =>
  run(
    () => ({ json: token }),
    (api) =>
      Effect.gen(function* () {
        const authorization = yield* AnthropicOAuth.begin()
        const result = yield* Effect.result(
          AnthropicOAuth.complete({ ...authorization, expires: 0 }, `code#${authorization.state}`, {
            authUrl: api.url,
          }),
        )
        expect(result).toMatchObject({ _tag: 'Failure', failure: { reason: 'Timeout' } })
        expect(api.requests).toHaveLength(0)
      }),
  ))

test('login carries a host loopback redirect through authorization and exchange', () =>
  run(
    () => ({ json: token }),
    (api) =>
      Effect.gen(function* () {
        let callback = ''
        const redirectUri = 'http://localhost:54321/callback'
        yield* AnthropicOAuth.login({
          name: 'browser',
          authUrl: api.url,
          redirectUri,
          onAuthorize: (value) =>
            Effect.sync(() => {
              const url = new URL(value)
              expect(url.searchParams.get('redirect_uri')).toBe(redirectUri)
              expect(url.searchParams.get('code')).toBe('true')
              callback = `${redirectUri}?code=approved&state=${url.searchParams.get('state')}`
            }),
          completion: Effect.sync(() => callback),
        })
        expect(api.requests[0]?.body).toMatchObject({ redirect_uri: redirectUri, code: 'approved' })
        const store = yield* Credentials.Credentials
        expect((yield* store.get('browser'))?.type).toBe('oauth')
      }),
  ))
