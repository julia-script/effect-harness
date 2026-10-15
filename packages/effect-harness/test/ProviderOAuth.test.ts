import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Deferred from 'effect/Deferred'
import * as Schema from 'effect/Schema'
import * as Redacted from 'effect/Redacted'
import { toAiError } from '../src/provider-auth/AuthError.js'
import * as TestClock from 'effect/testing/TestClock'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as MemoryCredentialStore from '../src/provider-auth/MemoryCredentialStore.js'
import * as Codex from '../src/provider-openai-codex/OAuth.js'
import * as Claude from '../src/provider-anthropic/OAuth.js'

const token = `header.${btoa(JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: 'account-1' } }))}.signature`
const tokenResponse = { access_token: token, refresh_token: 'SECRET-refresh', expires_in: 3600 }
const bodySchema = Schema.fromJsonString(Schema.JsonObject)

describe('provider OAuth protocol fixtures', () => {
  for (const provider of ['openai-codex', 'anthropic'] as const)
    for (const valid of [true, false])
      it.effect(
        `${provider}: PKCE ${valid ? 'completion exchanges exact verifier and redirect' : 'state mismatch never exchanges or stores credentials'}`,
        () =>
          Effect.gen(function* () {
            const store = yield* MemoryCredentialStore.make
            let authorization: URL | undefined
            let calls = 0
            const http = HttpClient.make((request) =>
              Effect.gen(function* () {
                calls++
                assert.isDefined(authorization)
                let body: Record<string, unknown>
                if (request.body._tag !== 'Uint8Array')
                  return assert.fail('Expected token request body')
                const text = new TextDecoder().decode(request.body.body)
                body =
                  provider === 'openai-codex'
                    ? Object.fromEntries(new URLSearchParams(text))
                    : yield* Schema.decodeEffect(bodySchema)(text).pipe(Effect.orDie)
                assert.strictEqual(body.grant_type, 'authorization_code')
                assert.strictEqual(body.code, 'one-time-code')
                assert.strictEqual(
                  body.redirect_uri,
                  authorization!.searchParams.get('redirect_uri'),
                )
                assert.strictEqual(
                  body.client_id,
                  provider === 'openai-codex' ? Codex.protocol.clientId : Claude.protocol.clientId,
                )
                const verifier = String(body.code_verifier)
                const digest = yield* Effect.promise(() =>
                  crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier)),
                )
                const challenge = btoa(String.fromCharCode(...new Uint8Array(digest)))
                  .replace(/\+/g, '-')
                  .replace(/\//g, '_')
                  .replace(/=+$/, '')
                assert.strictEqual(challenge, authorization!.searchParams.get('code_challenge'))
                return HttpClientResponse.fromWeb(
                  request,
                  new Response(JSON.stringify(tokenResponse)),
                )
              }),
            )
            const options = {
              profile: 'personal',
              interaction: {
                authorize: (url: string) =>
                  Effect.sync(() => {
                    authorization = new URL(url)
                  }),
                complete: Effect.sync(
                  () =>
                    `one-time-code#${valid ? authorization!.searchParams.get('state') : 'different-attempt'}`,
                ),
              },
            }
            const login = (
              provider === 'openai-codex'
                ? Codex.loginBrowser(options)
                : Claude.loginCopyCode(options)
            ).pipe(
              Effect.provideService(Store.CredentialStore, store),
              Effect.provideService(HttpClient.HttpClient, http),
            )
            if (valid) {
              const status = yield* login
              assert.strictEqual(status.binding.provider, provider)
              assert.strictEqual(status.binding.profile, 'personal')
              assert.strictEqual(status.expiresAt, 3_600_000)
              assert.strictEqual(calls, 1)
              assert.notInclude(JSON.stringify(status), 'SECRET')
              assert.strictEqual((yield* store.list).length, 1)
            } else {
              assert.strictEqual((yield* Effect.flip(login)).reason, 'InvalidState')
              assert.strictEqual(calls, 0)
              assert.deepEqual(yield* store.list, [])
            }
            assert.strictEqual(authorization!.searchParams.get('code_challenge_method'), 'S256')
            assert.strictEqual(
              authorization!.searchParams.get('scope'),
              provider === 'openai-codex' ? Codex.protocol.scope : Claude.protocol.scope,
            )
          }),
      )

  it.effect(
    'Codex device polling observes pending cadence and exchanges the returned device verifier',
    () =>
      Effect.gen(function* () {
        const store = yield* MemoryCredentialStore.make
        const started = yield* Deferred.make<void>()
        const polled = yield* Deferred.make<void>()
        let polls = 0
        let exchanged = false
        const http = HttpClient.make((request) =>
          Effect.gen(function* () {
            if (request.url.endsWith('/usercode'))
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  JSON.stringify({
                    device_auth_id: 'device-1',
                    user_code: 'ABCD',
                    interval: '2',
                    expires_in: 60,
                  }),
                ),
              )
            if (request.url.endsWith('/deviceauth/token')) {
              polls++
              yield* Deferred.succeed(polled, undefined)
              return HttpClientResponse.fromWeb(
                request,
                polls === 1
                  ? new Response('', { status: 403 })
                  : new Response(
                      JSON.stringify({
                        authorization_code: 'device-code',
                        code_verifier: 'device-verifier',
                      }),
                    ),
              )
            }
            exchanged = true
            if (request.body._tag !== 'Uint8Array') return assert.fail('Expected form body')
            const body = new URLSearchParams(new TextDecoder().decode(request.body.body))
            assert.strictEqual(body.get('code'), 'device-code')
            assert.strictEqual(body.get('code_verifier'), 'device-verifier')
            assert.strictEqual(
              body.get('redirect_uri'),
              'https://auth.openai.com/deviceauth/callback',
            )
            return HttpClientResponse.fromWeb(request, new Response(JSON.stringify(tokenResponse)))
          }),
        )
        const login = yield* Codex.loginDevice({
          profile: 'device',
          interaction: {
            authorize: () => Effect.die('Not browser login'),
            complete: Effect.never,
            device: (info) =>
              Effect.gen(function* () {
                assert.strictEqual(info.code, 'ABCD')
                yield* Deferred.succeed(started, undefined)
              }),
          },
        }).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(HttpClient.HttpClient, http),
          Effect.forkChild,
        )
        yield* Deferred.await(started)
        yield* TestClock.adjust(1999)
        assert.strictEqual(polls, 0)
        yield* TestClock.adjust(1)
        yield* Deferred.await(polled)
        assert.strictEqual(polls, 1)
        assert.isFalse(exchanged)
        yield* TestClock.adjust(2000)
        assert.strictEqual((yield* Fiber.join(login)).binding.provider, 'openai-codex')
        assert.strictEqual(polls, 2)
      }),
  )

  it.effect('token failures discard remote bodies and malformed secrets', () =>
    Effect.gen(function* () {
      for (const status of [200, 400, 503]) {
        const store = yield* MemoryCredentialStore.make
        let state = ''
        const error = yield* Claude.loginCopyCode({
          profile: 'test',
          interaction: {
            authorize: (url) =>
              Effect.sync(() => {
                state = new URL(url).searchParams.get('state')!
              }),
            complete: Effect.sync(() => `code#${state}`),
          },
        }).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.succeed(
                HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify({ access_token: 'SECRET-access', error: 'SECRET-response' }),
                    { status },
                  ),
                ),
              ),
            ),
          ),
          Effect.flip,
        )
        assert.notInclude(JSON.stringify(error), 'SECRET')
        assert.notInclude(String(error), 'SECRET')
        assert.deepEqual(yield* store.list, [])
      }
    }),
  )
})

it.effect('device expiry interrupts a pending interaction before any poll or token exchange', () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>()
    const store = yield* MemoryCredentialStore.make
    let requests = 0
    let interrupted = false
    const login = yield* Codex.loginDevice({
      profile: 'expiry',
      timeoutMs: 60_000,
      interaction: {
        authorize: () => Effect.void,
        complete: Effect.never,
        device: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                interrupted = true
              }),
            ),
          ),
      },
    }).pipe(
      Effect.provideService(Store.CredentialStore, store),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.sync(() => {
            requests++
            return HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  device_auth_id: 'id',
                  user_code: 'code',
                  interval: 1,
                  expires_in: 2,
                }),
              ),
            )
          }),
        ),
      ),
      Effect.flip,
      Effect.forkChild,
    )
    yield* Deferred.await(started)
    yield* TestClock.adjust(2000)
    assert.isDefined(login.pollUnsafe(), 'device expiry must end the interaction')
    assert.strictEqual((yield* Fiber.join(login)).reason, 'ExpiredCredential')
    assert.isTrue(interrupted)
    assert.strictEqual(requests, 1)
    assert.deepEqual(yield* store.list, [])
  }),
)

for (const provider of ['anthropic', 'openai-codex'] as const)
  for (const status of [400, 429, 503])
    it.effect(`${provider} refresh HTTP ${status} retains sanitized retry classification`, () =>
      Effect.gen(function* () {
        const credential = {
          type: 'oauth' as const,
          binding: { provider, profile: 'test', identity: 'login' },
          accessToken: Redacted.make('SECRET-access'),
          refreshToken: Redacted.make('SECRET-refresh'),
          expiresAt: 1,
        }
        const error = yield* (
          provider === 'anthropic' ? Claude.refresh(credential) : Codex.refresh(credential)
        ).pipe(
          Effect.provideService(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.succeed(
                HttpClientResponse.fromWeb(request, new Response('SECRET-response', { status })),
              ),
            ),
          ),
          Effect.flip,
        )
        assert.strictEqual(error.isRetryable, status !== 400)
        assert.strictEqual(toAiError(error).reason.isRetryable, status !== 400)
        assert.notInclude(JSON.stringify(error), 'SECRET')
      }),
    )
