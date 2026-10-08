import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Config from 'effect/Config'
import * as ConfigProvider from 'effect/ConfigProvider'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Credential from 'effect-harness/auth/Credential'
import * as CredentialStore from 'effect-harness/auth/CredentialStore'
import { Jwt } from 'effect-harness/auth/Jwt'
import * as Time from 'effect-harness/auth/Time'
import * as OAuth from 'effect-harness/provider-anthropic/OAuth'
import * as ChatGpt from 'effect-harness/provider-openai/ChatGpt'

const fixture = () => {
  let requests = 0
  const client = HttpClient.make((request) =>
    Effect.sync(() => {
      requests++
      return HttpClientResponse.fromWeb(
        request,
        Response.json({
          access_token: 'renewed',
          refresh_token: 'renewed-refresh',
          expires_in: 3600,
          token_type: 'Bearer',
        }),
      )
    }),
  )
  const dependencies = Layer.mergeAll(
    BunCrypto.layer,
    Layer.succeed(HttpClient.HttpClient, client),
    Layer.succeed(Jwt, Jwt.of({ verify: () => Effect.die('Unexpected identity verification') })),
  )
  const provide = <A, E, R>(layer: Layer.Layer<A, E, R>) =>
    layer.pipe(Layer.provideMerge(CredentialStore.layerMemory), Layer.provide(dependencies))
  return { provide, requests: () => requests }
}

describe('ProviderDurationOptions', () => {
  it.effect(
    'canonical numeric, unit-string and bigint duration options preserve exact deadlines',
    () =>
      Effect.gen(function* () {
        const inputs: ReadonlyArray<readonly [Duration.Input, number]> = [
          [1234.25, 1234.25],
          ['2 seconds', 2000],
          [Duration.nanos(10000250000n), 10000.25],
        ]
        for (const [authorizationLifetime, milliseconds] of inputs) {
          const f = fixture()
          yield* Effect.gen(function* () {
            const now = DateTime.toEpochMillis(yield* DateTime.now)
            const anthropic = yield* OAuth.OAuth
            const openai = yield* ChatGpt.ChatGpt
            const first = yield* anthropic.begin({ account: 'duration' })
            const second = yield* openai.begin({
              redirectUri: 'http://127.0.0.1:12345/auth/callback',
            })
            assert.strictEqual(DateTime.toEpochMillis(first.expiresAt), now + milliseconds)
            assert.strictEqual(DateTime.toEpochMillis(second.expiresAt), now + milliseconds)
            yield* anthropic.cancel(first.state)
            yield* openai.cancel(second.state)
            assert.strictEqual(f.requests(), 0)
          }).pipe(
            Effect.provide(
              f.provide(
                Layer.merge(
                  OAuth.layer({ authorizationLifetime, refreshSkew: 0 }),
                  ChatGpt.layer({
                    appName: 'duration-regression',
                    authorizationLifetime,
                    refreshSkew: '0 seconds',
                  }),
                ),
              ),
            ),
          )
        }
      }),
  )

  it.effect(
    'default authorization deadlines and provider-specific refresh boundaries remain exact',
    () =>
      Effect.gen(function* () {
        const f = fixture()
        yield* Effect.gen(function* () {
          const now = DateTime.toEpochMillis(yield* DateTime.now)
          const anthropic = yield* OAuth.OAuth
          const openai = yield* ChatGpt.ChatGpt
          const store = yield* CredentialStore.CredentialStore
          const first = yield* anthropic.begin({ account: 'default' })
          const second = yield* openai.begin({
            redirectUri: 'http://127.0.0.1:12345/auth/callback',
          })
          assert.strictEqual(DateTime.toEpochMillis(first.expiresAt), now + 600000)
          assert.strictEqual(DateTime.toEpochMillis(second.expiresAt), now + 600000)
          const opaque = (expiresAt: number): Credential.OpaqueOAuth => ({
            _tag: 'opaqueOAuth',
            provider: 'anthropic',
            authorizationServer: OAuth.authorizationServer,
            clientId: OAuth.clientId,
            accessToken: Redacted.make('old'),
            refreshToken: Redacted.make('refresh'),
            scopes: ['user:inference'],
            expiresAt: Time.fromEpochMillis(expiresAt),
          })
          yield* store.set('anthropic', opaque(now + 300001))
          assert.strictEqual(
            Redacted.value((yield* anthropic.refresh('anthropic')).accessToken),
            'old',
          )
          assert.strictEqual(f.requests(), 0)
          yield* store.set('anthropic', opaque(now + 300000))
          assert.strictEqual(
            Redacted.value((yield* anthropic.refresh('anthropic')).accessToken),
            'renewed',
          )
          assert.strictEqual(f.requests(), 1)
          const hostId = yield* store.hostId('openai')
          const identity = (expiresAt: number): Credential.OAuth => ({
            _tag: 'oauth',
            provider: 'openai',
            issuer: ChatGpt.issuer,
            subject: 'subject',
            clientId: 'issued-client',
            hostId,
            accessToken: Redacted.make('old'),
            refreshToken: Redacted.make('refresh'),
            idToken: Redacted.make('identity'),
            scopes: [ChatGpt.directScope],
            expiresAt: Time.fromEpochMillis(expiresAt),
          })
          const grant = identity(now + 60001)
          const account = Credential.accountKey(grant)
          yield* store.set(account, grant)
          assert.strictEqual(Redacted.value((yield* openai.refresh(account)).accessToken), 'old')
          assert.strictEqual(f.requests(), 1)
          yield* store.set(account, identity(now + 60000))
          assert.strictEqual(
            Redacted.value((yield* openai.refresh(account)).accessToken),
            'renewed',
          )
          assert.strictEqual(f.requests(), 2)
          yield* anthropic.cancel(first.state)
          yield* openai.cancel(second.state)
        }).pipe(
          Effect.provide(
            f.provide(
              Layer.merge(OAuth.layer(), ChatGpt.layer({ appName: 'defaults-regression' })),
            ),
          ),
        )
      }),
  )

  it.effect('Config.Wrap resolves duration inputs', () =>
    Effect.gen(function* () {
      const f = fixture()
      const configuration = {
        authorizationLifetime: Config.Duration('AUTH_LIFETIME'),
        refreshSkew: Config.succeed(0),
      }
      yield* Effect.gen(function* () {
        const now = DateTime.toEpochMillis(yield* DateTime.now)
        const anthropic = yield* OAuth.OAuth
        const openai = yield* ChatGpt.ChatGpt
        assert.strictEqual(
          DateTime.toEpochMillis((yield* anthropic.begin({ account: 'config' })).expiresAt),
          now + 2500,
        )
        assert.strictEqual(
          DateTime.toEpochMillis(
            (yield* openai.begin({ redirectUri: 'http://127.0.0.1:12345/auth/callback' }))
              .expiresAt,
          ),
          now + 2500,
        )
      }).pipe(
        Effect.provide(
          f.provide(
            Layer.merge(
              OAuth.layerConfig(configuration),
              ChatGpt.layerConfig({
                appName: Config.succeed('config-regression'),
                ...configuration,
              }),
            ),
          ),
        ),
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({ AUTH_LIFETIME: '2500 millis' }),
        ),
      )
      assert.strictEqual(f.requests(), 0)
    }),
  )
})
