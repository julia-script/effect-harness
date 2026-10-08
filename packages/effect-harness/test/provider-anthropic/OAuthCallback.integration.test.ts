import { OAuthLoopback } from './OAuthLoopback.ts'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import { assert, describe, it } from '@effect/vitest'
import { layerMemory } from 'effect-harness/auth/CredentialStore'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as OAuth from 'effect-harness/provider-anthropic/OAuth'
const token = { access_token: 'secret-access', refresh_token: 'secret-refresh', expires_in: 3600 }
const fixture = (
  reply: (
    request: HttpClientRequest.HttpClientRequest,
    signal: AbortSignal,
  ) => Effect.Effect<HttpClientResponse.HttpClientResponse, HttpClientError.HttpClientError> = (
    request,
  ) => Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(token))),
) => {
  const requests: Array<HttpClientRequest.HttpClientRequest> = []
  const client = HttpClient.make((request, _url, signal) => {
    requests.push(request)
    return reply(request, signal)
  })
  const layer = OAuth.layer({ authorizationLifetimeMs: 10000, refreshSkewMs: 0 }).pipe(
    Layer.provideMerge(layerMemory),
    Layer.provideMerge(Layer.merge(BunCrypto.layer, Layer.succeed(HttpClient.HttpClient, client))),
  )
  return { requests, layer }
}
describe('OAuthCallback', () => {
  it.effect(
    'opt-in scoped native loopback callback validates requests and settles a real consent exchange',
    () =>
      Effect.gen(function* () {
        const f = fixture()
        return yield* Effect.gen(function* () {
          const nativeServer = OAuthLoopback.layer
          const context = yield* Layer.build(
            OAuth.layerCallback({ account: 'browser-key' }).pipe(Layer.provide(nativeServer)),
          )
          const callback = Context.get(context, OAuth.Callback)
          const request = (url: string) =>
            HttpClient.get(url).pipe(Effect.provide(FetchHttpClient.layer))
          const wrong = yield* request(`${OAuth.browserRedirectUri}?code=c&state=wrong`)
          assert.strictEqual(wrong.status, 400)
          assert.strictEqual(f.requests.length, 0)
          const actual = yield* request(
            `${OAuth.browserRedirectUri}?code=c&state=${Redacted.value(callback.authorization.state)}`,
          )
          assert.strictEqual(actual.status, 200)
          assert.strictEqual(actual.headers['cache-control'], 'no-store')
          assert.strictEqual((yield* callback.await)._tag, 'opaqueOAuth')
          assert.strictEqual(f.requests.length, 1)
        }).pipe(Effect.provide(f.layer))
      }),
  )
})
