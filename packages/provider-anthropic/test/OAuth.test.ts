import { vi } from 'vitest'
vi.mock('effect/HashMap', { spy: true })
import * as HashMap from 'effect/HashMap'
import * as Inspectable from 'effect/Inspectable'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as NodeHttpServer from '@effect/platform-node/NodeHttpServer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import { createServer } from 'node:http'
import { assert, describe, it } from '@effect/vitest'
import { CredentialStore, layerMemory } from '@effect-harness/auth/CredentialStore'
import { type OpaqueOAuth } from '@effect-harness/auth/Credential'
import * as Pkce from '@effect-harness/auth/Pkce'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpServerRequest from 'effect/http/HttpServerRequest'
import type * as HttpServerResponse from 'effect/http/HttpServerResponse'
import type * as Scope from 'effect/Scope'
import * as NetAddress from 'effect/net/NetAddress'
import * as Crypto from 'effect/Crypto'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Result from 'effect/Result'
import * as TestClock from 'effect/testing/TestClock'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as OAuth from '../src/OAuth.ts'
const token = { access_token: 'secret-access', refresh_token: 'secret-refresh', expires_in: 3600 }
const body = (request: HttpClientRequest.HttpClientRequest) => {
  if (request.body._tag !== 'Uint8Array') throw new Error('Expected JSON request')
  return Schema.decodeUnknownSync(Schema.JsonObject)(
    JSON.parse(new TextDecoder().decode(request.body.body)),
  )
}
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
const old = (expiresAt = 0): OpaqueOAuth => ({
  kind: 'opaqueOAuth',
  provider: 'anthropic',
  authorizationServer: OAuth.authorizationServer,
  clientId: OAuth.clientId,
  accessToken: Redacted.make('old-access'),
  refreshToken: Redacted.make('old-refresh'),
  scopes: ['user:inference'],
  expiresAt,
})

describe('Pi-compatible Anthropic consent', () => {
  it.effect('interruption during browser begin cancels the acquired pending attempt', () => {
    const f = fixture()
    return Effect.gen(function* () {
      const auth = yield* OAuth.OAuth
      const started = yield* Deferred.make<OAuth.Authorization>()
      const release = yield* Deferred.make<void>()
      let cancelled = 0
      const delayed = OAuth.OAuth.of({
        ...auth,
        begin: (options) =>
          auth.begin(options).pipe(
            Effect.tap((authorization) => Deferred.succeed(started, authorization)),
            Effect.tap(() => Deferred.await(release)),
          ),
        cancel: (state) =>
          auth.cancel(state).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                cancelled++
              }),
            ),
          ),
      })
      const server = HttpServer.make({
        address: Result.getOrThrow(NetAddress.inetAddressV4(NetAddress.ipv4Loopback, 53692)),
        serve: () => Effect.void,
      })
      const layer = OAuth.layerCallback({ account: 'key' }).pipe(
        Layer.provide(Layer.succeed(OAuth.OAuth, delayed)),
        Layer.provide(Layer.succeed(HttpServer.HttpServer, server)),
      )
      const owner = yield* Effect.forkChild(Effect.scoped(Layer.build(layer)))
      const authorization = yield* Deferred.await(started)
      const interrupted = yield* Effect.forkChild(Fiber.interrupt(owner), {
        startImmediately: true,
      })
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(interrupted)
      assert.strictEqual(cancelled, 1)
      assert.strictEqual(
        (yield* auth.complete(authorization.state, 'code').pipe(Effect.flip)).reason._tag,
        'AuthCallbackError',
      )
      assert.strictEqual(f.requests.length, 0)
    }).pipe(Effect.provide(f.layer))
  })

  it.effect(
    'browser and copy-code URLs preserve pinned PKCE/scopes and redact verifier-state',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const auth = yield* OAuth.OAuth
        for (const method of ['browser', 'copyCode'] as const) {
          const attempt = yield* auth.begin({ account: 'my-account', method })
          const url = new URL(Redacted.value(attempt.url))
          const state = Redacted.value(attempt.state)
          assert.strictEqual(url.origin + url.pathname, OAuth.authorizeUrl)
          assert.strictEqual(url.searchParams.get('client_id'), OAuth.clientId)
          assert.strictEqual(url.searchParams.get('code'), 'true')
          assert.strictEqual(url.searchParams.get('scope'), OAuth.scopes.join(' '))
          assert.strictEqual(url.searchParams.get('state'), state)
          assert.strictEqual(
            url.searchParams.get('redirect_uri'),
            method === 'browser' ? OAuth.browserRedirectUri : OAuth.copyCodeRedirectUri,
          )
          const crypto = yield* Crypto.Crypto
          const digest = yield* crypto.digest('SHA-256', new TextEncoder().encode(state))
          assert.strictEqual(url.searchParams.get('code_challenge'), Pkce.base64Url(digest))
          assert.isFalse(JSON.stringify(attempt).includes(state))
          const credential = yield* auth.complete(attempt.state, `private-code#${state}`)
          assert.strictEqual(credential.kind, 'opaqueOAuth')
          assert.strictEqual(credential.expiresAt, (yield* Clock.currentTimeMillis) + 3600000)
          assert.isFalse('subject' in credential)
          const request = f.requests.at(-1)
          if (request === undefined) return yield* Effect.die('Missing request')
          assert.strictEqual(request.url, OAuth.tokenUrl)
          assert.deepEqual(body(request), {
            grant_type: 'authorization_code',
            client_id: OAuth.clientId,
            code: 'private-code',
            state,
            code_verifier: state,
            redirect_uri: attempt.redirectUri,
          })
          assert.strictEqual(request.headers['content-type'], 'application/json')
          assert.strictEqual(
            (yield* auth.complete(attempt.state, 'again').pipe(Effect.flip)).code,
            'callback',
          )
        }
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'wrong state/address, duplicate callback fields, empty code, expiry and cancellation do not exchange',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const auth = yield* OAuth.OAuth
        const attempt = yield* auth.begin({ account: 'key' })
        const state = Redacted.value(attempt.state)
        for (const input of [
          `code#wrong`,
          `https://evil.example/callback?code=c&state=${state}`,
          `${attempt.redirectUri}?code=c&state=${state}&code=d`,
          '',
          `${attempt.redirectUri}?code=c`,
        ])
          assert.strictEqual(
            (yield* auth.complete(attempt.state, input).pipe(Effect.flip)).code,
            'callback',
          )
        assert.strictEqual(
          (yield* auth
            .complete(attempt.state, `${attempt.redirectUri}?error=access_denied&state=${state}`)
            .pipe(Effect.flip)).code,
          'denied',
        )
        yield* auth.cancel(attempt.state)
        assert.strictEqual(
          (yield* auth.complete(attempt.state, 'code').pipe(Effect.flip)).code,
          'callback',
        )
        const expiring = yield* auth.begin({ account: 'key' })
        yield* TestClock.adjust('11 seconds')
        assert.strictEqual(
          (yield* auth.complete(expiring.state, 'code').pipe(Effect.flip)).code,
          'expired',
        )
        assert.strictEqual(f.requests.length, 0)
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'explicit manual raw code and full callback URLs work; an in-flight attempt exchanges only once',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const auth = yield* OAuth.OAuth
        const attempt = yield* auth.begin({ account: 'key' })
        const url = `${attempt.redirectUri}?code=code&state=${Redacted.value(attempt.state)}`
        const outcomes = yield* Effect.all(
          [
            auth.complete(attempt.state, url).pipe(Effect.result),
            auth.complete(attempt.state, url).pipe(Effect.result),
          ],
          { concurrency: 'unbounded' },
        )
        assert.strictEqual(outcomes.filter((value) => value._tag === 'Success').length, 1)
        assert.strictEqual(f.requests.length, 1)
        const manual = yield* auth.begin({ account: 'key', method: 'copyCode' })
        yield* auth.complete(manual.state, 'manual-code')
        const store = yield* CredentialStore
        assert.isTrue(Option.isSome(yield* store.get('key')))
        yield* auth.signOut('key')
        assert.strictEqual((yield* auth.accessToken('key').pipe(Effect.flip)).code, 'missing')
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'locked refresh rotates once for concurrent turns and preserves old credentials on failed refresh',
    () => {
      let reject = false
      const f = fixture((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json(reject ? { error: 'private-error-with-token' } : token, {
              status: reject ? 401 : 200,
            }),
          ),
        ),
      )
      return Effect.gen(function* () {
        const store = yield* CredentialStore
        const auth = yield* OAuth.OAuth
        yield* store.set('key', old())
        const refreshed = yield* Effect.all([auth.accessToken('key'), auth.accessToken('key')], {
          concurrency: 'unbounded',
        })
        assert.strictEqual(f.requests.length, 1)
        assert.deepEqual(body(f.requests[0]!), {
          grant_type: 'refresh_token',
          client_id: OAuth.clientId,
          refresh_token: 'old-refresh',
        })
        assert.strictEqual(Redacted.value(refreshed[0]!), 'secret-access')
        const saved = yield* store.get('key')
        reject = true
        const error = yield* auth.refresh('key', { force: true }).pipe(Effect.flip)
        assert.strictEqual(error.code, 'token')
        assert.isFalse(JSON.stringify(error).includes('private-'))
        assert.deepEqual(yield* store.get('key'), saved)
      }).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'malformed grants, missing inference scope, invalid type/lifetime and failed exchange stay redacted and retain old grants',
    () => {
      const cases: ReadonlyArray<unknown> = [
        { ...token, access_token: '' },
        { ...token, expires_in: -1 },
        { ...token, expires_in: Number.MAX_VALUE },
        { ...token, token_type: 'Basic' },
        { ...token, scope: 'user:profile' },
      ]
      return Effect.forEach(cases, (response) => {
        const f = fixture((request) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(response))),
        )
        return Effect.gen(function* () {
          const store = yield* CredentialStore
          const auth = yield* OAuth.OAuth
          yield* store.set('key', old())
          const error = yield* auth.refresh('key', { force: true }).pipe(Effect.flip)
          assert.isTrue(error.code === 'protocol' || error.reason._tag === 'AuthPermissionError')
          assert.isFalse(JSON.stringify(error).includes('secret-'))
          assert.deepEqual(yield* store.get('key'), Option.some(old()))
        }).pipe(Effect.provide(f.layer))
      })
    },
  )
  it.effect(
    'caller interruption interrupts pending native HTTP token work without overwriting the vault',
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        let interrupted = false
        const f = fixture(() =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(() => {
                interrupted = true
              }),
            ),
          ),
        )
        yield* Effect.gen(function* () {
          const store = yield* CredentialStore
          const auth = yield* OAuth.OAuth
          yield* store.set('key', old())
          const running = yield* Effect.forkChild(auth.refresh('key', { force: true }))
          yield* Deferred.await(started)
          yield* Fiber.interrupt(running)
          assert.isTrue(interrupted)
          assert.deepEqual(yield* store.get('key'), Option.some(old()))
        }).pipe(Effect.provide(f.layer))
      }),
  )
  it.effect(
    'opt-in scoped native loopback callback validates requests and settles a real consent exchange',
    () => {
      const f = fixture()
      return Effect.scoped(
        Effect.gen(function* () {
          const nativeServer = NodeHttpServer.layer(createServer, {
            host: '127.0.0.1',
            port: 53692,
            gracefulShutdownTimeout: '1 second',
          })
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
          assert.strictEqual((yield* callback.await).kind, 'opaqueOAuth')
          assert.strictEqual(f.requests.length, 1)
        }),
      ).pipe(Effect.provide(f.layer))
    },
  )
  it.effect('duplicate browser callbacks await the original in-flight exchange', () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const f = fixture((request) =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(HttpClientResponse.fromWeb(request, Response.json(token))),
        ),
      )
      let handler:
        | Effect.Effect<
            HttpServerResponse.HttpServerResponse,
            never,
            HttpServerRequest.HttpServerRequest | Scope.Scope
          >
        | undefined
      const server = HttpServer.make({
        address: Result.getOrThrow(NetAddress.inetAddressV4(NetAddress.ipv4Loopback, 53692)),
        // Native HttpServer boundary erases the application error channel.
        // oxlint-disable effecttsgo/any-unknown-in-error-context
        serve: (effect) =>
          Effect.sync(() => {
            handler = effect.pipe(Effect.orDie)
          }),
        // oxlint-enable effecttsgo/any-unknown-in-error-context
      })
      yield* Effect.gen(function* () {
        const callback = Context.get(
          yield* Layer.build(
            OAuth.layerCallback({ account: 'browser-key' }).pipe(
              Layer.provide(Layer.succeed(HttpServer.HttpServer, server)),
            ),
          ),
          OAuth.Callback,
        )
        const request = Effect.suspend(() => {
          if (handler === undefined) return Effect.die('Callback was not started')
          const url = `${OAuth.browserRedirectUri}?code=c&state=${Redacted.value(callback.authorization.state)}`
          return handler.pipe(
            Effect.provideService(
              HttpServerRequest.HttpServerRequest,
              HttpServerRequest.fromWeb(new Request(url)),
            ),
          )
        })
        const first = yield* Effect.forkChild(request)
        yield* Deferred.await(started)
        const duplicate = yield* Effect.forkChild(request)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual((yield* Fiber.join(first)).status, 200)
        const owner = yield* callback.await.pipe(Effect.result)
        assert.strictEqual(owner._tag, 'Success')
        assert.strictEqual((yield* Fiber.join(duplicate)).status, 200)
        assert.strictEqual((yield* callback.await).kind, 'opaqueOAuth')
        assert.strictEqual(f.requests.length, 1)
        assert.isTrue(Option.isSome(yield* (yield* CredentialStore).get('browser-key')))
      }).pipe(Effect.provide(f.layer))
    }),
  )

  it.effect(
    'callback rejects an incorrect bind address and pending browser wait expires with native Clock',
    () => {
      const f = fixture()
      return Effect.scoped(
        Effect.gen(function* () {
          const wrong = HttpServer.make({
            address: Result.getOrThrow(NetAddress.inetAddressV4(NetAddress.ipv4Unspecified, 53692)),
            serve: () => Effect.void,
          })
          const error = yield* Layer.build(
            OAuth.layerCallback({ account: 'key' }).pipe(
              Layer.provide(Layer.succeed(HttpServer.HttpServer, wrong)),
            ),
          ).pipe(Effect.flip)
          assert.strictEqual(error.code, 'configuration')
          const right = HttpServer.make({
            address: Result.getOrThrow(NetAddress.inetAddressV4(NetAddress.ipv4Loopback, 53692)),
            serve: () => Effect.void,
          })
          const context = yield* Layer.build(
            OAuth.layerCallback({ account: 'key' }).pipe(
              Layer.provide(Layer.succeed(HttpServer.HttpServer, right)),
            ),
          )
          const callback = Context.get(context, OAuth.Callback)
          const waiting = yield* Effect.forkChild(callback.await.pipe(Effect.flip))
          yield* TestClock.adjust('11 seconds')
          assert.strictEqual((yield* Fiber.join(waiting)).code, 'expired')
          const auth = yield* OAuth.OAuth
          assert.strictEqual(
            (yield* auth.complete(callback.authorization.state, 'code').pipe(Effect.flip)).code,
            'callback',
          )
          assert.strictEqual(f.requests.length, 0)
        }),
      ).pipe(Effect.provide(f.layer))
    },
  )
  it.effect(
    'network and malformed JSON failures omit raw token endpoint details and keep the stored grant',
    () => {
      const replies = [
        (request: HttpClientRequest.HttpClientRequest) =>
          Effect.fail(
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request,
                description: 'secret-reflected-refresh',
              }),
            }),
          ),
        (request: HttpClientRequest.HttpClientRequest) =>
          Effect.succeed(HttpClientResponse.fromWeb(request, new Response('secret-invalid-json'))),
      ]
      return Effect.forEach(replies, (reply) => {
        const f = fixture(reply)
        return Effect.gen(function* () {
          const auth = yield* OAuth.OAuth
          const store = yield* CredentialStore
          yield* store.set('key', old())
          const error = yield* auth.refresh('key', { force: true }).pipe(Effect.flip)
          assert.isTrue(error.reason._tag === 'AuthNetworkError' || error.code === 'protocol')
          assert.isFalse(JSON.stringify(error).includes('secret-'))
          assert.deepEqual(yield* store.get('key'), Option.some(old()))
        }).pipe(Effect.provide(f.layer))
      })
    },
  )
  it.effect(
    '30-second token timeout releases the refresh lock and interrupts native HTTP work',
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        let interrupted = false
        const f = fixture(() =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(() => {
                interrupted = true
              }),
            ),
          ),
        )
        yield* Effect.gen(function* () {
          const auth = yield* OAuth.OAuth
          const store = yield* CredentialStore
          yield* store.set('key', old())
          const waiting = yield* Effect.forkChild(
            auth.refresh('key', { force: true }).pipe(Effect.flip),
          )
          yield* Deferred.await(started)
          yield* TestClock.adjust('31 seconds')
          assert.strictEqual((yield* Fiber.join(waiting)).code, 'network')
          assert.isTrue(interrupted)
          assert.deepEqual(yield* store.get('key'), Option.some(old()))
          yield* store.remove('key')
        }).pipe(Effect.provide(f.layer))
      }),
  )
  it.effect(
    'whole-token deadline aborts a stalled body after headers, redacts partial secrets and leaves refresh usable',
    () =>
      Effect.gen(function* () {
        const headers = yield* Deferred.make<void>()
        let bodyStarted = false
        let aborted = false
        let calls = 0
        const f = fixture((request, signal) => {
          calls++
          if (calls > 1)
            return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json(token)))
          const stalled = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode('{"access_token":"secret-partial-body'))
              signal.addEventListener(
                'abort',
                () => {
                  aborted = true
                  controller.error(new Error('secret-aborted-body'))
                },
                { once: true },
              )
            },
            pull() {
              bodyStarted = true
            },
          })
          return Deferred.succeed(headers, undefined).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new Response(stalled, { headers: { 'content-type': 'application/json' } }),
              ),
            ),
          )
        })
        yield* Effect.gen(function* () {
          const auth = yield* OAuth.OAuth
          const store = yield* CredentialStore
          yield* store.set('key', old())
          const waiting = yield* Effect.forkChild(
            auth.refresh('key', { force: true }).pipe(Effect.flip),
          )
          yield* Deferred.await(headers)
          yield* TestClock.adjust('31 seconds')
          const error = yield* Fiber.join(waiting)
          assert.strictEqual(error.code, 'network')
          assert.strictEqual(error.message, 'Anthropic token request timed out')
          assert.isTrue(bodyStarted)
          assert.isTrue(aborted)
          assert.isFalse(JSON.stringify(error).includes('secret-'))
          assert.deepEqual(yield* store.get('key'), Option.some(old()))
          const recovered = yield* auth.refresh('key', { force: true })
          assert.strictEqual(Redacted.value(recovered.accessToken), token.access_token)
          assert.strictEqual(f.requests.length, 2)
        }).pipe(Effect.provide(f.layer))
      }),
  )
})

describe('Anthropic wrapped secret state ownership', () => {
  it.effect('closing the authorization service scope discards wrapped pending attempts', () => {
    const f = fixture()
    return Effect.gen(function* () {
      const acquired = yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(f.layer)
          const auth = Context.get(context, OAuth.OAuth)
          const attempt = yield* auth.begin({ account: 'closed-scope' })
          return { auth, attempt }
        }),
      )
      const error = yield* acquired.auth.complete(acquired.attempt.state, 'code').pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'AuthCallbackError')
      assert.strictEqual(f.requests.length, 0)
    })
  })

  it.effect(
    'pending state is redacted, reconstructed equal keys complete once and cancel by value',
    () => {
      const f = fixture()
      return Effect.gen(function* () {
        const auth = yield* OAuth.OAuth
        const insert = vi.mocked(HashMap.set)
        insert.mockClear()
        const attempt = yield* auth.begin({ account: 'wrapped-key' })
        const rawState = Redacted.value(attempt.state)
        const call = insert.mock.calls.find((args) => Redacted.isRedacted(args[1]))
        assert.isDefined(call)
        if (call === undefined) return yield* Effect.die('Expected redacted pending map key')
        assert.strictEqual(call[1], attempt.state)
        const pending = HashMap.set(call[0], call[1], call[2])
        assert.isFalse(Inspectable.toStringUnknown(pending).includes(rawState))
        assert.isFalse(JSON.stringify(pending).includes(rawState))
        const reconstructed = Redacted.make(rawState)
        const saved = yield* auth.complete(reconstructed, `wrapped-code#${rawState}`)
        assert.strictEqual(saved.kind, 'opaqueOAuth')
        assert.strictEqual(
          (yield* auth.complete(Redacted.make(rawState), 'again').pipe(Effect.flip)).reason._tag,
          'AuthCallbackError',
        )
        const wire = f.requests[0]
        if (wire === undefined) return yield* Effect.die('Expected JSON token request')
        assert.strictEqual(wire.headers['content-type'], 'application/json')
        const serialized = body(wire)
        assert.strictEqual(serialized.code, 'wrapped-code')
        assert.strictEqual(serialized.state, rawState)
        assert.strictEqual(serialized.code_verifier, rawState)
        const cancelled = yield* auth.begin({ account: 'cancelled' })
        yield* auth.cancel(Redacted.make(Redacted.value(cancelled.state)))
        assert.strictEqual(
          (yield* auth.complete(cancelled.state, 'again').pipe(Effect.flip)).reason._tag,
          'AuthCallbackError',
        )
        assert.strictEqual(f.requests.length, 1)
      }).pipe(Effect.provide(f.layer))
    },
  )
})
