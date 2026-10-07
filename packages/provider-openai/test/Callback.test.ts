import * as Time from '@effect-harness/auth/Time'
import {
  AuthCallbackError,
  AuthDeniedError,
  AuthError,
  type OAuth,
} from '@effect-harness/auth/Credential'
import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import type * as Scope from 'effect/Scope'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpServerRequest from 'effect/http/HttpServerRequest'
import type * as HttpServerResponse from 'effect/http/HttpServerResponse'
import * as NetAddress from 'effect/net/NetAddress'
import * as TestClock from 'effect/testing/TestClock'
import * as Callback from '@effect-harness/provider-openai/Callback'
import * as ChatGpt from '@effect-harness/provider-openai/ChatGpt'

const credential: OAuth = {
  _tag: 'oauth',
  provider: 'openai',
  issuer: ChatGpt.issuer,
  subject: 'subject',
  clientId: 'issued',
  hostId: 'urn:uuid:test',
  accessToken: Redacted.make('access'),
  refreshToken: Redacted.make('refresh'),
  idToken: Redacted.make('id'),
  expiresAt: Time.fromEpochMillis(3600000),
  scopes: [ChatGpt.directScope],
}
const makeFixture = (
  address = '127.0.0.1:43210',
  exchange?: Effect.Effect<OAuth, AuthError>,
  afterBegin: Effect.Effect<void> = Effect.void,
) => {
  let handler:
    | Effect.Effect<
        HttpServerResponse.HttpServerResponse,
        never,
        HttpServerRequest.HttpServerRequest | Scope.Scope
      >
    | undefined
  let active = false
  let cancelled = 0
  const cancelledStates: Array<string> = []
  let completed = 0
  // Native HttpServer.make deliberately erases the application's error type at its server boundary.
  const server = HttpServer.make({
    address: NetAddress.socketAddressFromInputUnsafe(address),
    // oxlint-disable effecttsgo/any-unknown-in-error-context
    serve: (effect) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          handler = effect.pipe(Effect.orDie)
          active = true
        }),
        () =>
          Effect.sync(() => {
            active = false
          }),
      ).pipe(Effect.asVoid),
    // oxlint-enable effecttsgo/any-unknown-in-error-context
  })
  const auth = ChatGpt.ChatGpt.of({
    begin: ({ redirectUri }) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now) => ({
          url: Redacted.make('https://auth.openai.com/authorize'),
          state: 'expected',
          redirectUri,
          expiresAt: Time.fromEpochMillis(now + 60000),
        })),
        Effect.tap(() => afterBegin),
      ),
    complete: (url) =>
      Effect.sync(() => {
        completed++
        return url
      }).pipe(
        Effect.flatMap((url) => {
          if (exchange !== undefined)
            return completed === 1
              ? exchange
              : Effect.fail(
                  new AuthError({
                    reason: new AuthCallbackError({ message: 'Consumed authorization state' }),
                  }),
                )
          return url.includes('error=')
            ? Effect.fail(
                new AuthError({
                  reason: new AuthDeniedError({ message: 'Private server diagnostic' }),
                }),
              )
            : Effect.succeed(credential)
        }),
      ),
    refresh: () => Effect.succeed(credential),
    accessToken: () => Effect.succeed(credential.accessToken),
    models: () => Effect.succeed([]),
    signOut: () => Effect.void,
    cancel: (state) =>
      Effect.sync(() => {
        cancelled++
        cancelledStates.push(state)
      }),
  })
  const layer = Callback.layer().pipe(
    Layer.provide(Layer.succeed(HttpServer.HttpServer, server)),
    Layer.provide(Layer.succeed(ChatGpt.ChatGpt, auth)),
  )
  const request = (url: string, method = 'GET') =>
    Effect.suspend(() =>
      handler === undefined
        ? Effect.die('Listener not started')
        : handler.pipe(
            Effect.provideService(
              HttpServerRequest.HttpServerRequest,
              HttpServerRequest.fromWeb(new Request(url, { method })),
            ),
          ),
    )
  return {
    layer,
    request,
    active: () => active,
    cancelled: () => cancelled,
    cancelledStates: () => cancelledStates,
    completed: () => completed,
  }
}

describe('Callback', () => {
  describe('scoped callback receiver', () => {
    it.effect(
      'interruption during begin waits for acquisition and cancels its exact pending state',
      () =>
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const f = makeFixture(
            '127.0.0.1:43210',
            undefined,
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
          )
          const owner = yield* Effect.forkChild(Effect.scoped(Layer.build(f.layer)))
          yield* Deferred.await(started)
          const interrupted = yield* Effect.forkChild(Fiber.interrupt(owner), {
            startImmediately: true,
          })
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(interrupted)
          assert.strictEqual(f.cancelled(), 1)
          assert.deepStrictEqual(f.cancelledStates(), ['expected'])
          assert.isFalse(f.active())
        }),
    )

    it.effect(
      'starts before exposing URL, checks method/path/state and tears down with its scope',
      () =>
        Effect.gen(function* () {
          const f = makeFixture()
          return yield* Effect.gen(function* () {
            yield* Effect.scoped(
              Effect.gen(function* () {
                const context = yield* Layer.build(f.layer)
                const receiver = Context.get(context, Callback.Callback)
                assert.isTrue(f.active())
                assert.strictEqual(
                  receiver.authorization.redirectUri,
                  'http://127.0.0.1:43210/auth/callback',
                )
                assert.strictEqual((yield* f.request('http://127.0.0.1:43210/other')).status, 404)
                assert.strictEqual(
                  (yield* f.request('http://127.0.0.1:43210/auth/callback?state=expected', 'POST'))
                    .status,
                  404,
                )
                assert.strictEqual(
                  (yield* f.request('http://127.0.0.1:43210/auth/callback?state=wrong&code=secret'))
                    .status,
                  400,
                )
                assert.strictEqual(f.completed(), 0)
                const response = yield* f.request(
                  'http://127.0.0.1:43210/auth/callback?state=expected&code=secret',
                )
                assert.strictEqual(response.status, 200)
                assert.strictEqual(response.headers['cache-control'], 'no-store')
                assert.isFalse(JSON.stringify(response).includes('secret'))
                assert.deepStrictEqual(yield* receiver.await, credential)
              }),
            )
            assert.isFalse(f.active())
            assert.strictEqual(f.cancelled(), 1)
          })
        }),
    )

    it.effect(
      'duplicate matching callbacks share the claimed exchange and cannot overwrite its result',
      () =>
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const f = makeFixture(
            undefined,
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(credential),
            ),
          )
          const receiver = Context.get(yield* Layer.build(f.layer), Callback.Callback)
          const url = 'http://127.0.0.1:43210/auth/callback?state=expected&code=secret'
          const first = yield* Effect.forkChild(f.request(url))
          yield* Deferred.await(started)
          const duplicate = yield* Effect.forkChild(f.request(url))
          yield* Effect.yieldNow
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* Fiber.join(first)).status, 200)
          const owner = yield* receiver.await.pipe(Effect.result)
          assert.strictEqual(owner._tag, 'Success')
          assert.strictEqual((yield* Fiber.join(duplicate)).status, 200)
          assert.strictEqual(f.completed(), 1)
          assert.deepStrictEqual(yield* receiver.await, credential)
        }),
    )

    it.effect('returns sanitized failure to browser and typed denial to owner', () =>
      Effect.gen(function* () {
        const f = makeFixture()
        return yield* Effect.gen(function* () {
          const receiver = Context.get(yield* Layer.build(f.layer), Callback.Callback)
          const response = yield* f.request(
            'http://127.0.0.1:43210/auth/callback?state=expected&error=access_denied',
          )
          assert.strictEqual(response.status, 400)
          assert.isFalse(JSON.stringify(response).includes('Private server diagnostic'))
          assert.strictEqual((yield* receiver.await.pipe(Effect.flip)).code, 'denied')
        })
      }),
    )

    it.effect('expiration releases the pending authorization instead of waiting forever', () =>
      Effect.gen(function* () {
        const f = makeFixture()
        return yield* Effect.gen(function* () {
          const receiver = Context.get(yield* Layer.build(f.layer), Callback.Callback)
          const waiter = yield* Effect.forkChild(receiver.await.pipe(Effect.flip))
          yield* TestClock.adjust('61 seconds')
          assert.strictEqual((yield* Fiber.join(waiter)).code, 'expired')
          assert.strictEqual(f.cancelled(), 1)
        })
      }),
    )

    it.effect('rejects a server listening on a non-loopback interface', () =>
      Effect.gen(function* () {
        const f = makeFixture('0.0.0.0:43210')
        return yield* Effect.gen(function* () {
          const error = yield* Layer.build(f.layer).pipe(Effect.flip)
          assert.strictEqual(error.code, 'configuration')
          assert.isFalse(f.active())
        })
      }),
    )
  })
})
