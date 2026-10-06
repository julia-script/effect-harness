import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as BunPath from '@effect/platform-bun/BunPath'
import { assert, describe, it } from '@effect/vitest'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Inspectable from 'effect/Inspectable'
import * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import {
  AuthError,
  AuthErrorCode,
  AuthNetworkError,
  AuthPermissionError,
  AuthStorageError,
  AuthTokenError,
} from '../src/Credential.ts'
import * as Store from '../src/CredentialStore.ts'
import * as Token from '../src/Token.ts'

const grant = {
  access_token: 'access',
  refresh_token: 'refresh',
  expires_in: 3600,
  token_type: 'Bearer',
}

describe('AuthError structured reasons and provenance', () => {
  it.effect(
    'all legacy input codes construct tagged reasons with delegated messages and statuses',
    () =>
      Effect.gen(function* () {
        for (const code of AuthErrorCode.literals) {
          const error = AuthError.fromLegacy({
            reason: code,
            message: `exact ${code}`,
            status: 401,
          })
          assert.strictEqual(
            error.reason._tag,
            `Auth${code[0]?.toUpperCase()}${code.slice(1)}Error`,
          )
          assert.strictEqual(error.code, code)
          assert.strictEqual(error.message, `exact ${code}`)
          assert.strictEqual(error.status, 401)
          assert.strictEqual(error.cause, undefined)
          const wire = Schema.fromJsonString(Schema.toCodecJson(AuthError))
          const decoded = yield* Schema.decodeEffect(wire)(yield* Schema.encodeEffect(wire)(error))
          assert.strictEqual(decoded.reason._tag, error.reason._tag)
          assert.strictEqual(decoded.code, code)
          assert.strictEqual(decoded.message, error.message)
          assert.strictEqual(decoded.status, 401)
        }
      }),
  )

  it.effect(
    'explicit JSON codecs preserve Error provenance while ordinary inspection redacts it',
    () =>
      Effect.gen(function* () {
        const inner = new Error('private-inner-diagnostic')
        const caught = new TypeError('private-outer-diagnostic', { cause: inner })
        const reason = new AuthNetworkError({ message: 'Sanitized network failure', cause: caught })
        const error = new AuthError({ reason })
        assert.strictEqual(error.reason, reason)
        assert.strictEqual(error.cause, caught)
        assert.strictEqual(caught.cause, inner)
        assert.isFalse(JSON.stringify(error).includes('private-'))
        assert.isFalse(JSON.stringify(reason).includes('private-'))
        assert.isFalse(Inspectable.toStringUnknown(error).includes('private-'))
        assert.isFalse(Inspectable.toStringUnknown(reason).includes('private-'))
        const wire = Schema.fromJsonString(Schema.toCodecJson(AuthError))
        const encoded = yield* Schema.encodeEffect(wire)(error)
        assert.include(encoded, 'private-outer-diagnostic')
        assert.isFalse(encoded.includes('stack'))
        const decoded = yield* Schema.decodeEffect(wire)(encoded)
        assert.strictEqual(decoded.reason._tag, 'AuthNetworkError')
        assert.strictEqual(decoded.message, error.message)
        if (!(decoded.cause instanceof Error))
          return yield* Effect.die('Expected decoded Error cause')
        assert.strictEqual(decoded.cause.name, 'TypeError')
        assert.strictEqual(decoded.cause.message, caught.message)
        if (!(decoded.cause.cause instanceof Error))
          return yield* Effect.die('Expected nested Error cause')
        assert.strictEqual(decoded.cause.cause.message, inner.message)
        assert.notStrictEqual(decoded.cause, caught)
      }),
  )

  it('only documented transient reasons delegate retry permission and retry timing', () => {
    const after = Duration.seconds(7)
    const transient = new AuthError({
      reason: new AuthTokenError({ message: 'Rejected grant', status: 429, retryAfter: after }),
    })
    const permanent = new AuthError({
      reason: new AuthPermissionError({ message: '503 please retry', status: 503 }),
    })
    const uncertain = new AuthError({
      reason: new AuthStorageError({ message: 'I/O failed', status: 503 }),
    })
    assert.isTrue(transient.isRetryable)
    assert.strictEqual(transient.retryAfter, after)
    assert.isTrue(
      new AuthError({ reason: new AuthTokenError({ message: 'Server fault', status: 503 }) })
        .isRetryable,
    )
    assert.isFalse(
      new AuthError({ reason: new AuthTokenError({ message: 'Invalid grant', status: 401 }) })
        .isRetryable,
    )
    assert.isFalse(
      new AuthError({ reason: new AuthTokenError({ message: 'Unknown status', status: 600 }) })
        .isRetryable,
    )
    assert.isFalse(permanent.isRetryable)
    assert.isFalse(uncertain.isRetryable)
  })

  it.effect(
    'foreign non-Error causes retain runtime identity and use the explicit defect codec',
    () =>
      Effect.gen(function* () {
        for (const caught of ['foreign failure', { detail: 'foreign detail' }, null]) {
          const error = new AuthError({
            reason: new AuthNetworkError({ message: 'Sanitized network failure', cause: caught }),
          })
          assert.strictEqual(error.cause, caught)
          const wire = Schema.fromJsonString(Schema.toCodecJson(AuthError))
          const decoded = yield* Schema.decodeEffect(wire)(yield* Schema.encodeEffect(wire)(error))
          assert.deepStrictEqual(decoded.cause, caught)
        }
      }),
  )

  it.effect(
    'token and revocation transport failures retain the exact caught HTTP error without ordinary disclosure',
    () =>
      Effect.gen(function* () {
        const request = HttpClientRequest.post('https://auth.example/token').pipe(
          HttpClientRequest.bodyUrlParams({ refresh_token: 'private-refresh' }),
        )
        const caught = new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            description: 'private-reflected-diagnostic',
            cause: new Error('wire failure'),
          }),
        })
        const http = HttpClient.make(() => Effect.fail(caught))
        for (const operation of [
          Token.request('https://auth.example/token', { refresh_token: 'private-refresh' }),
          Token.revoke('https://auth.example/revoke', { token: 'private-refresh' }),
        ]) {
          const error = yield* operation.pipe(
            Effect.provideService(HttpClient.HttpClient, http),
            Effect.flip,
          )
          assert.strictEqual(error.reason._tag, 'AuthNetworkError')
          assert.strictEqual(error.cause, caught)
          assert.isTrue(error.isRetryable)
          assert.isFalse(JSON.stringify(error).includes('private-'))
          assert.strictEqual(caught.reason.description, 'private-reflected-diagnostic')
        }
      }),
  )

  it.effect(
    'schema rejection retains its real cause while grant validation has no invented exception',
    () =>
      Effect.gen(function* () {
        const malformed = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(request, Response.json({ access_token: 'access' })),
          ),
        )
        const rejected = yield* Token.request('https://auth.example/token', {}).pipe(
          Effect.provideService(HttpClient.HttpClient, malformed),
          Effect.flip,
        )
        assert.strictEqual(rejected.reason._tag, 'AuthProtocolError')
        assert.isTrue(Schema.isSchemaError(rejected.cause))
        const unsupported = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(request, Response.json({ ...grant, token_type: 'Basic' })),
          ),
        )
        const validation = yield* Token.request('https://auth.example/token', {}).pipe(
          Effect.provideService(HttpClient.HttpClient, unsupported),
          Effect.flip,
        )
        assert.strictEqual(validation.reason._tag, 'AuthProtocolError')
        assert.strictEqual(validation.cause, undefined)
      }),
  )

  it.effect('protected-store platform failures retain the exact cause and stay permanent', () =>
    Effect.gen(function* () {
      const caught = new PlatformError.PlatformError(
        new PlatformError.SystemError({
          _tag: 'PermissionDenied',
          module: 'FileSystem',
          method: 'makeDirectory',
          description: 'private-filesystem-detail',
        }),
      )
      const fs = FileSystem.makeNoop({ makeDirectory: () => Effect.fail(caught) })
      const layer = Store.layerProtectedFile({ path: '/virtual-private/credentials.json' }).pipe(
        Layer.provide(
          Layer.mergeAll(BunPath.layer, BunCrypto.layer, Layer.succeed(FileSystem.FileSystem, fs)),
        ),
      )
      const error = yield* Layer.build(layer).pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'AuthStorageError')
      assert.strictEqual(error.cause, caught)
      assert.isFalse(error.isRetryable)
      assert.isFalse(JSON.stringify(error).includes('private-filesystem-detail'))
    }),
  )
})
