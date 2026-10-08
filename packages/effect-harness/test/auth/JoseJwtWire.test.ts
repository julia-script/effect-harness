import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as JoseJwt from 'effect-harness/auth/JoseJwt'

describe('JoseJwtWire', () => {
  it('moved JWT and JWKS optional wire keys reject explicit undefined', () => {
    for (const key of ['kid', 'alg', 'use', 'n', 'e', 'crv', 'x', 'y'])
      assert.isTrue(
        Option.isNone(
          Schema.decodeOption(JoseJwt.KeySet)({ keys: [{ kty: 'RSA', [key]: undefined }] }),
        ),
        key,
      )
    for (const key of ['nonce', 'email'])
      assert.isTrue(
        Option.isNone(
          Schema.decodeOption(JoseJwt.Claims)({
            sub: 'subject',
            iss: 'issuer',
            exp: 0.5,
            [key]: undefined,
          }),
        ),
        key,
      )
    assert.isTrue(
      Option.isSome(
        Schema.decodeOption(JoseJwt.Claims)({ sub: 'subject', iss: 'issuer', exp: 0.5 }),
      ),
    )
  })
  it.effect(
    'moved Jose JWT implementation rejects malformed JWKS wire fields before verification',
    () =>
      Effect.gen(function* () {
        const http = HttpClient.make((request) => {
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              globalThis.Response.json({ keys: [{ kty: 'RSA', kid: null }] }),
            ),
          )
        })
        const jwt = yield* JoseJwt.make.pipe(Effect.provideService(HttpClient.HttpClient, http))
        const error = yield* jwt
          .verify(Redacted.make('invalid-token'), {
            issuer: 'https://auth.example',
            audience: 'client',
            jwksUrl: 'https://auth.example/jwks',
          })
          .pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'AuthIdentityError')
        assert.strictEqual(error.message, 'Invalid verification keys')
        assert.isTrue(Schema.isSchemaError(error.cause))
      }),
  )
})
