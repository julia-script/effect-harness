import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as JoseJwt from 'effect-harness/auth/JoseJwt'

describe('JoseJwtWire', () => {
  it.effect('moved JWT and JWKS optional wire keys reject explicit undefined', () =>
    Effect.gen(function* () {
      const keySets = new TestSchema.Asserts(JoseJwt.KeySet)
      for (const key of ['kid', 'alg', 'use', 'n', 'e', 'crv', 'x', 'y']) {
        yield* keySets
          .decoding()
          .failEffect(
            { keys: [{ kty: 'RSA', [key]: undefined }] },
            `Expected string\n  at ["keys"][0]["${key}"]`,
          )
      }
      const claims = new TestSchema.Asserts(JoseJwt.Claims)
      for (const key of ['nonce', 'email']) {
        yield* claims
          .decoding()
          .failEffect(
            { sub: 'subject', iss: 'issuer', exp: 0.5, [key]: undefined },
            `Expected string\n  at ["${key}"]`,
          )
      }
      yield* claims
        .decoding()
        .succeedEffect(
          { sub: 'subject', iss: 'issuer', exp: 0.5 },
          { sub: 'subject', iss: 'issuer', exp: 0.5 },
        )
    }),
  )
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
