import * as DateTime from 'effect/DateTime'
import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import { exportJWK, generateKeyPair, SignJWT } from 'jose'
import * as Jwt from 'effect-harness/auth/Jwt'
import * as JoseJwt from 'effect-harness/auth/JoseJwt'

describe('JoseJwt', () => {
  it.effect('JWKS reads observe key rotation and recover after a failed observation', () =>
    Effect.gen(function* () {
      const first = yield* Effect.tryPromise(() => generateKeyPair('RS256', { extractable: true }))
      const second = yield* Effect.tryPromise(() => generateKeyPair('RS256', { extractable: true }))
      const keys = yield* Effect.tryPromise(() =>
        Promise.all([exportJWK(first.publicKey), exportJWK(second.publicKey)]),
      )
      const expiry = (yield* Clock.currentTimeMillis) / 1000 + 3600.00025
      const sign = (privateKey: typeof first.privateKey, kid: string) =>
        Effect.tryPromise(() =>
          new SignJWT({ sub: kid })
            .setProtectedHeader({ alg: 'RS256', kid })
            .setIssuer('https://issuer.test')
            .setAudience('client')
            .setExpirationTime(expiry)
            .sign(privateKey),
        )
      const tokens = [
        Redacted.make(yield* sign(first.privateKey, 'first')),
        Redacted.make(yield* sign(second.privateKey, 'second')),
      ]
      let reads = 0
      const http = HttpClient.make((request) => {
        const index = reads++
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            Response.json(
              index === 1
                ? { keys: [] }
                : {
                    keys: [{ ...keys[index === 0 ? 0 : 1], kid: index === 0 ? 'first' : 'second' }],
                  },
            ),
          ),
        )
      })
      const verifier = yield* JoseJwt.make.pipe(Effect.provideService(HttpClient.HttpClient, http))
      const options = {
        issuer: 'https://issuer.test',
        audience: 'client',
        jwksUrl: 'https://issuer.test/jwks',
      }
      const identity = yield* verifier.verify(tokens[0]!, options)
      assert.strictEqual(identity.sub, 'first')
      assert.isTrue(Jwt.isIdentity(identity))
      assert.strictEqual(DateTime.toEpochMillis(identity.exp), expiry * 1000)
      assert.isFalse(JoseJwt.isClaims(identity))
      assert.deepStrictEqual(yield* Schema.encodeEffect(Jwt.Identity)(identity), {
        sub: 'first',
        iss: 'https://issuer.test',
        exp: expiry * 1000,
      })
      assert.isFalse(Object.hasOwn(identity, 'nonce'))
      assert.isFalse(Object.hasOwn(identity, 'email'))
      assert.strictEqual(
        (yield* verifier.verify(tokens[1]!, options).pipe(Effect.flip)).reason._tag,
        'AuthIdentityError',
      )
      assert.strictEqual((yield* verifier.verify(tokens[1]!, options)).sub, 'second')
      assert.strictEqual(reads, 3)
    }),
  )
  it.effect(
    'JWT boundary verifies signature, issuer, audience, expiration, nonce and subject',
    () =>
      Effect.gen(function* () {
        const pair = yield* Effect.tryPromise(() => generateKeyPair('RS256', { extractable: true }))
        const key = yield* Effect.tryPromise(() => exportJWK(pair.publicKey))
        const now = Math.floor((yield* Clock.currentTimeMillis) / 1000)
        const sign = (overrides: Record<string, unknown>) =>
          Effect.tryPromise(() =>
            new SignJWT({ sub: 'verified', nonce: 'expected', ...overrides })
              .setProtectedHeader({ alg: 'RS256', kid: 'test' })
              .setIssuer('https://issuer.test')
              .setAudience('issued-client')
              .setExpirationTime(now + 3600)
              .sign(pair.privateKey),
          )
        const http = HttpClient.make((req) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              req,
              new Response(JSON.stringify({ keys: [{ ...key, kid: 'test' }] })),
            ),
          ),
        )
        const verifier = yield* Jwt.Jwt.pipe(
          Effect.provide(
            JoseJwt.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http))),
          ),
        )
        const signed = Redacted.make(yield* sign({}))
        const options = {
          issuer: 'https://issuer.test',
          audience: 'issued-client',
          jwksUrl: 'https://issuer.test/jwks',
          nonce: 'expected',
        }
        assert.strictEqual((yield* verifier.verify(signed, options)).sub, 'verified')
        for (const bad of [
          { ...options, nonce: 'wrong' },
          { ...options, issuer: 'https://other.test' },
          { ...options, audience: 'wrong' },
        ])
          assert.strictEqual(
            (yield* verifier.verify(signed, bad).pipe(Effect.flip)).reason._tag,
            'AuthIdentityError',
          )
        const missing = Redacted.make(yield* sign({ sub: '' }))
        assert.strictEqual(
          (yield* verifier.verify(missing, options).pipe(Effect.flip)).reason._tag,
          'AuthIdentityError',
        )
        const segments = Redacted.value(signed).split('.')
        const tampered = Redacted.make(`${segments[0]}.${segments[1]}.bad-signature`)
        assert.strictEqual(
          (yield* verifier.verify(tampered, options).pipe(Effect.flip)).reason._tag,
          'AuthIdentityError',
        )
        const expired = Redacted.make(
          yield* Effect.tryPromise(() =>
            new SignJWT({ sub: 'verified', nonce: 'expected' })
              .setProtectedHeader({ alg: 'RS256', kid: 'test' })
              .setIssuer(options.issuer)
              .setAudience(options.audience)
              .setExpirationTime(now - 1)
              .sign(pair.privateKey),
          ),
        )
        assert.strictEqual(
          (yield* verifier.verify(expired, options).pipe(Effect.flip)).reason._tag,
          'AuthIdentityError',
        )
      }),
  )
})
