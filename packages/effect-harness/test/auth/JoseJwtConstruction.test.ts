import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as JoseJwt from 'effect-harness/auth/JoseJwt'
import * as Jwt from 'effect-harness/auth/Jwt'

describe('JoseJwtConstruction', () => {
  it.effect('Jose construction captures the substituted HTTP client while Jwt stays portable', () =>
    Effect.gen(function* () {
      assert.isFalse('layer' in Jwt)
      let requests = 0
      const http = HttpClient.make((request) => {
        requests++
        return Effect.succeed(
          HttpClientResponse.fromWeb(request, Response.json({}, { status: 503 })),
        )
      })
      const service = yield* JoseJwt.make.pipe(Effect.provideService(HttpClient.HttpClient, http))
      const error = yield* service
        .verify(Redacted.make('not-a-token'), {
          issuer: 'issuer',
          audience: 'audience',
          jwksUrl: 'https://auth.example/keys',
        })
        .pipe(Effect.flip)
      assert.strictEqual(error.reason._tag, 'AuthIdentityError')
      assert.strictEqual(requests, 1)
    }),
  )
})
