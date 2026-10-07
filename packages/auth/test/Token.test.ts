import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Token from '@effect-harness/auth/Token'

describe('Token', () => {
  it.effect('token requests decode valid grants and sanitize error bodies', () =>
    Effect.gen(function* () {
      const request = yield* Token.request('https://auth.example/token', {
        grant_type: 'refresh_token',
        refresh_token: Redacted.make('not-logged'),
      }).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((req) => {
            assert.strictEqual(req.body._tag, 'Uint8Array')
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                req,
                new Response(
                  JSON.stringify({
                    access_token: 'access',
                    refresh_token: 'rotated',
                    expires_in: 3600,
                    token_type: 'Bearer',
                    scope: 'read',
                  }),
                ),
              ),
            )
          }),
        ),
      )
      assert.strictEqual(Redacted.value(request.refresh_token), 'rotated')
      const rejected = yield* Token.request('https://auth.example/token', {
        refresh_token: Redacted.make('not-logged'),
      }).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((req) =>
            Effect.succeed(
              HttpClientResponse.fromWeb(req, new Response('secret-echo', { status: 400 })),
            ),
          ),
        ),
        Effect.flip,
      )
      assert.strictEqual(rejected.status, 400)
      assert.isFalse(JSON.stringify(rejected).includes('secret-echo'))
      assert.isFalse(JSON.stringify(rejected).includes('not-logged'))
    }),
  )
})
