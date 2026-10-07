import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Token from '@effect-harness/auth/Token'

describe('TokenSecrets', () => {
  it.effect(
    'sensitive helper arguments stay redacted and serialize only into URL-encoded bodies',
    () =>
      Effect.gen(function* () {
        const fields: Token.Fields = {
          grant_type: 'authorization_code',
          client_id: 'public-client',
          code: Redacted.make('code &?#'),
          code_verifier: Redacted.make('private-verifier'),
          state: Redacted.make('private-state'),
          refresh_token: Redacted.make('private-refresh'),
        }
        assert.isFalse(JSON.stringify(fields).includes('private-'))
        assert.isFalse(JSON.stringify(fields).includes('code &?#'))
        let requests = 0
        const http = HttpClient.make((request) => {
          requests++
          assert.strictEqual(request.headers['content-type'], 'application/x-www-form-urlencoded')
          if (request.body._tag !== 'Uint8Array') return Effect.die('Expected URL-encoded body')
          const serialized = new URLSearchParams(new TextDecoder().decode(request.body.body))
          assert.strictEqual(serialized.get('client_id'), 'public-client')
          assert.strictEqual(serialized.get('code'), 'code &?#')
          assert.strictEqual(serialized.get('code_verifier'), 'private-verifier')
          assert.strictEqual(serialized.get('state'), 'private-state')
          assert.strictEqual(serialized.get('refresh_token'), 'private-refresh')
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json({
                access_token: 'access',
                refresh_token: 'refresh',
                token_type: 'Bearer',
                expires_in: 60,
              }),
            ),
          )
        })
        yield* Token.request('https://auth.example/token', fields).pipe(
          Effect.provideService(HttpClient.HttpClient, http),
        )
        yield* Token.revoke('https://auth.example/revoke', fields).pipe(
          Effect.provideService(HttpClient.HttpClient, http),
        )
        assert.strictEqual(requests, 2)
        if (fields.code_verifier === undefined) return yield* Effect.die('Expected verifier')
        assert.strictEqual(Redacted.value(fields.code_verifier), 'private-verifier')
      }),
  )
})
