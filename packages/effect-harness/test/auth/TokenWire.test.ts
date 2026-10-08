import { describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Token from 'effect-harness/auth/Token'

describe('TokenWire', () => {
  it.effect('token optional wire keys reject explicit undefined and preserve absent keys', () =>
    Effect.gen(function* () {
      const token = {
        access_token: 'access',
        refresh_token: 'refresh',
        token_type: 'Bearer',
        expires_in: 0.5,
      }
      const checks = new TestSchema.Asserts(Token.TokenResponse)
      const expected = {
        access_token: Redacted.make('access'),
        refresh_token: Redacted.make('refresh'),
        token_type: 'Bearer',
        expires_in: 0.5,
      }
      yield* checks.decoding().succeedEffect(token, expected)
      // effect-nit-allow P8-testschema-asserts: decode supplies the encoding assertion with observed native Redacted contents, which deep object equality cannot inspect.
      const decoded = yield* Schema.decodeEffect(Token.TokenResponse)(token)
      yield* checks.encoding().succeedEffect(decoded, {
        access_token: 'access',
        refresh_token: 'refresh',
        token_type: 'Bearer',
        expires_in: 0.5,
      })
      for (const [key, issue] of [
        ['id_token', 'Expected string\n  at ["id_token"]'],
        ['scope', 'Expected string\n  at ["scope"]'],
        ['earliest_refresh_at', 'Expected number\n  at ["earliest_refresh_at"]'],
      ] as const) {
        yield* checks.decoding().failEffect({ ...token, [key]: undefined }, issue)
      }
    }),
  )
})
