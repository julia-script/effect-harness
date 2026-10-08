import { describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Time from 'effect-harness/auth/Time'
import * as Credential from 'effect-harness/auth/Credential'

describe('CredentialWire', () => {
  it.effect('owned opaque credentials accept undefined optional redirect URI', () =>
    Effect.gen(function* () {
      const credential = {
        _tag: 'opaqueOAuth' as const,
        provider: 'anthropic',
        authorizationServer: 'https://auth.example',
        clientId: 'client',
        accessToken: 'access',
        refreshToken: 'refresh',
        scopes: [],
        expiresAt: 0.5,
        redirectUri: undefined,
      }
      const expected = {
        _tag: 'opaqueOAuth' as const,
        provider: 'anthropic',
        authorizationServer: 'https://auth.example',
        clientId: 'client',
        accessToken: Redacted.make('access'),
        refreshToken: Redacted.make('refresh'),
        scopes: [],
        expiresAt: Time.fromEpochMillis(0.5),
        redirectUri: undefined,
      }
      const checks = new TestSchema.Asserts(Credential.OpaqueOAuth)
      yield* checks.decoding().succeedEffect(credential, expected)
      // effect-nit-allow P8-testschema-asserts: decode supplies the encoding assertion with observed native Redacted contents, which deep object equality cannot inspect.
      const decoded = yield* Schema.decodeEffect(Credential.OpaqueOAuth)(credential)
      yield* checks.encoding().succeedEffect(decoded, {
        _tag: 'opaqueOAuth',
        provider: 'anthropic',
        authorizationServer: 'https://auth.example',
        clientId: 'client',
        accessToken: 'access',
        refreshToken: 'refresh',
        scopes: [],
        expiresAt: 0.5,
        redirectUri: undefined,
      })
    }),
  )
})
