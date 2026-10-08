import { assert, describe, it } from '@effect/vitest'
import * as DateTime from 'effect/DateTime'
import * as Redacted from 'effect/Redacted'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Credential from 'effect-harness/auth/Credential'
import * as Time from 'effect-harness/auth/Time'

describe('CredentialGuard', () => {
  it.effect(
    'credential variants validate decoded secrets, exact UTC values and actual optional fields',
    () =>
      Effect.gen(function* () {
        // effect-nit-allow P8-testschema-asserts: decoding prepares the native secret-bearing value whose public guards are the subject.
        const apiKey = yield* Schema.decodeEffect(Credential.ApiKey)({
          _tag: 'apiKey',
          provider: 'openai',
          apiKey: 'key',
        })
        assert.isTrue(Credential.isApiKey(apiKey))
        assert.isTrue(Credential.isCredential(apiKey))
        assert.isFalse(Credential.isApiKey({ ...apiKey, apiKey: 'raw-secret' }))
        assert.isFalse(Credential.isCredential({ ...apiKey, provider: '' }))
        const registration = {
          _tag: 'registration',
          provider: 'openai',
          issuer: 'issuer',
          subject: 'subject',
          clientId: 'client',
          hostId: 'host',
          email: undefined,
        } as const
        assert.isTrue(Credential.isRegistration(registration))
        assert.isFalse(Credential.isRegistration({ ...registration, subject: '' }))
        // effect-nit-allow P8-testschema-asserts: decoding prepares the OAuth guard subject; UTC representation is asserted separately.
        const oauth = yield* Schema.decodeEffect(Credential.OAuth)({
          ...registration,
          _tag: 'oauth',
          accessToken: 'access',
          refreshToken: 'refresh',
          idToken: 'identity',
          scopes: ['scope'],
          expiresAt: 0.5,
        })
        assert.isTrue(Credential.isOAuth(oauth))
        assert.strictEqual(DateTime.toEpochMillis(oauth.expiresAt), 0.5)
        assert.isFalse(Credential.isOAuth({ ...oauth, expiresAt: 0.5 }))
        const opaque = {
          _tag: 'opaqueOAuth',
          provider: 'anthropic',
          authorizationServer: 'issuer',
          clientId: 'client',
          accessToken: Redacted.make('access'),
          refreshToken: Redacted.make('refresh'),
          scopes: [],
          expiresAt: Time.fromEpochMillis(0.5),
          redirectUri: undefined,
        } as const
        assert.isTrue(Credential.isOpaqueOAuth(opaque))
        assert.isTrue(Credential.isCredential(opaque))
        assert.isFalse(Credential.isOpaqueOAuth({ ...opaque, accessToken: Redacted.make('') }))
        // effect-nit-allow P8-testschema-asserts: this assertion pins decoded own-key enumeration order rather than schema value equality.
        assert.deepStrictEqual(Object.keys(apiKey), ['_tag', 'provider', 'apiKey'])
        yield* new TestSchema.Asserts(Credential.ApiKey).encoding().succeedEffect(apiKey, {
          _tag: 'apiKey',
          provider: 'openai',
          apiKey: 'key',
        })
      }),
  )
})
