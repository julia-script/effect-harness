import { assert, describe, it } from '@effect/vitest'
import * as DateTime from 'effect/DateTime'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Credential from '@effect-harness/auth/Credential'
import * as Time from '@effect-harness/auth/Time'

describe('CredentialGuard', () => {
  it('credential variants validate decoded secrets, exact UTC values and actual optional fields', () => {
    const apiKey = Schema.decodeSync(Credential.ApiKey)({
      kind: 'apiKey',
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
    const oauth = Schema.decodeSync(Credential.OAuth)({
      ...registration,
      kind: 'oauth',
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
    assert.deepStrictEqual(Object.keys(apiKey), ['_tag', 'provider', 'apiKey'])
    assert.deepStrictEqual(Schema.encodeSync(Credential.ApiKey)(apiKey), {
      kind: 'apiKey',
      provider: 'openai',
      apiKey: 'key',
    })
  })
})
