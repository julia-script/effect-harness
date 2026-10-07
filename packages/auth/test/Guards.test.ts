import { assert, describe, it } from '@effect/vitest'
import * as DateTime from 'effect/DateTime'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Credential from '../src/Credential.ts'
import * as JoseJwt from '../src/JoseJwt.ts'
import * as Jwt from '../src/Jwt.ts'
import * as Token from '../src/Token.ts'
import * as Time from '../src/Time.ts'

describe('schema-backed public auth guards', () => {
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
      kind: 'registration',
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
      kind: 'opaqueOAuth',
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
    assert.deepEqual(Object.keys(apiKey), ['kind', 'provider', 'apiKey'])
    assert.deepEqual(Schema.encodeSync(Credential.ApiKey)(apiKey), {
      kind: 'apiKey',
      provider: 'openai',
      apiKey: 'key',
    })
  })

  it('JWT identity uses domain UTC while raw claims and token responses keep protocol units', () => {
    const claims = { sub: 'subject', iss: 'issuer', exp: 0.5 }
    assert.isTrue(JoseJwt.isClaims(claims))
    assert.isFalse(Jwt.isIdentity(claims))
    const identity = { ...claims, exp: Time.fromEpochMillis(500.5), nonce: undefined }
    assert.isTrue(Jwt.isIdentity(identity))
    assert.isFalse(JoseJwt.isClaims(identity))
    assert.isFalse(JoseJwt.isClaims({ ...claims, nonce: undefined }))
    assert.isFalse(JoseJwt.isClaims({ ...claims, exp: Infinity }))
    assert.isTrue(JoseJwt.isKeySet({ keys: [{ kty: 'RSA' }] }))
    assert.isFalse(JoseJwt.isKeySet({ keys: [{ kty: 'RSA', kid: undefined }] }))
    const token = Schema.decodeSync(Token.TokenResponse)({
      access_token: 'access',
      refresh_token: 'refresh',
      token_type: 'Bearer',
      expires_in: 0.5,
    })
    assert.isTrue(Token.isTokenResponse(token))
    assert.isFalse(Token.isTokenResponse({ ...token, expires_in: Infinity }))
    assert.isFalse(Token.isTokenResponse({ ...token, id_token: undefined }))
    assert.deepEqual(Schema.encodeSync(Token.TokenResponse)(token), {
      access_token: 'access',
      refresh_token: 'refresh',
      token_type: 'Bearer',
      expires_in: 0.5,
    })
  })
})
