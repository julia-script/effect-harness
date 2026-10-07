import { assert, describe, it } from '@effect/vitest'
import * as Schema from 'effect/Schema'
import * as JoseJwt from '@effect-harness/auth/JoseJwt'
import * as Jwt from '@effect-harness/auth/Jwt'
import * as Token from '@effect-harness/auth/Token'
import * as Time from '@effect-harness/auth/Time'

describe('JwtGuard', () => {
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
    assert.deepStrictEqual(Schema.encodeSync(Token.TokenResponse)(token), {
      access_token: 'access',
      refresh_token: 'refresh',
      token_type: 'Bearer',
      expires_in: 0.5,
    })
  })
})
