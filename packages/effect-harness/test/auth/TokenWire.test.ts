import { assert, describe, it } from '@effect/vitest'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Token from 'effect-harness/auth/Token'

describe('TokenWire', () => {
  it('token optional wire keys reject explicit undefined and preserve absent keys', () => {
    const token = {
      access_token: 'access',
      refresh_token: 'refresh',
      token_type: 'Bearer',
      expires_in: 0.5,
    }
    const decode = Schema.decodeUnknownOption(Token.TokenResponse)
    assert.isTrue(Option.isSome(decode(token)))
    for (const key of ['id_token', 'scope', 'earliest_refresh_at'])
      assert.isTrue(Option.isNone(decode({ ...token, [key]: undefined })), key)
  })
})
