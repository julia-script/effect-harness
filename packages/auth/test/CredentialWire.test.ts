import { assert, describe, it } from '@effect/vitest'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Credential from '@effect-harness/auth/Credential'

describe('CredentialWire', () => {
  it('owned opaque credentials accept undefined optional redirect URI', () => {
    const credential = {
      kind: 'opaqueOAuth',
      provider: 'anthropic',
      authorizationServer: 'https://auth.example',
      clientId: 'client',
      accessToken: 'access',
      refreshToken: 'refresh',
      scopes: [],
      expiresAt: 0.5,
      redirectUri: undefined,
    }
    assert.isTrue(Option.isSome(Schema.decodeUnknownOption(Credential.OpaqueOAuth)(credential)))
  })
})
