import * as Schema from 'effect/Schema'
import * as Redacted from 'effect/Redacted'
import * as Credential from '../../src/provider-auth/Credential.js'
export type OAuthFixture = Credential.OAuth & { readonly binding: Credential.Binding }
export type ApiKeyFixture = Credential.ApiKey & {
  readonly binding: Credential.Binding
  readonly type: 'api-key'
}
export const encodeCredential = (input: OAuthFixture | ApiKeyFixture): Credential.Stored => {
  const { binding, ...secrets } = input
  const value =
    input.type === 'oauth'
      ? Schema.encodeSync(Schema.toCodecJson(Credential.OAuth))(secrets as Credential.OAuth)
      : Schema.encodeSync(Schema.toCodecJson(Credential.ApiKey))(secrets as Credential.ApiKey)
  return { binding, value: Redacted.make(value) }
}
export const decodeOAuth = (stored: Credential.Stored | undefined): OAuthFixture | undefined =>
  stored === undefined
    ? undefined
    : {
        ...Schema.decodeSync(Schema.toCodecJson(Credential.OAuth))(Redacted.value(stored.value)),
        binding: stored.binding,
      }
