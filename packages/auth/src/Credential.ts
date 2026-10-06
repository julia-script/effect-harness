import * as Schema from 'effect/Schema'

export class AuthError extends Schema.Error<AuthError>('@effect-harness/auth/Credential/AuthError')(
  {
    _tag: Schema.tag('AuthError'),
    reason: Schema.Literals([
      'storage',
      'busy',
      'configuration',
      'missing',
      'callback',
      'denied',
      'token',
      'network',
      'identity',
      'permission',
      'expired',
      'protocol',
    ]),
    message: Schema.String,
    status: Schema.optional(Schema.Int),
  },
) {}

export const Secret = Schema.RedactedFromValue(Schema.NonEmptyString)
export const ApiKey = Schema.Struct({
  kind: Schema.Literal('apiKey'),
  provider: Schema.NonEmptyString,
  apiKey: Secret,
})
const RegistrationFields = {
  provider: Schema.NonEmptyString,
  issuer: Schema.NonEmptyString,
  subject: Schema.NonEmptyString,
  clientId: Schema.NonEmptyString,
  hostId: Schema.NonEmptyString,
  email: Schema.optional(Schema.String),
  redirectUri: Schema.optional(Schema.String),
}
export const Registration = Schema.Struct({
  kind: Schema.Literal('registration'),
  ...RegistrationFields,
})
export type Registration = typeof Registration.Type
export const OAuth = Schema.Struct({
  kind: Schema.Literal('oauth'),
  ...RegistrationFields,
  accessToken: Secret,
  refreshToken: Secret,
  idToken: Secret,
  scopes: Schema.Array(Schema.NonEmptyString),
  expiresAt: Schema.Finite,
  earliestRefreshAt: Schema.optional(Schema.Finite),
})
export type OAuth = typeof OAuth.Type
/** Opaque OAuth grants carry no verified OIDC subject or identity token. The caller owns the storage key. */
export const OpaqueOAuth = Schema.Struct({
  kind: Schema.Literal('opaqueOAuth'),
  provider: Schema.NonEmptyString,
  authorizationServer: Schema.NonEmptyString,
  clientId: Schema.NonEmptyString,
  accessToken: Secret,
  refreshToken: Secret,
  scopes: Schema.Array(Schema.NonEmptyString),
  expiresAt: Schema.Finite,
  redirectUri: Schema.optionalKey(Schema.String),
})
export type OpaqueOAuth = typeof OpaqueOAuth.Type
export const Credential = Schema.Union([ApiKey, OAuth, Registration, OpaqueOAuth])
export type Credential = typeof Credential.Type
/** Registration identity is provider, issued client and verified account, never email. */
export const accountKey = (
  credential: Pick<OAuth, 'provider' | 'issuer' | 'clientId' | 'subject'>,
): string =>
  JSON.stringify([credential.provider, credential.issuer, credential.clientId, credential.subject])
