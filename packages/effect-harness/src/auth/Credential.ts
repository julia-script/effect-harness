/**
 * Redacted credential codecs for API keys and OAuth accounts.
 */
import * as Time from './Time.ts'
import * as Schema from 'effect/Schema'
import { HostId } from './HostId.ts'

/**
 * Nonempty secret decoded to a Redacted value.
 *
 * @category models
 */
export const Secret = Schema.RedactedFromValue(Schema.NonEmptyString)
/**
 * Nonempty secret decoded to a Redacted value.
 *
 * @category models
 */
export type Secret = typeof Secret.Type
const ApiKeyFields = { provider: Schema.NonEmptyString, apiKey: Secret }
/**
 * Provider API key stored under an application-owned account key.
 *
 * @category models
 */
export const ApiKey = Schema.TaggedStruct('apiKey', ApiKeyFields)
/**
 * Provider API key stored under an application-owned account key.
 *
 * @category models
 */
export type ApiKey = typeof ApiKey.Type
/**
 * Checks whether a value satisfies the decoded `ApiKey` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isApiKey: (u: unknown) => u is ApiKey = Schema.is(ApiKey)
const RegistrationFields = {
  provider: Schema.NonEmptyString,
  issuer: Schema.NonEmptyString,
  subject: Schema.NonEmptyString,
  clientId: Schema.NonEmptyString,
  hostId: HostId,
  email: Schema.optional(Schema.String),
  redirectUri: Schema.optional(Schema.String),
}
/**
 * Dynamic OAuth client registration retained for later sign-in.
 *
 * @category models
 */
export const Registration = Schema.TaggedStruct('registration', RegistrationFields)
/**
 * Dynamic OAuth client registration retained for later sign-in.
 *
 * @category models
 */
export type Registration = typeof Registration.Type
/**
 * Checks whether a value satisfies the decoded `Registration` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isRegistration: (u: unknown) => u is Registration = Schema.is(Registration)
const OAuthFields = {
  ...RegistrationFields,
  accessToken: Secret,
  refreshToken: Secret,
  idToken: Secret,
  scopes: Schema.Array(Schema.NonEmptyString),
  expiresAt: Time.EpochMillis,
  earliestRefreshAt: Schema.optional(Time.EpochMillis),
}
/**
 * Verified OAuth account identity and refreshable token grant.
 *
 * @category models
 */
export const OAuth = Schema.TaggedStruct('oauth', OAuthFields)
/**
 * Verified OAuth account identity and refreshable token grant.
 *
 * @category models
 */
export type OAuth = typeof OAuth.Type
/**
 * Checks whether a value satisfies the decoded `OAuth` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isOAuth: (u: unknown) => u is OAuth = Schema.is(OAuth)
/** Opaque OAuth grants carry no verified OIDC subject or identity token. The caller owns the storage key. */
const OpaqueOAuthFields = {
  provider: Schema.NonEmptyString,
  authorizationServer: Schema.NonEmptyString,
  clientId: Schema.NonEmptyString,
  accessToken: Secret,
  refreshToken: Secret,
  scopes: Schema.Array(Schema.NonEmptyString),
  expiresAt: Time.EpochMillis,
  redirectUri: Schema.optional(Schema.String),
}
/**
 * Schema for an OAuth grant under a caller-selected account key.
 *
 * **Details**
 *
 * Stores provider, authorization server and refreshable secrets without claiming a verified
 * OIDC subject. Anthropic account authorization uses this representation.
 *
 * @category models
 */
export const OpaqueOAuth = Schema.TaggedStruct('opaqueOAuth', OpaqueOAuthFields)
/**
 * Refreshable OAuth grant under a caller-selected key without an OIDC identity claim.
 *
 * @category models
 */
export type OpaqueOAuth = typeof OpaqueOAuth.Type
/**
 * Checks whether a value satisfies the decoded `OpaqueOAuth` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isOpaqueOAuth: (u: unknown) => u is OpaqueOAuth = Schema.is(OpaqueOAuth)
/**
 * API key, client registration or account token grant.
 *
 * @category models
 */
export const Credential = Schema.Union([ApiKey, OAuth, Registration, OpaqueOAuth])
/**
 * API key, client registration or account token grant.
 *
 * @category models
 */
export type Credential = typeof Credential.Type
/**
 * Checks whether a value satisfies the decoded `Credential` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isCredential: (u: unknown) => u is Credential = Schema.is(Credential)
/**
 * Builds an account key from provider and verified issuer/client/subject identity.
 *
 * **Gotchas**
 *
 * Do not substitute email or an unverified claim for the identity tuple.
 *
 * @category combinators
 */
export const accountKey = (
  self: Pick<OAuth, 'provider' | 'issuer' | 'clientId' | 'subject'>,
): string => JSON.stringify([self.provider, self.issuer, self.clientId, self.subject])

/** Checks the decoded Secret contract without decoding or coercing input.
 * @category guards
 */
export const isSecret: (u: unknown) => u is Secret = Schema.is(Schema.toType(Secret))
