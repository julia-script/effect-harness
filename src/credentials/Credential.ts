/**
 * Stored credentials, modeled on pi's `auth/types.ts`: one per provider, an API key or OAuth
 * tokens. Secrets decode to `Redacted`, so they stay out of logs and inspection; they encode back
 * to strings, for a store or a transport.
 */
import { Schema } from 'effect'
import { JsonObject } from '../storage/schema/Json.ts'

/** A secret string: `Redacted` once decoded. */
export const Secret = Schema.RedactedFromValue(Schema.String)
export type Secret = typeof Secret.Type

/** An API key, with provider-scoped settings such as an account or gateway ID. */
export const ApiKey = Schema.TaggedStruct('ApiKey', {
  key: Secret,
  env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
})
export type ApiKey = typeof ApiKey.Type

/**
 * Provider-asserted account identity, scoped by a canonical issuer namespace.
 *
 * **Details**
 *
 * Never populated from credential IDs, user labels, or token secrets.
 *
 * @category schemas
 */
export const Identity = Schema.TaggedStruct('Identity', {
  issuer: Schema.NonEmptyString,
  subject: Schema.NonEmptyString,
  email: Schema.optionalKey(Schema.String),
  name: Schema.optionalKey(Schema.String),
})
export type Identity = typeof Identity.Type

/** OAuth tokens; `expires` is when `access` expires, in epoch milliseconds. */
export const OAuth = Schema.TaggedStruct('OAuth', {
  access: Secret,
  refresh: Secret,
  expires: Schema.Finite,
  /** Absent when the provider has not supplied a stable account identity. */
  identity: Schema.optionalKey(Identity),
  /** Opaque sign-in generation; changes on reauthentication, not token refresh. */
  revision: Schema.optionalKey(Schema.NonEmptyString),
  /** Provider-specific token data, such as an account ID. */
  extra: Schema.optionalKey(JsonObject),
})
export type OAuth = typeof OAuth.Type

export const Credential = Schema.Union([ApiKey, OAuth])
export type Credential = typeof Credential.Type

/** What a credential is, without its secrets, for status and account lists. */
export const Info = Schema.TaggedStruct('Info', {
  provider: Schema.String,
  type: Schema.Literals(['ApiKey', 'OAuth']),
})
export type Info = typeof Info.Type
