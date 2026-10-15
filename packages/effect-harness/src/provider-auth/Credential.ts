/** Schema-backed secrets and opaque credential references. Expiry uses Unix milliseconds. */
import * as Schema from 'effect/Schema'

export const Binding = Schema.Struct({
  credentialId: Schema.NonEmptyString,
  /** Changes on replacement; token refresh preserves it. */
  identity: Schema.NonEmptyString,
})
export type Binding = typeof Binding.Type

export const OAuth = Schema.Struct({
  type: Schema.Literal('oauth'),
  accessToken: Schema.Redacted(Schema.NonEmptyString),
  refreshToken: Schema.Redacted(Schema.NonEmptyString),
  expiresAt: Schema.Int.check(Schema.isGreaterThan(0)),
  accountId: Schema.optionalKey(Schema.NonEmptyString),
})
export type OAuth = typeof OAuth.Type
export const ApiKey = Schema.Struct({
  apiKey: Schema.Redacted(Schema.NonEmptyString),
})
export type ApiKey = typeof ApiKey.Type

/** Generic encoded secrets. Only credential stores persist the unwrapped JSON. */
export const Stored = Schema.Struct({
  binding: Binding,
  value: Schema.Redacted(Schema.Json),
})
export type Stored = typeof Stored.Type
export const metadata = (credential: Stored): Binding => ({ ...credential.binding })
