/** Live secrets and non-secret, durable account bindings. Expiry uses Unix milliseconds. */
import * as Schema from 'effect/Schema'

export const Provider = Schema.Literals(['openai-codex', 'anthropic'])
export type Provider = typeof Provider.Type
export const Binding = Schema.Struct({
  provider: Provider,
  profile: Schema.NonEmptyString,
  /** New random identity on every login/replacement; refresh preserves it. */
  identity: Schema.NonEmptyString,
})
export type Binding = typeof Binding.Type

export const OAuth = Schema.Struct({
  type: Schema.Literal('oauth'),
  binding: Binding,
  accessToken: Schema.Redacted(Schema.NonEmptyString),
  refreshToken: Schema.Redacted(Schema.NonEmptyString),
  expiresAt: Schema.Int.check(Schema.isGreaterThan(0)),
  accountId: Schema.optionalKey(Schema.NonEmptyString),
})
export type OAuth = typeof OAuth.Type
export const ApiKey = Schema.Struct({
  type: Schema.Literal('api-key'),
  binding: Binding,
  apiKey: Schema.Redacted(Schema.NonEmptyString),
})
export type ApiKey = typeof ApiKey.Type
export const Credential = Schema.Union([OAuth, ApiKey])
export type Credential = typeof Credential.Type
export interface Metadata {
  readonly binding: Binding
  readonly type: Credential['type']
  readonly expiresAt?: number
}
export const metadata = (credential: Credential): Metadata => ({
  binding: credential.binding,
  type: credential.type,
  ...(credential.type === 'oauth' ? { expiresAt: credential.expiresAt } : {}),
})
export const key = (binding: Pick<Binding, 'provider' | 'profile'>): string =>
  JSON.stringify([binding.provider, binding.profile])
