/** Public provider state. Encoded configuration and opaque credential references contain no secrets. */
import * as Schema from 'effect/Schema'
import * as Credential from './provider-auth/Credential.js'

declare const IdTypeId: unique symbol
export type Id<P extends string = string> = string & { readonly [IdTypeId]: P }
export const Stored = Schema.Struct({
  id: Schema.NonEmptyString,
  provider: Schema.NonEmptyString,
  version: Schema.Int.check(Schema.isGreaterThan(0)),
  revision: Schema.NonEmptyString,
  label: Schema.NonEmptyString,
  config: Schema.Json,
  credential: Schema.optionalKey(Credential.Binding),
})
export type Stored = typeof Stored.Type
export interface ProviderInstance<P extends string, C> extends Omit<
  Stored,
  'id' | 'provider' | 'config'
> {
  readonly id: Id<P>
  readonly provider: P
  readonly config: C
}
