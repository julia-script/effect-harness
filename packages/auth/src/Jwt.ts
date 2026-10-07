import * as Schema from 'effect/Schema'
import * as Time from './Time.ts'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Redacted from 'effect/Redacted'
import type { AuthError } from './Credential.ts'

/** Verified domain identity: exact UTC expiry encoded as epoch milliseconds; raw JWT seconds use JoseJwt.Claims. */
export const Identity = Schema.Struct({
  sub: Schema.NonEmptyString,
  iss: Schema.NonEmptyString,
  exp: Time.EpochMillis,
  nonce: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
})
export type Identity = typeof Identity.Type
export const isIdentity: (value: unknown) => value is Identity = Schema.is(Identity)
export interface VerifyOptions {
  readonly issuer: string
  readonly audience: string
  readonly jwksUrl: string
  readonly nonce?: string | undefined
  readonly algorithms?: ReadonlyArray<string> | undefined
}
export class Jwt extends Context.Service<
  Jwt,
  {
    readonly verify: (
      token: Redacted.Redacted<string>,
      options: VerifyOptions,
    ) => Effect.Effect<Identity, AuthError>
  }
>()('@effect-harness/auth/Jwt') {}
