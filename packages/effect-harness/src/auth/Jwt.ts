/**
 * Portable verified JWT identity and verification service contracts.
 */
import * as Schema from 'effect/Schema'
import * as Time from './Time.ts'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Redacted from 'effect/Redacted'
import type { AuthError } from './Credential.ts'

/**
 * Verified domain identity: exact UTC expiry encoded as epoch milliseconds; raw JWT seconds use JoseJwt.Claims.
 *
 * @category models
 */
export const Identity = Schema.Struct({
  sub: Schema.NonEmptyString,
  iss: Schema.NonEmptyString,
  exp: Time.EpochMillis,
  nonce: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
})
/**
 * Verified issuer and subject identity with token expiry.
 *
 * @category models
 */
export type Identity = typeof Identity.Type
/**
 * Checks whether a value satisfies the decoded `Identity` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isIdentity: (u: unknown) => u is Identity = Schema.is(Identity)
/**
 * Type-level contracts for `Jwt`.
 *
 * @category utility types
 */
export declare namespace Jwt {
  /**
   * Expected issuer, client, nonce and JWKS endpoint for token verification.
   *
   * @category models
   */
  export interface VerifyOptions {
    readonly issuer: string
    readonly audience: string
    readonly jwksUrl: string
    readonly nonce?: string | undefined
    readonly algorithms?: ReadonlyArray<string> | undefined
  }
}
/**
 * Expected issuer, client, nonce and JWKS endpoint for token verification.
 *
 * @category models
 */
export type VerifyOptions = Jwt.VerifyOptions
/**
 * Service verifying an identity token against expected authorization claims.
 *
 * **Details**
 *
 * Returns verified issuer/subject identity and expiry after checking the expected client.
 * Email is informational and does not determine account identity.
 *
 * @category services
 */
export class Jwt extends Context.Service<
  Jwt,
  {
    readonly verify: (
      token: Redacted.Redacted<string>,
      options: VerifyOptions,
    ) => Effect.Effect<Identity, AuthError>
  }
>()('@effect-harness/auth/Jwt') {}
