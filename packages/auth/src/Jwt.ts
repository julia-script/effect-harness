/**
 * Portable verified JWT identity and verification service contracts.
 *
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const Identity = Schema.Struct({
  sub: Schema.NonEmptyString,
  iss: Schema.NonEmptyString,
  exp: Time.EpochMillis,
  nonce: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
})
export type Identity = typeof Identity.Type
/**
 * Tests whether an unknown value satisfies the decoded Identity schema.
 *
 * @category guards
 * @since 0.0.0
 */
export const isIdentity: (u: unknown) => u is Identity = Schema.is(Identity)
/**
 * Types owned by the Jwt concept.
 *
 * @category types
 * @since 0.0.0
 */
export declare namespace Jwt {
  /**
   * Describes the VerifyOptions contract.
   *
   * @category types
   * @since 0.0.0
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
 * Describes the VerifyOptions contract.
 *
 * @category types
 * @since 0.0.0
 */
export type VerifyOptions = Jwt.VerifyOptions
/**
 * Identifies the Jwt service in the Effect context.
 *
 * @category services
 * @since 0.0.0
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
