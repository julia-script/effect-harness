/**
 * Cryptographic PKCE challenges and unpadded base64url encoding.
 *
 * @since 0.0.0
 */
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Base64Url from 'effect/encoding/Base64Url'
import { AuthConfigurationError, AuthError } from './Credential.ts'
/**
 * Describes the Challenge contract.
 *
 * @category types
 * @since 0.0.0
 */
export interface Challenge {
  readonly verifier: Redacted.Redacted<string>
  readonly challenge: string
  readonly state: string
  readonly nonce: string
}
/**
 * Encodes bytes with the unpadded URL-safe base64 alphabet.
 *
 * @category encoding
 * @since 0.0.0
 */
export const base64Url = (bytes: Uint8Array): string => Base64Url.encode(bytes)
/**
 * Constructs Pkce with the caller-provided services.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make: Effect.Effect<Challenge, AuthError, Crypto.Crypto> = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto
  const random = (size: number) =>
    crypto.randomBytes(size).pipe(
      Effect.map(base64Url),
      Effect.mapError(
        (cause) =>
          new AuthError({
            reason: new AuthConfigurationError({
              cause,
              message: 'Secure random generation failed',
            }),
          }),
      ),
    )
  const verifier = yield* random(32)
  const digest = yield* crypto.digest('SHA-256', new TextEncoder().encode(verifier)).pipe(
    Effect.mapError(
      (cause) =>
        new AuthError({
          reason: new AuthConfigurationError({ cause, message: 'PKCE digest failed' }),
        }),
    ),
  )
  return {
    verifier: Redacted.make(verifier),
    challenge: base64Url(digest),
    state: yield* random(32),
    nonce: yield* random(32),
  }
})
