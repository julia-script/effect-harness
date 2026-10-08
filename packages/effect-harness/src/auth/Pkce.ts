/**
 * Cryptographic PKCE challenges and unpadded base64url encoding.
 */
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Base64Url from 'effect/encoding/Base64Url'
import { AuthConfigurationError, AuthError } from './Credential.ts'
/**
 * Secret PKCE verifier, SHA-256 challenge and independent state/nonce values.
 *
 * @category models
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
 */
export const base64Url = (bytes: Uint8Array): string => Base64Url.encode(bytes)
/**
 * Creates a fresh S256 PKCE challenge with independent state and nonce.
 *
 * **Details**
 *
 * Generates 32 random bytes for each verifier, state and nonce and uses unpadded base64url
 * encoding. The verifier is Redacted; the challenge is its SHA-256 digest.
 *
 * **Gotchas**
 *
 * Secure random or digest failures use AuthConfigurationError. Keep the verifier private
 * until token exchange.
 *
 * @category constructors
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
