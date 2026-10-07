import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Base64 from 'effect/encoding/Base64'
import { AuthConfigurationError, AuthError } from './Credential.ts'
export interface Challenge {
  readonly verifier: Redacted.Redacted<string>
  readonly challenge: string
  readonly state: string
  readonly nonce: string
}
export const base64Url = (bytes: Uint8Array): string =>
  Base64.encode(bytes).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '')
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
