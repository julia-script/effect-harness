const ChallengeTypeId = '~effect-harness/auth/Pkce/Challenge'

/**
 * Cryptographic PKCE challenges and unpadded base64url encoding.
 */
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
import * as Crypto from 'effect/Crypto'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Base64Url from 'effect/encoding/Base64Url'
import { AuthConfigurationError, AuthError } from './AuthError.ts'
/**
 * Secret PKCE verifier, SHA-256 challenge and independent state/nonce values.
 *
 * **Details**
 *
 * This owned handle supports piping and bounded inspection. `toJSON` is a diagnostic
 * projection; use the original fields for protocol values and resource references.
 *
 * @category models
 */
export interface Challenge extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [ChallengeTypeId]: typeof ChallengeTypeId
  readonly verifier: Redacted.Redacted<string>
  readonly challenge: string
  readonly state: string
  readonly nonce: string
}

/**
 * Checks the established nominal `Challenge` marker; it does not validate arbitrary payload fields.
 *
 * @category guards
 */
export const isChallenge = (u: unknown): u is Challenge =>
  Predicate.hasProperty(u, ChallengeTypeId) && u[ChallengeTypeId] === ChallengeTypeId

/**
 * Owns a `Challenge` handle while preserving payload descriptors and exact resource references.
 *
 * **Details**
 *
 * Construction and diagnostics do not evaluate payload accessors. Inspection is a bounded
 * diagnostic projection; read the original fields for protocol values.
 *
 * @category constructors
 */
export const makeChallenge = (
  input: Omit<
    Challenge,
    typeof ChallengeTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): Challenge => {
  const handle: Challenge = Object.create(ChallengeProto)
  const descriptors = Object.getOwnPropertyDescriptors(input)
  // The owned protocol cannot be replaced by extra runtime payload keys.
  for (const key of [ChallengeTypeId, 'pipe', 'toJSON', 'toString', Inspectable.NodeInspectSymbol])
    Reflect.deleteProperty(descriptors, key)
  Object.defineProperties(handle, descriptors)
  Object.defineProperty(handle, ChallengeTypeId, { value: ChallengeTypeId, enumerable: false })
  return handle
}

const ChallengeProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return {
      _id: 'effect-harness/auth/Pkce/Challenge',
      verifier: '<redacted>',
      state: '<redacted>',
      nonce: '<redacted>',
    }
  },
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
  return makeChallenge({
    verifier: Redacted.make(verifier),
    challenge: base64Url(digest),
    state: yield* random(32),
    nonce: yield* random(32),
  })
})
