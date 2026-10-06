import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import { createLocalJWKSet, jwtVerify } from 'jose'
import { AuthIdentityError, AuthNetworkError, AuthError } from './Credential.ts'
const KeySet = Schema.Struct({
  keys: Schema.Array(
    Schema.Struct({
      kty: Schema.String,
      kid: Schema.optional(Schema.String),
      alg: Schema.optional(Schema.String),
      use: Schema.optional(Schema.String),
      n: Schema.optional(Schema.String),
      e: Schema.optional(Schema.String),
      crv: Schema.optional(Schema.String),
      x: Schema.optional(Schema.String),
      y: Schema.optional(Schema.String),
    }),
  ),
})
const Claims = Schema.Struct({
  sub: Schema.NonEmptyString,
  iss: Schema.NonEmptyString,
  exp: Schema.Finite,
  nonce: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
})
export type Identity = typeof Claims.Type
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
export const layer = Layer.effect(Jwt)(
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    return Jwt.of({
      verify: Effect.fnUntraced(function* (token, options) {
        const response = yield* client.get(options.jwksUrl).pipe(
          Effect.mapError(
            (cause) =>
              new AuthError({
                reason: new AuthNetworkError({
                  cause,
                  message: 'Identity verification keys could not be loaded',
                }),
              }),
          ),
        )
        if (response.status !== 200)
          return yield* new AuthError({
            reason: new AuthIdentityError({
              message: 'Identity verification keys unavailable',
            }),
          })
        const json = yield* response.json.pipe(
          Effect.mapError(
            (cause) =>
              new AuthError({
                reason: new AuthIdentityError({ cause, message: 'Invalid verification keys' }),
              }),
          ),
        )
        const keys = yield* Schema.decodeUnknownEffect(KeySet)(json).pipe(
          Effect.mapError(
            (cause) =>
              new AuthError({
                reason: new AuthIdentityError({ cause, message: 'Invalid verification keys' }),
              }),
          ),
        )
        const keyResolver = yield* Effect.try({
          try: () =>
            createLocalJWKSet({
              keys: keys.keys.map((key) => ({
                kty: key.kty,
                ...(key.kid === undefined ? {} : { kid: key.kid }),
                ...(key.alg === undefined ? {} : { alg: key.alg }),
                ...(key.use === undefined ? {} : { use: key.use }),
                ...(key.n === undefined ? {} : { n: key.n }),
                ...(key.e === undefined ? {} : { e: key.e }),
                ...(key.crv === undefined ? {} : { crv: key.crv }),
                ...(key.x === undefined ? {} : { x: key.x }),
                ...(key.y === undefined ? {} : { y: key.y }),
              })),
            }),
          catch: (cause) =>
            new AuthError({
              reason: new AuthIdentityError({ cause, message: 'Invalid verification keys' }),
            }),
        })
        const now = yield* Clock.currentTimeMillis
        const verified = yield* Effect.tryPromise({
          try: () =>
            jwtVerify(Redacted.value(token), keyResolver, {
              issuer: options.issuer,
              audience: options.audience,
              algorithms: [...(options.algorithms ?? ['RS256', 'ES256'])],
              requiredClaims: ['sub', 'iss', 'aud', 'exp'],
              currentDate: new Date(now),
            }),
          catch: (cause) =>
            new AuthError({
              reason: new AuthIdentityError({
                cause,
                message: 'ID token signature or claims are invalid',
              }),
            }),
        })
        const claims = yield* Schema.decodeUnknownEffect(Claims)(verified.payload).pipe(
          Effect.mapError(
            (cause) =>
              new AuthError({
                reason: new AuthIdentityError({ cause, message: 'ID token identity is invalid' }),
              }),
          ),
        )
        if (options.nonce !== undefined && claims.nonce !== options.nonce)
          return yield* new AuthError({
            reason: new AuthIdentityError({
              message: 'ID token nonce does not match the authorization attempt',
            }),
          })
        return claims
      }),
    })
  }),
)
