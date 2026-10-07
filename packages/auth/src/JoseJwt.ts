import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import { createLocalJWKSet, jwtVerify } from 'jose'
import { AuthIdentityError, AuthNetworkError, AuthError } from './Credential.ts'
import { Identity, Jwt } from './Jwt.ts'
export const KeySet = Schema.Struct({
  keys: Schema.Array(
    Schema.Struct({
      kty: Schema.String,
      kid: Schema.optionalKey(Schema.String),
      alg: Schema.optionalKey(Schema.String),
      use: Schema.optionalKey(Schema.String),
      n: Schema.optionalKey(Schema.String),
      e: Schema.optionalKey(Schema.String),
      crv: Schema.optionalKey(Schema.String),
      x: Schema.optionalKey(Schema.String),
      y: Schema.optionalKey(Schema.String),
    }),
  ),
})
export type KeySet = typeof KeySet.Type
export const isKeySet: (value: unknown) => value is KeySet = Schema.is(KeySet)
export const Claims = Schema.Struct({
  sub: Schema.NonEmptyString,
  iss: Schema.NonEmptyString,
  exp: Schema.Finite,
  nonce: Schema.optionalKey(Schema.String),
  email: Schema.optionalKey(Schema.String),
})
export type Claims = typeof Claims.Type
export const isClaims: (value: unknown) => value is Claims = Schema.is(Claims)
export const make: Effect.Effect<typeof Jwt.Service, never, HttpClient.HttpClient> = Effect.gen(
  function* () {
    const client = yield* HttpClient.HttpClient
    return Jwt.of({
      verify: Effect.fnUntraced(function* (token, options) {
        // P5-request-resolver-batching: each verification independently samples this rotating
        // issuer key set. There is no bulk endpoint; sharing/deduplicating equal URLs could hide
        // a rotation between tokens or retain a failed lookup. No HTTP response or result cache.
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
        const now = yield* DateTime.now
        const verified = yield* Effect.tryPromise({
          try: () =>
            jwtVerify(Redacted.value(token), keyResolver, {
              issuer: options.issuer,
              audience: options.audience,
              algorithms: [...(options.algorithms ?? ['RS256', 'ES256'])],
              requiredClaims: ['sub', 'iss', 'aud', 'exp'],
              currentDate: DateTime.toDateUtc(now),
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
        // Claims is raw JWT seconds. Identity owns the portable UTC domain shape;
        // decode the same exact fractional millisecond codec once at this boundary.
        return yield* Schema.decodeEffect(Identity)({ ...claims, exp: claims.exp * 1000 }).pipe(
          Effect.mapError(
            (cause) =>
              new AuthError({
                reason: new AuthIdentityError({ cause, message: 'ID token expiry is invalid' }),
              }),
          ),
        )
      }),
    })
  },
)

export const layer: Layer.Layer<Jwt, never, HttpClient.HttpClient> = Layer.effect(Jwt, make)
