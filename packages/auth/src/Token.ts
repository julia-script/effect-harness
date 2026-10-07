import * as Redacted from 'effect/Redacted'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import {
  AuthNetworkError,
  AuthProtocolError,
  AuthTokenError,
  AuthError,
  Secret,
} from './Credential.ts'
export const TokenResponse = Schema.Struct({
  access_token: Secret,
  refresh_token: Secret,
  id_token: Schema.optionalKey(Secret),
  token_type: Schema.String,
  expires_in: Schema.Finite,
  scope: Schema.optionalKey(Schema.String),
  earliest_refresh_at: Schema.optionalKey(Schema.Finite),
})
export type TokenResponse = typeof TokenResponse.Type
/** Sensitive protocol fields stay wrapped until the final HTTP body serialization. */
export interface Fields {
  readonly [key: string]: string | Redacted.Redacted<string> | undefined
  readonly refresh_token?: Redacted.Redacted<string> | undefined
  readonly access_token?: Redacted.Redacted<string> | undefined
  readonly token?: Redacted.Redacted<string> | undefined
  readonly code?: Redacted.Redacted<string> | undefined
  readonly code_verifier?: Redacted.Redacted<string> | undefined
  readonly state?: Redacted.Redacted<string> | undefined
}

/** Failures omit raw requests and response bodies, which can contain credentials. */
export const request = Effect.fnUntraced(function* (
  endpoint: string,
  fields: Fields,
): Effect.fn.Return<TokenResponse, AuthError, HttpClient.HttpClient> {
  const client = yield* HttpClient.HttpClient
  const response = yield* client
    .execute(
      HttpClientRequest.post(endpoint).pipe(
        HttpClientRequest.bodyUrlParams(
          Object.fromEntries(
            Object.entries(fields).flatMap(([key, value]) =>
              value === undefined
                ? []
                : [[key, Redacted.isRedacted(value) ? Redacted.value(value) : value]],
            ),
          ),
        ),
      ),
    )
    .pipe(
      Effect.mapError(
        (cause) =>
          new AuthError({
            reason: new AuthNetworkError({ cause, message: 'Token endpoint could not be reached' }),
          }),
      ),
    )
  if (response.status !== 200)
    return yield* new AuthError({
      reason: new AuthTokenError({
        message: 'Token endpoint rejected the grant',
        status: response.status,
      }),
    })
  const body = yield* response.json.pipe(
    Effect.mapError(
      (cause) =>
        new AuthError({
          reason: new AuthProtocolError({ cause, message: 'Invalid token response' }),
        }),
    ),
  )
  const token = yield* Schema.decodeUnknownEffect(TokenResponse)(body).pipe(
    Effect.mapError(
      (cause) =>
        new AuthError({
          reason: new AuthProtocolError({ cause, message: 'Invalid token response' }),
        }),
    ),
  )
  if (
    token.token_type.toLowerCase() !== 'bearer' ||
    !Number.isFinite(token.expires_in) ||
    token.expires_in <= 0
  )
    return yield* new AuthError({
      reason: new AuthProtocolError({
        message: 'Unsupported token type or lifetime',
      }),
    })
  return token
})
export const revoke = Effect.fnUntraced(function* (
  endpoint: string,
  fields: Fields,
): Effect.fn.Return<void, AuthError, HttpClient.HttpClient> {
  const client = yield* HttpClient.HttpClient
  const response = yield* client
    .execute(
      HttpClientRequest.post(endpoint).pipe(
        HttpClientRequest.bodyUrlParams(
          Object.fromEntries(
            Object.entries(fields).flatMap(([key, value]) =>
              value === undefined
                ? []
                : [[key, Redacted.isRedacted(value) ? Redacted.value(value) : value]],
            ),
          ),
        ),
      ),
    )
    .pipe(
      Effect.mapError(
        (cause) =>
          new AuthError({
            reason: new AuthNetworkError({ cause, message: 'Revocation could not be confirmed' }),
          }),
      ),
    )
  if (response.status !== 200)
    return yield* new AuthError({
      reason: new AuthTokenError({
        message: 'Revocation could not be confirmed',
        status: response.status,
      }),
    })
})
