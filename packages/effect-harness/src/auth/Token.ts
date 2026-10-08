/**
 * Redacted OAuth token and revocation HTTP boundaries.
 */
import * as Option from 'effect/Option'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Record from 'effect/Record'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import { Secret } from './Credential.ts'
import { AuthNetworkError, AuthProtocolError, AuthTokenError, AuthError } from './AuthError.ts'
/**
 * Token endpoint grant with Redacted tokens and lifetime metadata.
 *
 * @category models
 */
export const TokenResponse = Schema.Struct({
  access_token: Secret,
  refresh_token: Secret,
  id_token: Schema.optionalKey(Secret),
  token_type: Schema.String,
  expires_in: Schema.Finite,
  scope: Schema.optionalKey(Schema.String),
  earliest_refresh_at: Schema.optionalKey(Schema.Finite),
})
/**
 * Token endpoint grant with Redacted tokens and lifetime metadata.
 *
 * @category models
 */
export type TokenResponse = typeof TokenResponse.Type
/**
 * Checks whether a value satisfies the decoded `TokenResponse` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isTokenResponse: (u: unknown) => u is TokenResponse = Schema.is(TokenResponse)
const FieldSecret = Schema.RedactedFromValue(Schema.String)
const FieldValue = Schema.Union([Schema.String, FieldSecret, Schema.Undefined])
const FieldMap = Schema.Record(Schema.String, FieldValue)
const FieldWire = Schema.Record(Schema.String, Schema.String)
const omitUndefinedFields = {
  decode: SchemaGetter.passthrough<typeof FieldWire.Type>(),
  encode: SchemaGetter.transform((fields: Readonly<Record<string, string | undefined>>) =>
    Record.filter(fields, (value): value is string => value !== undefined),
  ),
}
/**
 * Extensible token request fields whose six sensitive keys require Redacted strings.
 *
 * **Details**
 *
 * Empty strings are valid, including redacted empty strings. Unknown string keys retain
 * string, Redacted<string> and explicit undefined values; encoding omits undefined entries.
 *
 * @category models
 */
export const Fields = Schema.StructWithRest(
  Schema.Struct({
    refresh_token: Schema.optional(FieldSecret),
    access_token: Schema.optional(FieldSecret),
    token: Schema.optional(FieldSecret),
    code: Schema.optional(FieldSecret),
    code_verifier: Schema.optional(FieldSecret),
    state: Schema.optional(FieldSecret),
  }),
  [FieldMap],
).pipe(Schema.encodeTo(FieldWire, omitUndefinedFields))
/**
 * Extensible token request fields with Redacted sensitive values.
 *
 * @category models
 */
export type Fields = typeof Fields.Type

// The native Record codec visits caller keys in order instead of fixed Struct keys first.
// Named sensitive-key validation still comes from the schema-derived Fields model above.
const FieldsCodec = FieldMap.pipe(Schema.encodeTo(FieldWire, omitUndefinedFields))
const invalidFields = () =>
  new AuthError({ reason: new AuthProtocolError({ message: 'Invalid token request fields' }) })
/**
 * Encodes token fields once at the final transport boundary, retaining caller key order.
 *
 * **Details**
 *
 * Capture own enumerable string fields once so accessor values are not sampled twice.
 * Validate the sensitive-key policy before native schema encoding unwraps Redacted values.
 * Diagnostics omit both request values and schema issues that could contain secrets.
 *
 * @category combinators
 */
export const encodeFields = Effect.fnUntraced(function* (
  fields: Fields,
): Effect.fn.Return<typeof FieldWire.Type, AuthError> {
  const snapshot = yield* Effect.try({
    try: () => Object.fromEntries(Object.entries(fields)),
    catch: invalidFields,
  })
  yield* Schema.decodeEffect(Schema.toType(Fields))(snapshot).pipe(Effect.mapError(invalidFields))
  return yield* Schema.encodeEffect(FieldsCodec)(snapshot).pipe(Effect.mapError(invalidFields))
})

/**
 * Requests an OAuth token using the protocol form-encoded endpoint.
 *
 * **Details**
 *
 * Failures omit raw requests and response bodies, which can contain credentials.
 *
 * @category combinators
 */
export const request = Effect.fnUntraced(function* (
  endpoint: string,
  fields: Fields,
): Effect.fn.Return<TokenResponse, AuthError, HttpClient.HttpClient> {
  const client = yield* HttpClient.HttpClient
  const response = yield* client
    .execute(
      HttpClientRequest.post(endpoint).pipe(
        HttpClientRequest.bodyUrlParams(yield* encodeFields(fields)),
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
  if (response.status !== 200) {
    // Read only a recognized rejection discriminator; never retain the response body.
    // JSON/codec failures stay reportable as the original token rejection, while defects
    // and interruption propagate through Effect.option.
    const rejection =
      response.status === 400 || response.status === 401
        ? yield* response.json.pipe(
            Effect.flatMap(
              Schema.decodeUnknownEffect(Schema.Struct({ error: Schema.Literal('invalid_grant') })),
            ),
            Effect.option,
          )
        : Option.none()
    return yield* new AuthError({
      reason: new AuthTokenError({
        message: 'Token endpoint rejected the grant',
        status: response.status,
        ...(Option.isSome(rejection) ? { grantRejection: rejection.value.error } : {}),
      }),
    })
  }
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
/**
 * Revokes a token using the protocol form-encoded endpoint.
 *
 * @category combinators
 */
export const revoke = Effect.fnUntraced(function* (
  endpoint: string,
  fields: Fields,
): Effect.fn.Return<void, AuthError, HttpClient.HttpClient> {
  const client = yield* HttpClient.HttpClient
  const response = yield* client
    .execute(
      HttpClientRequest.post(endpoint).pipe(
        HttpClientRequest.bodyUrlParams(yield* encodeFields(fields)),
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
