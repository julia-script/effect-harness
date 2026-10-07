/**
 * Typed authentication failures and redacted credential codecs with compatible persisted wire representations.
 */
import * as Time from './Time.ts'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Struct from 'effect/Struct'

const AuthReasonFields = {
  message: Schema.String,
  status: Schema.optional(Schema.Int),
  // Keep the exact runtime value. Explicit JSON codecs use Defect's documented
  // Error name/message/cause representation, without preserving identity or stack.
  cause: Schema.optional(Schema.Defect()),
  retryAfter: Schema.optional(Schema.Duration),
}
/** Ordinary inspection omits sensitive foreign diagnostics; explicit schema codecs preserve provenance. */
const diagnostic = (reason: {
  readonly _tag: string
  readonly message: string
  readonly status?: number | undefined
  readonly cause?: unknown
}) => ({
  _tag: reason._tag,
  message: reason.message,
  ...(reason.status === undefined ? {} : { status: reason.status }),
  ...(reason.cause === undefined ? {} : { cause: '[REDACTED]' }),
})

/**
 * Failure reading or replacing application-owned credential storage.
 *
 * @category errors
 */
export class AuthStorageError extends Schema.Error<AuthStorageError>(
  '@effect-harness/auth/Credential/AuthStorageError',
)({
  _tag: Schema.tag('AuthStorageError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'storage' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure acquiring the credential store’s cross-process update lock.
 *
 * @category errors
 */
export class AuthBusyError extends Schema.Error<AuthBusyError>(
  '@effect-harness/auth/Credential/AuthBusyError',
)({
  _tag: Schema.tag('AuthBusyError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'busy' as const
  }
  get isRetryable(): boolean {
    return true
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure caused by invalid authentication configuration or unavailable cryptographic setup.
 *
 * @category errors
 */
export class AuthConfigurationError extends Schema.Error<AuthConfigurationError>(
  '@effect-harness/auth/Credential/AuthConfigurationError',
)({
  _tag: Schema.tag('AuthConfigurationError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'configuration' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure reporting an absent credential or account registration.
 *
 * @category errors
 */
export class AuthMissingError extends Schema.Error<AuthMissingError>(
  '@effect-harness/auth/Credential/AuthMissingError',
)({
  _tag: Schema.tag('AuthMissingError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'missing' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure validating a consent callback or its pending authorization.
 *
 * @category errors
 */
export class AuthCallbackError extends Schema.Error<AuthCallbackError>(
  '@effect-harness/auth/Credential/AuthCallbackError',
)({
  _tag: Schema.tag('AuthCallbackError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'callback' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure reporting consent denied by the authorization server.
 *
 * @category errors
 */
export class AuthDeniedError extends Schema.Error<AuthDeniedError>(
  '@effect-harness/auth/Credential/AuthDeniedError',
)({
  _tag: Schema.tag('AuthDeniedError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'denied' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure exchanging, refreshing or validating an account token grant.
 *
 * @category errors
 */
export class AuthTokenError extends Schema.Error<AuthTokenError>(
  '@effect-harness/auth/Credential/AuthTokenError',
)({
  _tag: Schema.tag('AuthTokenError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'token' as const
  }
  get isRetryable(): boolean {
    return (
      this.status === 429 || (this.status !== undefined && this.status >= 500 && this.status < 600)
    )
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure communicating with an authentication endpoint.
 *
 * @category errors
 */
export class AuthNetworkError extends Schema.Error<AuthNetworkError>(
  '@effect-harness/auth/Credential/AuthNetworkError',
)({
  _tag: Schema.tag('AuthNetworkError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'network' as const
  }
  get isRetryable(): boolean {
    return true
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure verifying an identity token or its expected claims.
 *
 * @category errors
 */
export class AuthIdentityError extends Schema.Error<AuthIdentityError>(
  '@effect-harness/auth/Credential/AuthIdentityError',
)({
  _tag: Schema.tag('AuthIdentityError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'identity' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure reporting missing granted permission for the requested provider operation.
 *
 * @category errors
 */
export class AuthPermissionError extends Schema.Error<AuthPermissionError>(
  '@effect-harness/auth/Credential/AuthPermissionError',
)({
  _tag: Schema.tag('AuthPermissionError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'permission' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure reporting an expired authorization or token state.
 *
 * @category errors
 */
export class AuthExpiredError extends Schema.Error<AuthExpiredError>(
  '@effect-harness/auth/Credential/AuthExpiredError',
)({
  _tag: Schema.tag('AuthExpiredError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'expired' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Failure reporting an unexpected authentication protocol response.
 *
 * @category errors
 */
export class AuthProtocolError extends Schema.Error<AuthProtocolError>(
  '@effect-harness/auth/Credential/AuthProtocolError',
)({
  _tag: Schema.tag('AuthProtocolError'),
  ...AuthReasonFields,
}) {
  get code() {
    return 'protocol' as const
  }
  get isRetryable(): boolean {
    return false
  }
  override toJSON() {
    return diagnostic(this)
  }
}

/**
 * Schema for structured authentication, consent, token, identity and storage failures.
 *
 * @category models
 */
export const AuthErrorReason = Schema.Union([
  AuthStorageError,
  AuthBusyError,
  AuthConfigurationError,
  AuthMissingError,
  AuthCallbackError,
  AuthDeniedError,
  AuthTokenError,
  AuthNetworkError,
  AuthIdentityError,
  AuthPermissionError,
  AuthExpiredError,
  AuthProtocolError,
])
/**
 * Decoded value validated by the `AuthErrorReason` schema.
 *
 * @category models
 */
export type AuthErrorReason = typeof AuthErrorReason.Type
/**
 * Schema for legacy authentication reason codes.
 *
 * @category models
 */
export const AuthErrorCode = Schema.Literals([
  'storage',
  'busy',
  'configuration',
  'missing',
  'callback',
  'denied',
  'token',
  'network',
  'identity',
  'permission',
  'expired',
  'protocol',
])
/**
 * Decoded value validated by the `AuthErrorCode` schema.
 *
 * @category models
 */
export type AuthErrorCode = typeof AuthErrorCode.Type
/**
 * Legacy error code and details accepted when constructing a structured auth reason.
 *
 * @category models
 */
export interface LegacyAuthErrorInput {
  readonly reason: AuthErrorCode
  readonly message: string
  readonly status?: number | undefined
  readonly cause?: unknown
  readonly retryAfter?: import('effect/Duration').Duration | undefined
}
/**
 * Constructs a tagged authentication reason from a legacy error input.
 *
 * **Details**
 *
 * Explicit input adapter; the runtime reason is always a tagged leaf, never a literal code.
 *
 * @category constructors
 */
export const makeAuthErrorReason = ({
  reason,
  ...fields
}: LegacyAuthErrorInput): AuthErrorReason => {
  switch (reason) {
    case 'storage':
      return new AuthStorageError(fields)
    case 'busy':
      return new AuthBusyError(fields)
    case 'configuration':
      return new AuthConfigurationError(fields)
    case 'missing':
      return new AuthMissingError(fields)
    case 'callback':
      return new AuthCallbackError(fields)
    case 'denied':
      return new AuthDeniedError(fields)
    case 'token':
      return new AuthTokenError(fields)
    case 'network':
      return new AuthNetworkError(fields)
    case 'identity':
      return new AuthIdentityError(fields)
    case 'permission':
      return new AuthPermissionError(fields)
    case 'expired':
      return new AuthExpiredError(fields)
    case 'protocol':
      return new AuthProtocolError(fields)
  }
}

/**
 * Structured account authorization, token, identity or credential-storage failure.
 *
 * **Details**
 *
 * Match reason._tag for the stable failure category. Runtime causes remain available for
 * diagnosis; ordinary JSON inspection redacts foreign diagnostics that may contain secrets.
 *
 * **Gotchas**
 *
 * Explicit schema encoding can retain diagnostic causes. Treat encoded authentication
 * failures as sensitive data.
 *
 * @category errors
 */
export class AuthError extends Schema.Error<AuthError>('@effect-harness/auth/Credential/AuthError')(
  {
    _tag: Schema.tag('AuthError'),
    reason: AuthErrorReason,
  },
) {
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get status(): number | undefined {
    return this.reason.status
  }
  get code(): AuthErrorCode {
    return this.reason.code
  }
  get isRetryable(): boolean {
    return this.reason.isRetryable
  }
  get retryAfter() {
    return this.reason.retryAfter
  }
  override toJSON() {
    return { _tag: this._tag, reason: this.reason.toJSON() }
  }
  static fromLegacy(input: LegacyAuthErrorInput): AuthError {
    return new AuthError({ reason: makeAuthErrorReason(input) })
  }
}

/**
 * Nonempty secret decoded to a Redacted value.
 *
 * @category models
 */
export const Secret = Schema.RedactedFromValue(Schema.NonEmptyString)
/**
 * Nonempty secret decoded to a Redacted value.
 *
 * @category models
 */
export type Secret = typeof Secret.Type
const ApiKeyFields = { provider: Schema.NonEmptyString, apiKey: Secret }
/**
 * Provider API key stored under an application-owned account key.
 *
 * @category models
 */
export const ApiKey = Schema.Struct({ kind: Schema.tag('apiKey'), ...ApiKeyFields }).pipe(
  Schema.decodeTo(
    Schema.TaggedStruct('apiKey', {
      provider: Schema.NonEmptyString,
      apiKey: Schema.toType(Secret),
    }),
    {
      decode: SchemaGetter.transform((self) => ({
        ...Struct.omit(self, ['kind']),
        _tag: 'apiKey' as const,
      })),
      encode: SchemaGetter.transform((self) => ({
        ...Struct.omit(self, ['_tag']),
        kind: 'apiKey' as const,
      })),
    },
  ),
)
/**
 * Provider API key stored under an application-owned account key.
 *
 * @category models
 */
export type ApiKey = typeof ApiKey.Type
/**
 * Checks whether a value satisfies the decoded `ApiKey` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isApiKey: (u: unknown) => u is ApiKey = Schema.is(ApiKey)
const RegistrationFields = {
  provider: Schema.NonEmptyString,
  issuer: Schema.NonEmptyString,
  subject: Schema.NonEmptyString,
  clientId: Schema.NonEmptyString,
  hostId: Schema.NonEmptyString,
  email: Schema.optional(Schema.String),
  redirectUri: Schema.optional(Schema.String),
}
/**
 * Dynamic OAuth client registration retained for later sign-in.
 *
 * @category models
 */
export const Registration = Schema.Struct({
  kind: Schema.tag('registration'),
  ...RegistrationFields,
}).pipe(
  Schema.decodeTo(Schema.TaggedStruct('registration', RegistrationFields), {
    decode: SchemaGetter.transform((self) => ({
      ...Struct.omit(self, ['kind']),
      _tag: 'registration' as const,
    })),
    encode: SchemaGetter.transform((self) => ({
      ...Struct.omit(self, ['_tag']),
      kind: 'registration' as const,
    })),
  }),
)
/**
 * Dynamic OAuth client registration retained for later sign-in.
 *
 * @category models
 */
export type Registration = typeof Registration.Type
/**
 * Checks whether a value satisfies the decoded `Registration` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isRegistration: (u: unknown) => u is Registration = Schema.is(Registration)
const OAuthFields = {
  ...RegistrationFields,
  accessToken: Secret,
  refreshToken: Secret,
  idToken: Secret,
  scopes: Schema.Array(Schema.NonEmptyString),
  expiresAt: Time.EpochMillis,
  earliestRefreshAt: Schema.optional(Time.EpochMillis),
}
/**
 * Verified OAuth account identity and refreshable token grant.
 *
 * @category models
 */
export const OAuth = Schema.Struct({ kind: Schema.tag('oauth'), ...OAuthFields }).pipe(
  Schema.decodeTo(
    Schema.TaggedStruct('oauth', {
      ...RegistrationFields,
      accessToken: Schema.toType(Secret),
      refreshToken: Schema.toType(Secret),
      idToken: Schema.toType(Secret),
      scopes: Schema.Array(Schema.NonEmptyString),
      expiresAt: Schema.toType(Time.EpochMillis),
      earliestRefreshAt: Schema.optional(Schema.toType(Time.EpochMillis)),
    }),
    {
      decode: SchemaGetter.transform((self) => ({
        ...Struct.omit(self, ['kind']),
        _tag: 'oauth' as const,
      })),
      encode: SchemaGetter.transform((self) => ({
        ...Struct.omit(self, ['_tag']),
        kind: 'oauth' as const,
      })),
    },
  ),
)
/**
 * Verified OAuth account identity and refreshable token grant.
 *
 * @category models
 */
export type OAuth = typeof OAuth.Type
/**
 * Checks whether a value satisfies the decoded `OAuth` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isOAuth: (u: unknown) => u is OAuth = Schema.is(OAuth)
/** Opaque OAuth grants carry no verified OIDC subject or identity token. The caller owns the storage key. */
const OpaqueOAuthFields = {
  provider: Schema.NonEmptyString,
  authorizationServer: Schema.NonEmptyString,
  clientId: Schema.NonEmptyString,
  accessToken: Secret,
  refreshToken: Secret,
  scopes: Schema.Array(Schema.NonEmptyString),
  expiresAt: Time.EpochMillis,
  redirectUri: Schema.optional(Schema.String),
}
/**
 * Schema for an OAuth grant under a caller-selected account key.
 *
 * **Details**
 *
 * Stores provider, authorization server and refreshable secrets without claiming a verified
 * OIDC subject. Anthropic account authorization uses this representation.
 *
 * @category models
 */
export const OpaqueOAuth = Schema.Struct({
  kind: Schema.tag('opaqueOAuth'),
  ...OpaqueOAuthFields,
}).pipe(
  Schema.decodeTo(
    Schema.TaggedStruct('opaqueOAuth', {
      provider: Schema.NonEmptyString,
      authorizationServer: Schema.NonEmptyString,
      clientId: Schema.NonEmptyString,
      accessToken: Schema.toType(Secret),
      refreshToken: Schema.toType(Secret),
      scopes: Schema.Array(Schema.NonEmptyString),
      expiresAt: Schema.toType(Time.EpochMillis),
      redirectUri: Schema.optional(Schema.String),
    }),
    {
      decode: SchemaGetter.transform((self) => ({
        ...Struct.omit(self, ['kind']),
        _tag: 'opaqueOAuth' as const,
      })),
      encode: SchemaGetter.transform((self) => ({
        ...Struct.omit(self, ['_tag']),
        kind: 'opaqueOAuth' as const,
      })),
    },
  ),
)
/**
 * Refreshable OAuth grant under a caller-selected key without an OIDC identity claim.
 *
 * @category models
 */
export type OpaqueOAuth = typeof OpaqueOAuth.Type
/**
 * Checks whether a value satisfies the decoded `OpaqueOAuth` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isOpaqueOAuth: (u: unknown) => u is OpaqueOAuth = Schema.is(OpaqueOAuth)
/**
 * API key, client registration or account token grant.
 *
 * @category models
 */
export const Credential = Schema.Union([ApiKey, OAuth, Registration, OpaqueOAuth])
/**
 * API key, client registration or account token grant.
 *
 * @category models
 */
export type Credential = typeof Credential.Type
/**
 * Checks whether a value satisfies the decoded `Credential` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isCredential: (u: unknown) => u is Credential = Schema.is(Credential)
/**
 * Builds an account key from provider and verified issuer/client/subject identity.
 *
 * **Gotchas**
 *
 * Do not substitute email or an unverified claim for the identity tuple.
 *
 * @category combinators
 */
export const accountKey = (
  self: Pick<OAuth, 'provider' | 'issuer' | 'clientId' | 'subject'>,
): string => JSON.stringify([self.provider, self.issuer, self.clientId, self.subject])
