/**
 * Typed authentication failures and redacted credential codecs with compatible persisted wire representations.
 *
 * @since 0.0.0
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
 * Carries the AuthStorageError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthBusyError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthConfigurationError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthMissingError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthCallbackError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthDeniedError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthTokenError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthNetworkError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthIdentityError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthPermissionError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthExpiredError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Carries the AuthProtocolError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Defines AuthErrorReason for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
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
export type AuthErrorReason = typeof AuthErrorReason.Type
/**
 * Defines AuthErrorCode for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
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
export type AuthErrorCode = typeof AuthErrorCode.Type
/**
 * Describes the LegacyAuthErrorInput contract.
 *
 * @category types
 * @since 0.0.0
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
 * @since 0.0.0
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
 * Carries the AuthError reason with its original failure provenance.
 *
 * @category errors
 * @since 0.0.0
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
 * Defines Secret for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
 */
export const Secret = Schema.RedactedFromValue(Schema.NonEmptyString)
export type Secret = typeof Secret.Type
const ApiKeyFields = { provider: Schema.NonEmptyString, apiKey: Secret }
/**
 * Defines ApiKey for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
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
export type ApiKey = typeof ApiKey.Type
/**
 * Tests whether an unknown value satisfies the decoded ApiKey schema.
 *
 * @category guards
 * @since 0.0.0
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
 * Defines Registration for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
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
export type Registration = typeof Registration.Type
/**
 * Tests whether an unknown value satisfies the decoded Registration schema.
 *
 * @category guards
 * @since 0.0.0
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
 * Defines OAuth for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
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
export type OAuth = typeof OAuth.Type
/**
 * Tests whether an unknown value satisfies the decoded OAuth schema.
 *
 * @category guards
 * @since 0.0.0
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
 * Defines OpaqueOAuth for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
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
export type OpaqueOAuth = typeof OpaqueOAuth.Type
/**
 * Tests whether an unknown value satisfies the decoded OpaqueOAuth schema.
 *
 * @category guards
 * @since 0.0.0
 */
export const isOpaqueOAuth: (u: unknown) => u is OpaqueOAuth = Schema.is(OpaqueOAuth)
/**
 * Defines Credential for the Credential boundary.
 *
 * @category models
 * @since 0.0.0
 */
export const Credential = Schema.Union([ApiKey, OAuth, Registration, OpaqueOAuth])
export type Credential = typeof Credential.Type
/**
 * Tests whether an unknown value satisfies the decoded Credential schema.
 *
 * @category guards
 * @since 0.0.0
 */
export const isCredential: (u: unknown) => u is Credential = Schema.is(Credential)
/**
 * Returns the stable storage key for a verified account registration.
 *
 * **Details**
 *
 * Registration identity is provider, issued client and verified account, never email.
 *
 * @category combinators
 * @since 0.0.0
 */
export const accountKey = (
  self: Pick<OAuth, 'provider' | 'issuer' | 'clientId' | 'subject'>,
): string => JSON.stringify([self.provider, self.issuer, self.clientId, self.subject])
