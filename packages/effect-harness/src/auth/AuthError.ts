/**
 * Shared typed authentication failures with redacted diagnostics and structured error codecs.
 */
import * as Schema from 'effect/Schema'
import * as ErrorReporter from 'effect/ErrorReporter'

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
  '@effect-harness/auth/AuthError/AuthStorageError',
)({
  _tag: Schema.tag('AuthStorageError'),
  ...AuthReasonFields,
}) {
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
  '@effect-harness/auth/AuthError/AuthBusyError',
)({
  _tag: Schema.tag('AuthBusyError'),
  ...AuthReasonFields,
}) {
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
  '@effect-harness/auth/AuthError/AuthConfigurationError',
)({
  _tag: Schema.tag('AuthConfigurationError'),
  ...AuthReasonFields,
}) {
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
  '@effect-harness/auth/AuthError/AuthMissingError',
)({
  _tag: Schema.tag('AuthMissingError'),
  ...AuthReasonFields,
}) {
  override get [ErrorReporter.ignore](): boolean {
    return this.status === undefined || this.status === 404
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
  '@effect-harness/auth/AuthError/AuthCallbackError',
)({
  _tag: Schema.tag('AuthCallbackError'),
  ...AuthReasonFields,
}) {
  override get [ErrorReporter.ignore](): boolean {
    return this.status === undefined || this.status === 400
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
  '@effect-harness/auth/AuthError/AuthDeniedError',
)({
  _tag: Schema.tag('AuthDeniedError'),
  ...AuthReasonFields,
  // Only an explicitly classified access_denied callback is an expected denial.
  authorizationError: Schema.optional(Schema.Literal('access_denied')),
}) {
  override get [ErrorReporter.ignore](): boolean {
    return (
      this.authorizationError === 'access_denied' &&
      (this.status === undefined ||
        this.status === 400 ||
        this.status === 401 ||
        this.status === 403)
    )
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
  '@effect-harness/auth/AuthError/AuthTokenError',
)({
  _tag: Schema.tag('AuthTokenError'),
  ...AuthReasonFields,
  // Unknown, invalid-client, throttled and server grant failures remain reportable.
  grantRejection: Schema.optional(Schema.Literal('invalid_grant')),
}) {
  override get [ErrorReporter.ignore](): boolean {
    return this.grantRejection === 'invalid_grant' && (this.status === 400 || this.status === 401)
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
  '@effect-harness/auth/AuthError/AuthNetworkError',
)({
  _tag: Schema.tag('AuthNetworkError'),
  ...AuthReasonFields,
}) {
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
  '@effect-harness/auth/AuthError/AuthIdentityError',
)({
  _tag: Schema.tag('AuthIdentityError'),
  ...AuthReasonFields,
}) {
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
  '@effect-harness/auth/AuthError/AuthPermissionError',
)({
  _tag: Schema.tag('AuthPermissionError'),
  ...AuthReasonFields,
}) {
  override get [ErrorReporter.ignore](): boolean {
    return this.status === undefined || this.status === 401 || this.status === 403
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
  '@effect-harness/auth/AuthError/AuthExpiredError',
)({
  _tag: Schema.tag('AuthExpiredError'),
  ...AuthReasonFields,
}) {
  override get [ErrorReporter.ignore](): boolean {
    return this.status === undefined || this.status === 401
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
  '@effect-harness/auth/AuthError/AuthProtocolError',
)({
  _tag: Schema.tag('AuthProtocolError'),
  ...AuthReasonFields,
}) {
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
export class AuthError extends Schema.Error<AuthError>('@effect-harness/auth/AuthError/AuthError')({
  _tag: Schema.tag('AuthError'),
  reason: AuthErrorReason,
}) {
  override get [ErrorReporter.ignore](): boolean {
    return ErrorReporter.isIgnored(this.reason)
  }
  override get message(): string {
    return this.reason.message
  }
  override get cause(): unknown {
    return this.reason.cause
  }
  get status(): number | undefined {
    return this.reason.status
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
}

/** Checks the decoded AuthErrorReason contract without decoding or coercing input.
 * @category guards
 */
export const isAuthErrorReason: (u: unknown) => u is AuthErrorReason = Schema.is(
  Schema.toType(AuthErrorReason),
)

/** Checks the decoded AuthStorageError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthStorageError: (u: unknown) => u is AuthStorageError = Schema.is(
  Schema.toType(AuthStorageError),
)

/** Checks the decoded AuthBusyError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthBusyError: (u: unknown) => u is AuthBusyError = Schema.is(
  Schema.toType(AuthBusyError),
)

/** Checks the decoded AuthConfigurationError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthConfigurationError: (u: unknown) => u is AuthConfigurationError = Schema.is(
  Schema.toType(AuthConfigurationError),
)

/** Checks the decoded AuthMissingError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthMissingError: (u: unknown) => u is AuthMissingError = Schema.is(
  Schema.toType(AuthMissingError),
)

/** Checks the decoded AuthCallbackError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthCallbackError: (u: unknown) => u is AuthCallbackError = Schema.is(
  Schema.toType(AuthCallbackError),
)

/** Checks the decoded AuthDeniedError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthDeniedError: (u: unknown) => u is AuthDeniedError = Schema.is(
  Schema.toType(AuthDeniedError),
)

/** Checks the decoded AuthTokenError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthTokenError: (u: unknown) => u is AuthTokenError = Schema.is(
  Schema.toType(AuthTokenError),
)

/** Checks the decoded AuthNetworkError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthNetworkError: (u: unknown) => u is AuthNetworkError = Schema.is(
  Schema.toType(AuthNetworkError),
)

/** Checks the decoded AuthIdentityError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthIdentityError: (u: unknown) => u is AuthIdentityError = Schema.is(
  Schema.toType(AuthIdentityError),
)

/** Checks the decoded AuthPermissionError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthPermissionError: (u: unknown) => u is AuthPermissionError = Schema.is(
  Schema.toType(AuthPermissionError),
)

/** Checks the decoded AuthExpiredError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthExpiredError: (u: unknown) => u is AuthExpiredError = Schema.is(
  Schema.toType(AuthExpiredError),
)

/** Checks the decoded AuthProtocolError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthProtocolError: (u: unknown) => u is AuthProtocolError = Schema.is(
  Schema.toType(AuthProtocolError),
)

/** Checks the decoded AuthError contract without decoding or coercing input.
 * @category guards
 */
export const isAuthError: (u: unknown) => u is AuthError = Schema.is(Schema.toType(AuthError))
