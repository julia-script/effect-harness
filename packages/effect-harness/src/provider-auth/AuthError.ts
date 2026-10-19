/** Sanitized authentication failures. Never attach remote bodies, requests or nested causes. */
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'

export class AuthError extends Schema.TaggedError<AuthError>(
  'effect-harness/provider-auth/AuthError',
)('AuthError', {
  reason: Schema.Literals([
    'MissingCredential',
    'ExpiredCredential',
    'InvalidGrant',
    'InvalidResponse',
    'AccountMismatch',
    'UnsupportedAuthMode',
    'StoreFailure',
    'StoreCommitUncertain',
    'TransportFailure',
    'Timeout',
    'InvalidState',
    'AccessDenied',
    'InvalidConfiguration',
  ]),
}) {
  override get message(): string {
    return `Provider authentication: ${this.reason}`
  }
  get isRetryable(): boolean {
    return this.reason === 'TransportFailure' || this.reason === 'Timeout'
  }
}

/** Retains semantic retry classification without retaining the credential-bearing cause. */
export const toAiError = (error: AuthError): AiError.AiError =>
  new AiError.AiError({
    module: 'ProviderAuth',
    method: 'resolve',
    reason: error.isRetryable
      ? new AiError.InternalProviderError({ description: error.message })
      : new AiError.AuthenticationError({ kind: 'Unknown', description: error.message }),
  })
