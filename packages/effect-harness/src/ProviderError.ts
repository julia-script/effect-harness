/** Public provider-management errors never contain configuration or secret values. */
import * as Schema from 'effect/Schema'
export class ProviderError extends Schema.TaggedError<ProviderError>(
  'effect-harness/ProviderError',
)('ProviderError', {
  reason: Schema.Literals([
    'UnknownProvider',
    'NotFound',
    'InvalidConfiguration',
    'UnsupportedVersion',
    'Conflict',
    'StoreFailure',
    'StoreCommitUncertain',
    'CredentialCleanupFailed',
  ]),
  instanceId: Schema.optionalKey(Schema.String),
}) {
  override get message(): string {
    return `Provider management: ${this.reason}`
  }
}
