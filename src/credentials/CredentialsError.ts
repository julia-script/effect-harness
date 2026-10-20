/**
 * Errors a `Credentials` store reports.
 */
import { Schema } from 'effect'

/** The store failed: it could not be read or written. */
export class CredentialsError extends Schema.TaggedError<
  CredentialsError,
  { readonly brand: unique symbol }
>('effect-harness/credentials/CredentialsError/CredentialsError')('CredentialsError', {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/** An `update` expected a credential that is no longer stored: another writer changed it. */
export class CredentialChanged extends Schema.TaggedError<
  CredentialChanged,
  { readonly brand: unique symbol }
>('effect-harness/credentials/CredentialsError/CredentialChanged')('CredentialChanged', {
  message: Schema.String,
  provider: Schema.String,
}) {}

/** The store cannot be written, as a `.env` file through `Config` cannot. */
export class ReadOnly extends Schema.TaggedError<ReadOnly, { readonly brand: unique symbol }>(
  'effect-harness/credentials/CredentialsError/ReadOnly',
)('ReadOnly', {
  message: Schema.String,
  provider: Schema.String,
}) {}
