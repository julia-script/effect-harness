/**
 * Provider credentials, shared by a client and its loops through one store.
 */

/** The `Credentials` service and its stores. */
export * as Credentials from './Credentials.ts'

/** Stored credentials: API keys and OAuth tokens. */
export * as Credential from './Credential.ts'

/** The protocol of a remote store. */
export * as CredentialsRpcs from './CredentialsRpcs.ts'

/** Errors a store reports. */
export * as CredentialsError from './CredentialsError.ts'
