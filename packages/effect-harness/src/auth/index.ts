/**
 * Public concept namespaces for this package.
 */
// @barrel
/**
 * Typed authentication failures and redacted credential codecs with compatible persisted
 * wire representations.
 *
 * @category re-exports
 */
export * as Credential from './Credential.ts'
/**
 * Locked in-memory and protected-file credential transactions with scoped atomic
 * persistence.
 *
 * @category re-exports
 */
export * as CredentialStore from './CredentialStore.ts'
/**
 * Guarded normalization of native and external duration inputs.
 *
 * @category re-exports
 */
export * as Duration from './Duration.ts'
/**
 * JOSE-backed JWT verification with fresh key reads and Effect-clock expiry checks.
 *
 * @category re-exports
 */
export * as JoseJwt from './JoseJwt.ts'
/**
 * Portable verified JWT identity and verification service contracts.
 *
 * @category re-exports
 */
export * as Jwt from './Jwt.ts'
/**
 * Cryptographic PKCE challenges and unpadded base64url encoding.
 *
 * @category re-exports
 */
export * as Pkce from './Pkce.ts'
/**
 * Exact finite epoch-millisecond codecs for UTC domain timestamps.
 *
 * @category re-exports
 */
export * as Time from './Time.ts'
/**
 * Redacted OAuth token and revocation HTTP boundaries.
 *
 * @category re-exports
 */
export * as Token from './Token.ts'
