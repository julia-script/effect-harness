# @effect-harness/auth

Application-owned credentials and authorization building blocks. Public subpaths include `/Credential`, `/CredentialStore`, `/Pkce`, `/Token` and `/Jwt`. Secrets are `Redacted` values; provider services expose typed `AuthError` failures.

`CredentialStore.layerMemory` requires native Crypto. `layerProtectedFile({ path })` requires FileSystem, Path and Crypto, and stores credentials in a dedicated owner-only directory/file with atomic replacement and cross-process locking. The file contains secrets protected by permissions, not encryption. `modify` serializes credential updates, including refresh-token rotation. Stale locks are reported as busy rather than stolen; operators must establish that the prior process is gone before recovering one.

OAuth identity keys use verified provider/issuer/client/subject identity, not email. Pi Anthropic credentials use a distinct `OpaqueOAuth` record under a caller-selected account key and make no OIDC identity claim. API keys, OAuth credentials and dynamic client registrations have separate schemas. `Jwt.layer` verifies claims using JWKS and consumes a native HttpClient.

The host initiates consent explicitly and owns the browser/UI interaction. These services do not import another application's credential files. Follow [providers.md](../../docs/providers.md) for provider-specific begin/complete/refresh flows and scoped loopback callbacks. Credential protocol tests use fake HTTP and crypto boundaries; no live account is required by the public example.
