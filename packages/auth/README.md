# @effect-harness/auth

Application-owned credentials and authorization building blocks for Effect v4. Secrets use Redacted values, and protocol/storage failures use structured AuthError reasons.

```sh
bun add @effect-harness/auth effect@4.0.1
```

Public subpaths include `Credential`, `CredentialStore`, `Pkce`, `Token`, `Jwt` and `JoseJwt`. Root imports expose concept namespaces.

CredentialStore.layerMemory requires Crypto. Its protected-file Layer requires FileSystem, Path and Crypto, with atomic replacement and serialized credential updates. Files contain plaintext secrets protected by owner-only permissions; they are not encrypted. JoseJwt.layer supplies signature and claim verification using HttpClient.

Follow [account sign-in](../../docs/how-to/account-sign-in.md) to compose these services with a provider's authorization flow. [Authorization reference](../../docs/reference/packages.md#authorization) describes account identities, service dependencies and storage constraints.
