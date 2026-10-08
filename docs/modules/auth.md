# effect-harness/auth

Authorization primitives and application-owned credential storage for Effect.

Compose PKCE, token handling, JWT verification and credential persistence into provider account flows. Secrets use Redacted values; protocol and storage failures carry structured AuthError reasons.

## Install

```sh
npm install effect-harness effect
```

Use the equivalent `pnpm add`, `yarn add` or `bun add` command if you prefer.

## Choose a credential store

For an application that retains authorized accounts, compose the protected-file Layer with platform services:

```ts
import * as CredentialStore from 'effect-harness/auth/CredentialStore'

export const Credentials = CredentialStore.layerProtectedFile({
  path: './private-credentials/accounts.json',
})
```

Provide FileSystem, Path and Crypto at the application boundary. The store serializes credential updates and replaces the file atomically. Its file contains plaintext secrets protected by owner-only permissions; keep it outside committed source and served files.

Use `CredentialStore.layerMemory` for an ephemeral store; it requires Crypto. Use `JoseJwt.layer` with HttpClient when a provider flow needs signature and claim verification.

## Find the right API

| Module            | Purpose                                          |
| ----------------- | ------------------------------------------------ |
| `Credential`      | Credential variants and stable account keys      |
| `CredentialStore` | Credential lookup, updates and persistence       |
| `Pkce`            | Proof-key generation for authorization exchanges |
| `Token`           | Token protocol handling                          |
| `Jwt`, `JoseJwt`  | JWT verification contract and implementation     |

The application owns consent, browser navigation and callback delivery. Provider modules supply the authorization protocol and inference transport. Keeping these services separate lets the same credential store serve multiple provider flows.

## Continue

[Add account sign-in](../how-to/account-sign-in.md) walks through credential storage, ChatGPT consent, Anthropic authorization and installed Claude Code login. The [authorization reference](../reference/packages.md#authorization) records account identity, service dependencies and storage constraints.
