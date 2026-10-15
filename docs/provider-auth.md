# Provider credentials

`effect-harness/provider-auth` separates live credentials from conversation storage. It exposes schema-backed OAuth/API-key values, a credential store contract, sanitized errors, and per-attempt OAuth resolution. Optional Node/Bun file storage and loopback callbacks live in `effect-harness/provider-auth-node`.

```ts
import * as Effect from 'effect/Effect'
import { CredentialStore } from 'effect-harness/provider-auth'
import { FileSystemCredentialStore } from 'effect-harness/provider-auth-node'

const status = Effect.gen(function* () {
  const store = yield* FileSystemCredentialStore.make({ path: './private/credentials.json' })
  return yield* store.list // provider/profile/identity/type/expiry, never tokens
})

const logout = CredentialStore.logout({ provider: 'openai-codex', profile: 'personal' })
// Provide the selected CredentialStore to logout and provider constructors.
```

Use `MemoryCredentialStore.layer` from `effect-harness/provider-auth` for temporary storage. The file store uses an explicit path, 0600 files, synchronized atomic replacement, and a cross-process lock. The store opens its directory for synchronization before publication. `StoreFailure` means publication did not occur; `StoreCommitUncertain` means the replacement was published but subsequent durability or lock cleanup failed. Reread the credential before retrying an uncertain commit, and never assume remote token rotation was rolled back. Its contents are plaintext secrets; use a private local directory or implement the service with a keychain. It never reads a home-directory credential file. Stale locks are not stolen: after a process crash, establish that the owner has stopped before removing its `.lock` file.

OAuth tokens are `Redacted` values. `expiresAt` is Unix milliseconds. Bindings identify `{ provider, profile, identity }`; each login or account replacement must assign a new identity, while refresh preserves it. `save` imports a credential, `list` returns metadata, and `logout` removes the selected profile locally. This does not remotely revoke tokens or retract accepted inference requests.

`Resolver.make` captures the store and returns a `resolve` Effect. It checks an injected Clock for each attempt. Its default minimum validity is five minutes; zero checks only actual expiry. Omitting its refresh callback selects external-refresh mode, which rejects expired tokens. Refresh failures never select an ambient API key.

Stores must serialize the entire refresh/read/recheck/persist operation through `modify`, including logout and replacement. Waiting for a lock is interruptible. Once refresh starts, a bounded worker (15 seconds by default) finishes and its result is persisted before the transaction unlocks, even if its original caller is interrupted. A crash between remote rotation and local commit can require re-login. Custom stores must bound I/O and coordinate every process sharing credentials; an in-process semaphore alone is insufficient.

The optional `Login.CallbackServer` service is acquired only during an explicit login. Its Node implementation binds loopback, validates state, rejects duplicate completion, and releases its listener with the login scope. Interaction callbacks are application-owned cancellable Effects. No generation operation displays a prompt or opens a browser.

Credential values, token HTTP bodies, and raw transport causes must stay out of prepared requests, Session documents, response metadata, logs, and tool results. AuthError contains a semantic reason without remote response text or a nested transport cause.
