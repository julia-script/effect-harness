# Provider credentials

`effect-harness/provider-auth` separates live credentials from conversation storage. It exposes schema-backed OAuth/API-key values, a credential store contract, sanitized errors, and per-attempt OAuth resolution. Optional Node/Bun file storage and loopback callbacks live in `effect-harness/provider-auth-node`.

The provider manager captures an injected credential store. On `add` or credential
replacement it validates the definition's secrets schema, asks the credential
manager to allocate a fresh reference, and saves only `{ credentialId, identity }`
in the public instance. `providers.list` and `providers.get` never return secrets.

```ts
// Authentication is explicit; login helpers return secrets without persisting them.
const secrets = yield * OAuth.loginBrowser({ interaction })
yield * providers.update(instance.id, { secrets })
// Local logout keeps the instance and its public config.
yield * providers.update(instance.id, { secrets: null })
```

The low-level `CredentialStore.create(schema, secrets)` returns a binding.
`read(binding, schema)` verifies identity and decodes secrets. `save` imports an
encoded `{ binding, value: Redacted<Json> }` envelope; `remove(binding)` deletes it.
The acquired service's `list` returns only credential IDs and identities, never
secret fields. Applications normally use `Providers` for these operations.

Public instance and credential stores are separate transaction domains. The manager
publishes the new credential reference before retiring the old credential. A known
public commit failure cleans up the new credential; `StoreCommitUncertain` retains
credentials that may still be referenced. `CredentialCleanupFailed` means publication
may have succeeded but retirement failed: reread the instance before retrying.
Interrupted acquired writes finish and reconcile references before cleanup.
A process crash can still leave unreferenced credentials; reconcile references
against all public instances before removing them. Never share one store path between
public instances and credentials.

Use `MemoryCredentialStore.layer` from `effect-harness/provider-auth` for temporary storage. The file store uses an explicit path, 0600 files, synchronized atomic replacement, and a cross-process lock. The store opens its directory for synchronization before publication. `StoreFailure` means publication did not occur; `StoreCommitUncertain` means the replacement was published but subsequent durability or lock cleanup failed. Reread the credential before retrying an uncertain commit, and never assume remote token rotation was rolled back. Its contents are plaintext secrets; use a private local directory or implement the service with a keychain. It never reads a home-directory credential file. Stale locks are not stolen: after a process crash, establish that the owner has stopped before removing its `.lock` file.

Bun applications can use `BunSecretsCredentialStore` from `effect-harness/provider-auth-bun` to keep credentials in the OS credential manager:

```ts
import { BunSecretsCredentialStore } from 'effect-harness/provider-auth-bun'

const credentials = BunSecretsCredentialStore.layer({
  service: 'com.example.my-app',
  lockPath: '/absolute/private/my-app/credentials.lock',
})
// Provide credentials to effects that require CredentialStore.CredentialStore.
```

`make` returns a store directly. `name` defaults to `credentials` and `lockTimeoutMs` to 20,000. Native persistence uses Bun’s default policy. All writers for the same service/name must share the same private local `lockPath`. Only the empty lock file is written to disk; the credential collection is stored as one schema-validated JSON secret. This allows consistent listing and replacement without a separate index, but the **whole collection must fit the platform's secret-size limit**. Removing the final credential deletes the native entry. Do not modify this entry outside the store's lock protocol or share it through Windows roaming across concurrently writing machines.

[Bun Secrets](https://bun.com/docs/runtime/secrets) requires an available OS credential service (macOS Keychain, Linux libsecret, or Windows Credential Manager). The OS may prompt to unlock or grant access. Importing the module does not access the keychain; constructing it outside a runtime with `Bun.secrets` fails with `InvalidConfiguration`. Native operations have no cancellation API, so an acquired transaction waits for them to settle before releasing its lock; `lockTimeoutMs` bounds contention only. Native write/delete failures return `StoreCommitUncertain`, since Bun does not promise that a rejected mutation left the entry unchanged. Read/decode failures return `StoreFailure`, and raw keychain errors never escape. Stale locks require the same manual recovery as the file store.

OAuth tokens are `Redacted` values. `expiresAt` is Unix milliseconds. Bindings identify `{ credentialId, identity }`; the manager assigns a new reference and identity for each credential replacement, while refresh preserves both. Login helpers return unbound secrets. Removing secrets from an instance performs local logout. This does not remotely revoke tokens or retract accepted inference requests.

`Resolver.make` captures the store and returns a `resolve` Effect. It checks an injected Clock for each attempt. Its default minimum validity is five minutes; zero checks only actual expiry. Omitting its refresh callback selects external-refresh mode, which rejects expired tokens. Refresh failures never select an ambient API key.

Stores must serialize the entire refresh/read/recheck/persist operation through `modify`, including logout and replacement. Waiting for a lock is interruptible. Once refresh starts, a bounded worker (15 seconds by default) finishes and its result is persisted before the transaction unlocks, even if its original caller is interrupted. A crash between remote rotation and local commit can require re-login. Custom stores must bound I/O and coordinate every process sharing credentials; an in-process semaphore alone is insufficient.

The optional `Login.CallbackServer` service is acquired only during an explicit login. Its Node implementation binds loopback, validates state, rejects duplicate completion, and releases its listener with the login scope. Interaction callbacks are application-owned cancellable Effects. No generation operation displays a prompt or opens a browser.

Credential values, token HTTP bodies, and raw transport causes must stay out of prepared requests, Session documents, response metadata, logs, and tool results. AuthError contains a semantic reason without remote response text or a nested transport cause.
