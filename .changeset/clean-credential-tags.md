---
'effect-harness': minor
---

Rename the credentials service, module file, and credentials subpath namespace export to `CredentialStore`.

Use tagged structs for credentials and their metadata: API keys use `_tag: 'ApiKey'`, OAuth credentials use `_tag: 'OAuth'`, identities use `_tag: 'Identity'`, and credential listings use `_tag: 'Info'` with a separate `type: 'ApiKey' | 'OAuth'` field.

This changes the public API and serialized credential format. Existing `type: 'api_key'` / `type: 'oauth'` records are not migrated or decoded.
