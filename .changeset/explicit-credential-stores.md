---
'effect-harness': minor
---

Give credential store implementations explicit backend names. Import `FileSystemCredentialStore` from `effect-harness/provider-auth-node` (or its `/FileSystemCredentialStore` subpath), and `MemoryCredentialStore` from `effect-harness/provider-auth` (or its `/MemoryCredentialStore` subpath). Both modules expose `make` and `layer`; `CredentialStore` remains the shared service contract.

This is a breaking API change: replace `provider-auth-node/CredentialStore` with `provider-auth-node/FileSystemCredentialStore`, and replace `CredentialStore.makeMemory` / `CredentialStore.layerMemory` with `MemoryCredentialStore.make` / `MemoryCredentialStore.layer`.
