---
'effect-harness': minor
---

Support browser OAuth in the account registry, including the native Claude subscription adapter, authorization events, redacted completion submissions, and explicit cancellation over the service and RPC APIs. Sign-in credentials are staged and committed only after success; account reservations, stale-target checks, and removal fencing isolate concurrent account flows.

Add optional provider-asserted `Credential.Identity` and a separate sign-in `revision` to OAuth credentials and account status. Native Codex and Claude sign-ins generate a new revision; refresh preserves identity/revision and rejects known account changes. Claude retains account UUID/email metadata when supplied instead of generating a random `extra.identity`. Existing serialized credentials without the new fields remain readable.

Migration: custom OAuth adapters implement `login(account, flow)` and use `flow.onCode`, or `flow.onAuthorize` plus `flow.completion`. Sign-in consumers must handle the new `started` and `authorization` events and attempt IDs. Do not use `revision` as proof of account identity. Callback listeners, active-account policy, conversation bindings, and cross-process storage coordination remain host responsibilities.
