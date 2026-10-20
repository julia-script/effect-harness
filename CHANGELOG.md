# effect-harness

## 0.9.0

### Minor Changes

- 33f7679: Tool handlers can require the `Harness` service: the tool task provides the Harness it runs under to each call (`Toolkit.Provided`), so a tool can create, submit to and wait on conversations without the Harness in its layer.

## 0.8.1

### Patch Changes

- b63aa82: Watches over `HarnessTransport.layerWebSocket` open again as soon as a dropped connection is back, instead of waiting out a backoff of up to 5 seconds. The transport now also provides `HarnessTransport.Connections`, the count of its connections.

## 0.8.0

### Minor Changes

- 723f573: Support browser OAuth in the account registry, including the native Claude subscription adapter, authorization events, redacted completion submissions, and explicit cancellation over the service and RPC APIs. Sign-in credentials are staged and committed only after success; account reservations, stale-target checks, and removal fencing isolate concurrent account flows.

  Add optional provider-asserted `Credential.Identity` and a separate sign-in `revision` to OAuth credentials and account status. Native Codex and Claude sign-ins generate a new revision; refresh preserves identity/revision and rejects known account changes. Claude retains account UUID/email metadata when supplied instead of generating a random `extra.identity`. Existing serialized credentials without the new fields remain readable.

  Migration: custom OAuth adapters implement `login(account, flow)` and use `flow.onCode`, or `flow.onAuthorize` plus `flow.completion`. Sign-in consumers must handle the new `started` and `authorization` events and attempt IDs. Do not use `revision` as proof of account identity. Callback listeners, active-account policy, conversation bindings, and cross-process storage coordination remain host responsibilities.

## 0.7.0

### Minor Changes

- 447110d: Reorganize `effect-harness` into a single package with concept-oriented public subpaths and separate client/runtime APIs.

  - Replace the previous uppercase and `provider-*` imports with lowercase subpaths such as `effect-harness/client`, `effect-harness/harness`, `effect-harness/model`, and `effect-harness/tool`. The root export groups the portable core namespaces.
  - Expose `HarnessServer` and `HarnessClient` for composing the runtime and accessing it through memory or WebSocket RPC transports.
  - Reserve `$`-prefixed identifiers for built-in entry, document, and task kinds and tool-output discriminants. Custom input-entry kinds must not start with `$`.
  - Make provider and platform integrations optional peer dependencies, and include source files alongside compiled ESM and declarations so published source maps resolve.

  This is a breaking pre-1.0 release. Applications must update imports and composition to the new APIs.
