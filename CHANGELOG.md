# effect-harness

## 0.12.0

### Minor Changes

- cca5afc: Runs move to fallback models. A conversation's `fallbacks` (or `Settings.fallbacks`) take over for the rest of a run when its model is `Unavailable` (out of quota, or its credentials stopped working), unknown, out of retries, or asks to wait longer than `retry.maxDelayMs`. The run appends a `$fallback` entry and sets `$live.run.fallback`; the next run starts on the conversation's model again. Quota and authentication failures, and missing credentials, now fail with the new `Unavailable` reason instead of `Rejected`.

### Patch Changes

- 1a36085: Keep Claude thinking signatures. Anthropic sends a signature with a thinking delta and redacted thinking with the block's start, but only metadata on a part's end was kept, so Claude's thinking was never sent back, even to the same model.
- c146a95: Offer a tool whose parameters are `Schema.Struct({})` an empty object schema. Its JSON Schema had no `type`, and Anthropic refused every request that carried the tool.
- 1a36085: Replay the transcript for the model a request goes to, so a conversation can switch models. Other models' empty text blocks, provider item IDs, thought signatures and reasoning are left out (Claude thinking still goes to Claude on the same account), tool call IDs another provider refuses are rewritten with their results, and images become a note for text-only models. Adds `Transcript.replay`; stored messages are unchanged.

## 0.11.0

### Minor Changes

- 11b1af7: Breaking: the contract between a Harness and its clients moves to a new `effect-harness/protocol` subpath (and `Protocol` on the root import). Import `HarnessRpcs`, `Init`, `Submission`, `HarnessError`, `AgentState`, `ViewState` and `AgentEvent` from there. `effect-harness/client` now exports only `HarnessClient` and `HarnessTransport`, and `effect-harness/harness` no longer exports the five protocol modules it shared with the client. The `HarnessError` tags move from `effect-harness/harness/HarnessError/*` to `effect-harness/protocol/HarnessError/*`.

## 0.10.0

### Minor Changes

- 4105325: Rename the credentials service, module file, and credentials subpath namespace export to `CredentialStore`.

  Use tagged structs for credentials and their metadata: API keys use `_tag: 'ApiKey'`, OAuth credentials use `_tag: 'OAuth'`, identities use `_tag: 'Identity'`, and credential listings use `_tag: 'Info'` with a separate `type: 'ApiKey' | 'OAuth'` field.

  This changes the public API and serialized credential format. Existing `type: 'api_key'` / `type: 'oauth'` records are not migrated or decoded.

### Patch Changes

- 2279850: Use Effect's own helpers throughout: `Effect.fnUntraced` for functions that only run a generator, `Equal.equals` and `Predicate` guards instead of hand-rolled ones, and error reasons checked by their `_tag`.

## 0.9.2

### Patch Changes

- 7e47855: Anthropic OAuth models listed in `adaptiveModels` send tool schemas non-strict again. 0.9.1 moved the non-strict setting into the shared Anthropic config, and the adaptive-model path rebuilt the config without it, so those requests carried `strict: true` tools.

## 0.9.1

### Patch Changes

- 73a4fda: OpenAI, Codex and Anthropic API-key requests send tool schemas as non-strict. `@effect/ai-openai` and `@effect/ai-anthropic` default tools to strict mode, which refuses schemas with optional properties, so Codex rejected every request carrying the bundled tools with `Invalid schema for function 'read': ... additionalProperties must be false for a strict schema`. Anthropic OAuth already sent them non-strict.

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
