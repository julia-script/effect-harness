# Subscription providers

`provider-openai-codex` provides ChatGPT OAuth and a native Effect `LanguageModel` over the Codex Responses SSE endpoint. `provider-anthropic` adds explicit Claude OAuth constructors alongside the existing API-key constructors. The harness continues to own tool execution, permissions, conversation history, retries, and recovery.

These are direct model transports. They do not run either vendor's CLI, agent runtime, sandbox, or filesystem tools. Claude OAuth uses the pinned pi protocol, including its Claude Code wire preamble, headers, and canonical tool casing. This integration is unofficial and selected through the OAuth constructors. It is not evidence of account access, permitted usage, or subscription billing. Anthropic's [current guidance](https://support.claude.com/en/articles/13189465-log-in-to-your-claude-account) distinguishes third-party usage credits from subscription limits and prohibits misrepresented client identity. Prefer the existing API-key integration for supported third-party applications. No live account verification was performed for these adapters.

## Choose an integration boundary

Use `CodexLanguageModel.layer` or `AnthropicLanguageModel.layerOAuth` for a native Effect AI service. Use `provider-openai-codex/Model.make` or `provider-anthropic/OAuthModel.make` for durable harness execution: these constructors also install the configuration codec that freezes accepted settings and pins the account binding. Pass the returned descriptor to `HarnessRuntime.make({ models: [model] })` or the corresponding harness Layer.

```ts
import * as Codex from 'effect-harness/provider-openai-codex/Model'

const model =
  yield *
  Codex.make({
    model: 'gpt-5.4',
    binding: selectedCredentialMetadata.binding,
    config: { verbosity: 'low' },
    definition: {
      ref: { provider: 'openai-codex', modelId: 'gpt-5.4' },
      capabilities: {
        tools: true,
        images: false,
        reasoning: true,
        structuredOutput: false,
      },
    },
  })
```

The host supplies `HttpClient` and `CredentialStore`. Model names and capability declarations do not grant remote access. For images, declare the model's image limits using the existing harness definition. No new provider attaches `Model.Deferred`.

## Login and credential storage

The reusable service contract is documented in [provider credentials](provider-auth.md).

`provider-auth/CredentialStore.layerMemory` is explicitly temporary. `provider-auth-node/CredentialStore.layer({ path })` is an optional Node/Bun store that writes a caller-selected file. Portable provider imports neither load Node modules nor inspect a home directory. Hosts can implement `CredentialStore.Service` with their own database or keychain.

OAuth credentials hold `Redacted` access and refresh tokens, an expiry in **Unix milliseconds**, and a binding `{ provider, profile, identity }`. Each login creates a new random identity. Codex extracts and validates its account routing identifier from token claims; decoding those claims does not independently verify the token's signature. `store.list` exposes only binding, credential type, and expiry.

The file store uses mode 0600, atomic replacement with file/directory synchronization, and an exclusive lock file shared across processes. Use an application-owned private directory on a local filesystem. This store is plaintext, not a keychain. A process crash can leave a `.lock` file; lock acquisition times out rather than stealing a possibly live lock. Remove a stale lock only after establishing that its owner has stopped. Filesystem I/O requires a functioning local filesystem; a filesystem that indefinitely stalls system calls cannot provide bounded shutdown.

Custom stores must serialize the entire reread/refresh/commit sequence across every process that shares the credentials. All writers, including logout and account replacement, use `modify`. Reads must observe committed data; a failed callback must not publish a new credential. The file store reports `StoreFailure` before publication and `StoreCommitUncertain` if publication occurred but subsequent durability or lock cleanup failed. On an uncertain commit, reread the store before deciding whether to retry or log in again; do not assume the old refresh token remains valid. Callback/commit work is interruption-masked and must have bounded I/O. Do not place this store in Session documents or checkpoints.

Explicit operations:

- Codex: `OAuth.loginBrowser` or `OAuth.loginDevice`.
- Claude: `OAuth.loginBrowser` or `OAuth.loginCopyCode`.
- Status: `CredentialStore.list` on the acquired service.
- Local logout: `CredentialStore.logout({ provider, profile })`.
- Import: `CredentialStore.save(credential)`, with a fresh identity for each replacement.

Each login takes `profile` and an application-owned `interaction`. The host displays/opens the authorization URL, supplies a completion Effect, and displays device instructions. A URL or `code#state` must match the attempt's state. Bare codes are rejected unless `allowBareCode: true`; bare-code mode relies on the single live PKCE transaction rather than independent state validation. There is no state-check bypass for completions that carry state. Login has a ten-minute default deadline; device login defaults to fifteen minutes and additionally respects the returned expiry and polling cadence. Pending, slow-down, denied, expired, and malformed responses have distinct outcomes.

Providing `provider-auth-node/CallbackServer.layer` enables scoped loopback callbacks. Codex uses port 1455 and falls back to manual completion if it is unavailable. Claude prefers 53692, then an available port, then manual completion. Callback and manual completion race; the losing branch is interrupted and the listener closes on success, failure, timeout, or cancellation. Copy-code and device login require no listener. Generation never prompts or opens a browser.

## Refresh and account changes

New subscription constructors require an explicit binding and resolve only its stored OAuth credential. They perform no ambient API-key lookup and never fall back to a different account or billing mode. Existing API-key constructors retain their ConfigProvider behavior, including `ANTHROPIC_API_KEY`. The shared credential schema also represents API keys; subscription constructors reject that credential mode.

Resolution checks an injected Clock, acquires the store lock when refresh is needed, rereads, and checks again. The default minimum validity is five minutes; zero means just-in-time expiry checking. A bounded refresh worker defaults to fifteen seconds. Once rotation begins, caller interruption does not discard the returned replacement: the transaction waits for the worker and persists before unlocking. Logout and account replacement use the same lock. A crash between remote rotation and local commit can still require re-login; this is not a distributed exactly-once guarantee.

`automaticRefresh: false` selects external-refresh mode. Unexpired credentials work; expired credentials fail. This does not make expired tokens valid. Auth failures are sanitized and mapped to native `AiError`: missing, expired, invalid-grant, account mismatch, and unsupported modes are terminal; transport/refresh-timeout failures retain retryable classification. There is no reactive 401 replay and no retry inside inference adapters. The harness owns inference retry scheduling. A stream that already emitted output is never invisibly restarted by auth middleware.

Logout fences future resolution but cannot retract a remotely accepted request. Cancel the active harness operation separately if needed. Every login/replacement invalidates old prepared account bindings, even when the profile name is reused. Recovery requires the original binding or explicit reauthorization and repreparation. Refresh preserves identity and affinity.

## Configuration

Conversation options and the provider's own `Config` service accept only schema-backed generation fields. Claude freezes the normalized `apiVersion` in its durable protocol binding. Changing that version requires explicit repreparation. Snapshots record and validate the selected protocol, model, account identity, and accepted settings. Native provider configuration overrides are rejected by the OAuth adapters; use their own Config tags. API-key native options remain unchanged. Endpoint overrides and live clients belong to host construction, never serializable request options. HTTP deadlines, proxy routing, redirect policy, and diagnostic middleware belong to the injected HttpClient. No diagnostic payload hooks or implicit retries are installed.

Codex settings:

- `reasoningEffort`: `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`, only when explicitly included in the host's selected-model `reasoningEfforts` declaration.
- `reasoningSummary`: `auto`, `concise`, or `detailed`. Opaque encrypted reasoning remains in history independently of visible summary text.
- `verbosity`: `low` (default), `medium`, or `high`.
- `serviceTier`: `auto`, `default`, `flex`, or `priority`; this requests a tier without promising entitlement.
- `temperature`: 0–2, only with `supportsTemperature: true` for the selected model.
- `cache`: `affinity` (default) or `none`; `cacheKey` overrides the derived affinity. `none` suppresses both body and session headers. An explicit empty key suppresses session headers and remains an empty body key. No cache TTL is promised.

Codex always sets `store: false`, `stream: true`, `parallel_tool_calls: true`, and includes encrypted reasoning. Completion collects the terminal response from SSE; streaming emits native incremental parts. `parallel_tool_calls` does not change local scheduling. Named forced tool choice, output-token limits, arbitrary sampling options, structured final output, server-side tools, WebSocket/auto mode, compression, and deferred/background operations are unsupported. The private native compatibility client rejects embeddings; it is not exposed as a Codex capability.

Claude OAuth settings:

- `maxTokens`: positive integer, default 4096.
- `thinking`: disabled, enabled with a budget of at least 1024 and smaller than `maxTokens`, or adaptive. The host must declare supported `thinkingModes`.
- `effort`: `low`, `medium`, `high`, `xhigh`, or `max`, only for adaptive thinking and declared host `efforts`.
- `thinkingDisplay`: `summarized` or `omitted`, only when thinking is enabled. Native continuity signatures are preserved.
- `temperature`: 0–1, only with `supportsTemperature: true`; rejected when combined with enabled/adaptive thinking.
- `cache`: omitted preserves existing block controls, `none` strips every cache directive, `short` requests ephemeral caching, and `long` requests one hour only with `supportsLongCache: true`.
- `userId`: explicit attribution. Affinity/account identifiers are never substituted for this value.

The OAuth adapter adds its protocol preamble before application system blocks, protected bearer/beta/version headers, and reversible casing for the canonical pi tool names. Collisions fail before dispatch. Names in declarations, named choice, history, and returned calls are translated; harness registry/replay names remain canonical application names. Signed/redacted thinking, tool-result images/files, cache counters, and native argument assembly use Effect's provider translation. Structured final output, native provider tools, interleaved-thinking beta selection, fallback models, workload federation, arbitrary bearer gateways, and private Claude Code credential-file imports are not added by this adapter.

## Runnable examples

[SubscriptionProviders.ts](../apps/example/src/SubscriptionProviders.ts) is typechecked and uses an application-owned file store and a harmless `countCharacters` tool. Provide an explicit credential path and model:

```sh
bun apps/example/src/SubscriptionProviders.ts login-codex-browser ./private/auth.json
bun apps/example/src/SubscriptionProviders.ts login-codex-device ./private/auth.json
bun apps/example/src/SubscriptionProviders.ts login-claude-browser ./private/auth.json
bun apps/example/src/SubscriptionProviders.ts login-claude-copy ./private/auth.json
MODEL=gpt-5.4 bun apps/example/src/SubscriptionProviders.ts run-codex ./private/auth.json
MODEL=claude-sonnet-4-6 bun apps/example/src/SubscriptionProviders.ts run-claude ./private/auth.json
MODEL=claude-sonnet-4-6 bun apps/example/src/SubscriptionProviders.ts claude-api-key ./private/auth.json
```

The API-key command reads `ANTHROPIC_API_KEY`. The example displays URLs and supports manual completion rather than launching a browser itself. It prints the submission ID before waiting. After process loss, reopen the same files and model using `recover-codex` or `recover-claude` with that ID as the fourth argument. An expired stored credential refreshes on the recovered network attempt. For a deterministic offline proof of restart before I/O, run:

```sh
bunx vitest run packages/effect-harness/test/SubscriptionRecovery.test.ts
```

## Delivery and verification checklist

| Capability / delivery slice                                            | Codex                                         | Claude OAuth                                   | Claude API key                   |
| ---------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------- | -------------------------------- |
| Shared schema, redaction, memory/persistent stores, serialized refresh | Implemented                                   | Implemented                                    | Existing explicit keys preserved |
| Browser/manual login                                                   | Implemented                                   | Implemented                                    | Not applicable                   |
| Headless login                                                         | Device flow implemented                       | Copy-code implemented                          | Existing configuration           |
| Completion, text, application tools, incremental SSE                   | Implemented, fixture verified                 | Implemented, fixture verified                  | Regression suite retained        |
| Image/file tool results and original tool bindings                     | Implemented, full harness tool-round fixture  | Implemented, full harness tool-round fixture   | Existing translation preserved   |
| Reasoning continuity                                                   | Native encrypted content                      | Native signed/redacted thinking                | Existing native behavior         |
| Structured final output                                                | Unsupported by this adapter                   | Unsupported by OAuth adapter                   | Existing native behavior         |
| Prompt caching and explicit disable                                    | Affinity/body/header mapping implemented      | Block controls implemented                     | Existing native controls         |
| Durable preparation/reopen and account fencing                         | Implemented, cold JSONL/expired-token fixture | Shared account-bound codec implemented         | Existing behavior                |
| Usage                                                                  | Native counters, no invented zero cost        | Native cache/token counters                    | Existing pricing semantics       |
| WebSocket reuse, continuation, automatic transport fallback (slice 4)  | Deferred                                      | Not introduced                                 | Not introduced                   |
| Remote deferred/background jobs                                        | Unsupported                                   | Unsupported                                    | Not added                        |
| Embeddings/image/audio generation/batch endpoints                      | Unsupported                                   | Not exposed by OAuth Layer                     | Existing client unchanged        |
| Filesystem tools, permissions, approvals, sandbox                      | Harness/host owned                            | Harness/host owned                             | Harness/host owned               |
| Model/account entitlement and production protocol compatibility        | Requires live verification                    | Requires live verification; OAuth caveat above | Existing integration             |

The minimum code slices are shared auth, Codex SSE, Claude OAuth, and recovery/package/examples. Tests use injected HttpClient, Clock and stores and do not require accounts. They cover PKCE/state, device pending cadence, token-response redaction, refresh interruption/timeout/serialization, durable file reload, lock contention, callback cleanup, fragmented streams, failures, collision detection, actual harness tool/media rounds, and prepared-request restoration. They do not certify vendor account access or all possible model settings. Closing local I/O does not prove remote cancellation or absence of charges.

## Research basis

Implementation was checked against the installed Effect 4.0.1 sources and the read-only vendored Effect examples. Pi protocol reference: [`b45e0eb`](https://github.com/earendil-works/pi/tree/b45e0ebcc5ae2fc42a6f2f23104d10e656e6bedb/packages/ai/src), particularly OAuth browser/device helpers, `auth/resolve.ts`, `api/openai-codex-responses.ts`, and `api/anthropic-messages.ts`. Current upstream [Codex login source](https://github.com/openai/codex/blob/main/codex-rs/login/src/server.rs) was also inspected; its newer callback-port choices were not silently substituted for the pinned pi protocol. Adapted protocol constants and casing retain attribution in the package NOTICE.

## Retained Claude thinking

The shared `provider-anthropic/ThinkingPolicy` policy applies to every request, including after compaction, tool declaration changes, and restart. For the verified Fable 5.1, Opus 5.5, Sonnet 5.5 and Haiku 5.5 families (including dated IDs), the adapter requires declared adaptive thinking and sends `thinking.block_binding.prefix_mismatch_behavior: "drop_block"` with `thinking-binding-controls-2026-08-01`. The server can discard mismatched old blocks and use newly valid reasoning. Stored blocks, signatures, redacted thinking and tool-call/result groups remain intact. The policy does not claim preservation of the remote prompt cache after a prefix change.

Verified older Claude 4 families, Opus/Sonnet 5 and Mythos 5.1 preserve thinking without adding that beta. Unknown models reject enabled thinking or thinking replay until their policy is verified. The policy and supported configuration capabilities are part of the durable configuration binding; restoring a preparation with different capabilities fails. OAuth authentication alone never enables a thinking beta. See Anthropic's [preserved-thinking contract](https://platform.claude.com/docs/en/build-with-claude/preserved-thinking).
