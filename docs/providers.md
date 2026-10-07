# Choose and authorize a provider

Provider packages supply native Effect AI LanguageModels and harness Model.Catalog Layers. The host supplies HTTP, Crypto, credential storage and optional callback servers through ordinary Effect Layers. Model IDs, context/output limits, supported thinking/caching and prices come from caller-declared catalogue entries. Prices are USD per million tokens. Missing usage fields or price information remain unknown rather than silently claiming a reliable total.

| Transport                       | LanguageModel boundary                                   | Harness catalogue                                                      |
| ------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------- |
| OpenAI API key                  | `provider-openai/OpenAiLanguageModel.layerApiKey`        | `provider-openai/Catalog.layerApiKey`                                  |
| ChatGPT account                 | `provider-openai/ChatGptLanguageModel.layer`             | `provider-openai/Catalog.layerChatGpt`                                 |
| Anthropic API key               | `provider-anthropic/AnthropicLanguageModel.layerApiKey`  | `provider-anthropic/Catalog.layerApiKey`                               |
| Pi-compatible Anthropic account | `provider-anthropic/AnthropicAccountLanguageModel.layer` | `provider-anthropic/Catalog.layer` with `AnthropicAccountClient.layer` |
| Installed Claude CLI            | `provider-claude-code/ClaudeCodeLanguageModel.layer`     | `provider-claude-code/Catalog.layer`                                   |

The [offline example](../apps/example/src/main.ts) supplies a deterministic native LanguageModel directly. Replace its descriptor catalogue with your provider catalogue, preserve its generic Executor/Registry composition, and supply the provider's required services. API keys are Redacted values. Do not print credential values or authorization URLs in general application logs.

## ChatGPT consent and refresh

Provide `auth/CredentialStore`, `auth/Jwt`, native HttpClient and Crypto to `ChatGpt.layer({ appName })`. The app name must identify the actual application. Begin sign-in only after an explicit user action. `begin({ redirectUri, account? })` returns URL/state/expiry; the host opens the URL and returns the full callback URL to `complete`. Completion validates state, PKCE, redirect, OIDC claims and direct-inference scope. The stored identity key derives from verified issuer/client/subject, not email.

For a local browser flow, provide a scoped HttpServer bound to IPv4 `127.0.0.1` to `Callback.layer`. The listener exposes `authorization` and `await`; it is installed before the host opens the URL and is cancelled with its scope. Alternatively, the host can deliver a validated callback through its own UI boundary. `accessToken(account)` refreshes when needed under credential-store serialization; `refresh(account, { force: true })` requests an explicit refresh. Sign-out revokes the refresh token and retains the dynamic client registration for future sign-in.

Authorized account inference uses the public streaming Responses endpoint with storage disabled and validates completed responses. `models(account)` discovers visible models; catalogue entries still need declared limits/capabilities. Account entitlement and current server availability are runtime checks, not guarantees provided by these docs.

## Anthropic consent and refresh

Provide CredentialStore, HttpClient and Crypto to `OAuth.layer`. This implements the pinned Pi-compatible authorization protocol. On user-initiated sign-in, `begin({ account, method })` selects `browser` or `copyCode`. The host opens the Redacted URL and sends the callback URL or copied code to `complete(state, input)`. State itself is Redacted because the protocol uses its PKCE verifier as state. Completion requires the granted inference scope. The caller chooses the account key; the resulting OpaqueOAuth credential does not assert a verified OIDC identity.

`OAuth.layerCallback({ account })` handles the browser flow with a caller-supplied scoped HttpServer at the expected loopback address/port. Copy-code mode needs no local server. `accessToken` and `refresh` serialize token refresh/rotation; `cancel` closes a pending attempt and `signOut` removes the stored account. `AnthropicAccountClient.layer` authenticates native Anthropic Messages requests, preserves full structured Prompt history and adapts protocol identity/tool names at the boundary. It does not read the installed Claude CLI's credentials.

## Optional installed CLI

Use an already installed and independently signed-in Claude CLI only when that is the integration you intend. `Cli.layer` consumes a native ChildProcessSpawner. Requests require explicit `policyTrust: 'trusted-installed-cli'`: the host is responsible for auditing the executable and its managed policy. The adapter clears alternate provider credentials, restricts built-in tool execution and routes tool intents to the framework's scoped MCP server. Supply `IntentServer.layer` with a loopback HttpServer for tools, or `layerDisabled` without tools.

The default history mode rejects history it cannot faithfully import. Choosing `historyMode: 'transcript'` renders prior canonical messages as input data; it does not resume the CLI's own session. Unsupported files/options/history features produce typed errors. Prefer the direct Anthropic transport when full canonical multi-turn Prompt behavior is required.

These account flows are implemented and exercised with protocol fixtures. Live sign-in, account entitlements, paid inference and an installed CLI were not run by the offline example. Credential stores are application-owned; providers never silently import another application's credential files.

## Tool media and native validation

The generic `harness/ToolResult.encode` stores a validated `@effect-harness/ToolContent` envelope in a native Prompt tool-result value. It carries ordered native user-message parts and rendered diagnostics; private details, controls and usage stay outside the model-facing envelope. Ordinary JSON tool results remain ordinary JSON. Anthropic and OpenAI adapters recognize the envelope at their captured native client boundary and expand it within the original tool-result item, preserving call identity and mixed text/media order. They do not add a new user turn.

Anthropic maps native image/PDF/text-document inputs to Messages content blocks, retaining applicable cache, title, context and citation options and the required document beta. OpenAI maps to Responses text/image/file inputs, including image detail, file IDs, data and URL sources, and PDF filenames. Validated envelopes with unsupported media or invalid mapped provider options fail with typed native AI errors; values that do not decode as an envelope retain ordinary JSON behavior. Both API-key and account transports use the same conversion. [Anthropic HTTP fixtures](../packages/provider-anthropic/test/ToolResult.test.ts) and [OpenAI HTTP fixtures](../packages/provider-openai/test/ToolResult.test.ts) inspect actual native requests; [durable regression tests](../packages/durable/test/workflow/GenerationParity.integration.test.ts) cover committed result-to-context projection. These tests do not establish that every provider/model accepts every media kind.

Undeclared tool-call preservation separately requires the [pinned Effect AI patch](parity.md#required-effect-ai-patch). Native SDK validation remains the default; the opt-in is for callers that own unavailable-tool settlement. The optional installed CLI has its own documented media/history limits and is not equivalent to either direct transport.
