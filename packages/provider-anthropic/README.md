# @effect-harness/provider-anthropic

Native Anthropic Messages LanguageModels with API-key and explicit account transports. Public subpaths are `/Anthropic`, `/Catalog`, `/OAuth`, `/Account` and `/Prompt`.

`Anthropic.layer({ apiKey, model, config? })` consumes a native HttpClient. `Catalog.layerApiKey` produces the harness Model.Catalog from declared model capabilities, limits, thinking budgets/efforts, caching and optional USD prices per million tokens. `Catalog.layer` can instead consume an already supplied native AnthropicClient, including `Account.layerClient`.

`OAuth.layer` implements the account PKCE protocol and requires CredentialStore, HttpClient and Crypto. `begin({ account, method: 'browser' | 'copyCode' })` returns a Redacted authorization URL and state. The host obtains explicit user consent and passes the callback URL or copied code to `complete(state, input)`. State is secret because this protocol uses the PKCE verifier as state. `accessToken` refreshes as needed; explicit `refresh`, `cancel` and `signOut` operations are available. The record is OpaqueOAuth; the account key is host-selected, not a verified OIDC subject.

`OAuth.layerCallback({ account })` installs a scoped browser callback listener with a caller-supplied HttpServer at the protocol's expected loopback address/port. `Account.layerClient({ account })` consumes OAuth and HttpClient, refreshes tokens and adapts the native Messages client; `Account.layer({ account, model, config? })` provides the LanguageModel. Full canonical Prompt history is retained. Account request identity/tool-name adaptations occur at the transport boundary.

The protocol was adapted from the pinned Pi source under MIT; see `NOTICE`. Tests use HTTP fixtures, not a live Anthropic account. [providers.md](../../docs/providers.md) distinguishes this direct account transport from the optional installed CLI.

Both API-key and account transports expand validated harness tool-content envelopes within the original native `tool_result` block. Mixed text/image/PDF/text-document order and applicable native cache/title/context/citation options are retained. Ordinary JSON tool results retain native behavior. [HTTP fixtures](test/ToolResult.test.ts) verify actual requests; [tool media](../../docs/providers.md#tool-media-and-native-validation) describes supported boundaries. Harness unknown-call settlement additionally requires the checkout's [Effect AI patch](../../README.md).
