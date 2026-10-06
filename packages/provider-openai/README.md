# @effect-harness/provider-openai

Native Effect OpenAI LanguageModels and harness catalogue descriptors. Use public `/LanguageModel`, `/Catalog`, `/ChatGpt` and `/Callback` subpaths.

`LanguageModel.layerApiKey({ apiKey, model, config?, apiUrl? })` accepts a Redacted key and consumes a native HttpClient. `Catalog.layerApiKey` additionally takes declared models, limits/capabilities and optional USD prices per million tokens, producing `harness/Model.Catalog` for the generic Executor.

ChatGPT account authorization is explicit: construct `ChatGpt.layer({ appName })` with CredentialStore, Jwt, HttpClient and Crypto. The actual application name identifies its dynamic client registration. `begin({ redirectUri, account? })` returns an authorization URL; the host displays/opens it after the user initiates sign-in. `complete(callbackUrl)` validates the callback, token identity and granted direct-inference permission. `accessToken` refreshes when necessary; `refresh(account, { force? })`, `models(account)`, `signOut(account)` and `cancel(state)` are available.

`Callback.layer({ account? })` consumes ChatGpt plus a caller-supplied scoped HttpServer bound to `127.0.0.1`. It exposes `authorization` and `await` after installing the listener. `LanguageModel.layerChatGpt` and `Catalog.layerChatGpt` use the authorized account through the public Responses endpoint, require fresh tokens and validate terminal response completion. They preserve full structured Prompt history, disable remote response storage and do not reuse private application session credentials.

See [providers.md](../../docs/providers.md). Live consent, model availability and inference are not exercised by the offline example; use the account's discovered models and permissions rather than treating this README as a provider entitlement guarantee.

Both transports expand validated harness tool-content envelopes into native Responses `function_call_output` content, retaining mixed text/image/file order and supported native part options. Ordinary JSON tool results retain native behavior. [HTTP fixtures](test/ToolResult.test.ts) verify actual requests; [tool media](../../docs/providers.md#tool-media-and-native-validation) describes the boundary. Harness unknown-call settlement additionally requires the checkout's [Effect AI patch](../../docs/parity.md#required-effect-ai-patch).
