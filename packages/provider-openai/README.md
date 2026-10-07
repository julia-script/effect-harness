# @effect-harness/provider-openai

Native Effect OpenAI LanguageModels and harness catalogue descriptors for API-key and explicit ChatGPT account authorization.

```sh
bun add @effect-harness/provider-openai effect@4.0.1
```

Public subpaths include `OpenAiLanguageModel`, `ChatGptLanguageModel`, `ChatGptClient`, `LanguageModel`, `Catalog`, `ChatGpt` and `Callback`.

OpenAiLanguageModel.layerApiKey accepts a Redacted key and consumes HttpClient. Catalog.layerApiKey additionally accepts declared models and limits. ChatGPT inference uses the authorized account, preserves structured Prompt history and disables remote response storage. The host owns consent and callback delivery; authorization and inference permissions are validated at runtime.

Follow [provider integration](../../docs/providers.md) or [account sign-in](../../docs/how-to/account-sign-in.md). See [provider services](../../docs/reference/packages.md#providers), [tool media](../../docs/providers.md#tool-media-and-native-validation), and [Effect compatibility](../../docs/reference/compatibility.md).
