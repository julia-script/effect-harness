# @effect-harness/provider-anthropic

Native Anthropic Messages LanguageModels and harness catalogue descriptors for API-key and explicit account authorization.

```sh
bun add @effect-harness/provider-anthropic effect@4.0.1
```

Public subpaths include `AnthropicLanguageModel`, `AnthropicAccountLanguageModel`, `AnthropicAccountClient`, `Anthropic`, `Account`, `Catalog`, `OAuth` and `Prompt`.

AnthropicLanguageModel.layerApiKey consumes HttpClient and a Redacted key. Catalog.layerApiKey adds declared model limits/capabilities. OAuth captures application-owned credentials and explicit consent; AnthropicAccountClient supplies the authorized native transport. Both direct transports preserve canonical Prompt history. They are separate from the installed Claude Code CLI boundary.

Follow [provider integration](../../docs/providers.md) or [account sign-in](../../docs/how-to/account-sign-in.md). See [provider services](../../docs/reference/packages.md#providers), [tool media](../../docs/providers.md#tool-media-and-native-validation), [Effect compatibility](../../docs/reference/compatibility.md), and [NOTICE](NOTICE).
