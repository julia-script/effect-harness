# effect-harness/provider-openai

OpenAI models for native Effect AI and Effect Harness.

Use an API key for direct inference, or compose explicit ChatGPT account authorization. The adapter supplies native LanguageModels and harness catalogue descriptors, preserving structured Prompt history and translating harness tool content at the provider boundary.

## Install

```sh
npm install effect-harness effect
```

Use the equivalent `pnpm add`, `yarn add` or `bun add` command if you prefer.

## Provide a native model

Read credentials and the model ID through your ConfigProvider, then supply a native HTTP client:

```ts
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

export const Model = OpenAiLanguageModel.layerApiKeyConfig({
  apiKey: Config.Redacted('OPENAI_API_KEY'),
  model: Config.String('OPENAI_MODEL'),
}).pipe(Layer.provide(FetchHttpClient.layer))
```

Provide `Model` to an Effect using the native LanguageModel service. Select a model ID available to your account through `OPENAI_MODEL`; no provider model name is hard-coded in the Layer.

For harness execution, use `Catalog.layerApiKeyConfig` instead. Declare model IDs, context/output limits and capabilities, then provide that catalogue to the Executor. The [provider guide](../providers.md) includes the complete catalogue configuration and conversation model selection.

## Account authorization

`ChatGpt` handles consent, callback validation and credential refresh. `ChatGptClient` and `ChatGptLanguageModel` provide authorized inference; `Catalog.layerChatGpt` provides harness descriptors. Account inference preserves structured history and disables remote response storage.

The host owns the consent UI and callback delivery. Authorization and inference permissions are checked at runtime. Follow [account sign-in](../how-to/account-sign-in.md#chatgpt-begin-and-complete-consent) to compose the required services.

## Find the right API

| Modules                                 | Purpose                                              |
| --------------------------------------- | ---------------------------------------------------- |
| `OpenAiLanguageModel`                   | Native API-key model and client Layers               |
| `Catalog`                               | Harness model declarations, limits and capabilities  |
| `ChatGpt`, `Callback`                   | Account authorization and loopback callback handling |
| `ChatGptClient`, `ChatGptLanguageModel` | Authorized account transport and native model        |
| `LanguageModel`                         | Forwarding exports for model construction            |

## Continue

[Connect a provider](../providers.md) for catalogue integration, [provider reference](../reference/packages.md#providers) for service dependencies, and [tool media](../providers.md#tool-media-and-native-validation) for content constraints. See [Effect compatibility](../reference/compatibility.md).
