# effect-harness/provider-anthropic

Anthropic models for native Effect AI and Effect Harness.

Use an API key for direct inference. The adapter supplies native LanguageModels and harness catalogue descriptors, preserving structured Prompt history and translating harness tool content at the provider boundary.

## Install

```sh
npm install effect-harness effect
```

Use the equivalent `pnpm add`, `yarn add` or `bun add` command if you prefer.

## Provide a native model

Read credentials and the model ID through your ConfigProvider, then supply a native HTTP client:

```ts
import * as AnthropicLanguageModel from 'effect-harness/provider-anthropic/AnthropicLanguageModel'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

export const Model = AnthropicLanguageModel.layerApiKeyConfig({
  apiKey: Config.Redacted('ANTHROPIC_API_KEY'),
  model: Config.String('ANTHROPIC_MODEL'),
}).pipe(Layer.provide(FetchHttpClient.layer))
```

Provide `Model` to an Effect using the native LanguageModel service. Select a model ID available to your account through `ANTHROPIC_MODEL`; no provider model name is hard-coded in the Layer.

For harness execution, use `Catalog.layerApiKeyConfig` instead. Declare model IDs, context/output limits and capabilities, then provide that catalogue to the Executor used by Harness. The [provider guide](../providers.md) includes the complete catalogue configuration and conversation model selection.

## Find the right API

| Modules                  | Purpose                                                |
| ------------------------ | ------------------------------------------------------ |
| `AnthropicClient`        | Native client service and transport constructors       |
| `AnthropicLanguageModel` | Native API-key model and client Layers                 |
| `AnthropicTool`          | Native Anthropic tool constructors and schemas         |
| `Catalog`                | Harness model declarations, limits and capabilities    |
| `Prompt`, `ToolResult`   | Prompt projection and canonical tool-media translation |

## Continue

[Connect a provider](../providers.md) for catalogue integration, [provider reference](../reference/packages.md#providers) for service dependencies, and [tool media](../providers.md#tool-media-and-native-validation) for content constraints. See [Effect compatibility](../reference/compatibility.md). License attribution is retained in [NOTICE](../../packages/effect-harness/NOTICE).
