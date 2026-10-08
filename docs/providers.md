# How to connect a model provider

Use this guide to replace a local model with OpenAI or Anthropic while keeping the harness Executor, Registry and Persistence composition. You need an API key, a model ID available to that account and that model's context/output limits.

## Install Effect Harness

OpenAI and Anthropic adapters are included in the same package:

```sh
npm install effect-harness effect
```

Each adapter builds native Effect AI LanguageModels. A harness application uses its `Catalog` Layer to select those models by provider/model reference. An application making direct native LanguageModel calls can use the LanguageModel Layer instead; see [provider services](reference/packages.md#providers).

## Build an OpenAI catalogue

Supply the API key and the limits through your ConfigProvider. This example declares one model and uses the native fetch HTTP client:

```ts
import * as Catalog from 'effect-harness/provider-openai/Catalog'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

export const Models = Catalog.layerApiKeyConfig({
  provider: Config.succeed('openai'),
  apiKey: Config.Redacted('OPENAI_API_KEY'),
  models: Config.all({
    modelId: Config.String('OPENAI_MODEL'),
    contextWindow: Config.Int('MODEL_CONTEXT_WINDOW'),
    maxOutputTokens: Config.Int('MODEL_MAX_OUTPUT_TOKENS'),
  }).pipe(Config.map((model) => [model])),
}).pipe(Layer.provide(FetchHttpClient.layer))
```

Set all four variables through your application's ConfigProvider. Context and output limits must be positive, and the output limit cannot exceed the context window. Use values for the selected model rather than copying another model's advertised limits.

Provide `Models` to `effect-harness/Executor.layer`, then provide that Executor to `Harness.layer`. Reuse the same Layer values so descriptors and requests share the captured client.

## Build an Anthropic catalogue

The equivalent Anthropic Layer is:

```ts
import * as Catalog from 'effect-harness/provider-anthropic/Catalog'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'

export const Models = Catalog.layerApiKeyConfig({
  provider: Config.succeed('anthropic'),
  apiKey: Config.Redacted('ANTHROPIC_API_KEY'),
  models: Config.all({
    modelId: Config.String('ANTHROPIC_MODEL'),
    contextWindow: Config.Int('MODEL_CONTEXT_WINDOW'),
    maxOutputTokens: Config.Int('MODEL_MAX_OUTPUT_TOKENS'),
  }).pipe(Config.map((model) => [model])),
}).pipe(Layer.provide(FetchHttpClient.layer))
```

Declare thinking, caching and request-option capabilities in the catalogue entry when enabling those features. The adapter validates requested options against that declaration; [configuration reference](reference/configuration.md#model-configuration) describes the boundary.

## Select the model for a conversation

Configure the conversation with the matching provider/model reference:

```ts
import * as Conversation from 'effect-harness/Conversation'
import type * as Agent from 'effect-harness/Agent'

export const selectModel = (conversation: Conversation.Conversation, model: Agent.ModelRef) =>
  Conversation.configure(conversation, { model })
```

For the OpenAI catalogue, use provider `openai` and the model ID from `OPENAI_MODEL`. For Anthropic, use `anthropic` and your declared model ID. Successful catalogue construction validates local configuration; inference still checks remote credentials and availability.

You can now submit messages through the conversation API.

## Tool media and native validation

For mixed text/media results, bind a tool projector that returns native Prompt parts in ToolResult.content. Keep private details and controls in their separate fields. Select a provider/model that accepts those parts and handle typed AI failures for unsupported content. The [tool-media reference](reference/packages.md#tool-media-and-native-validation) lists the translation and validation boundaries.

See [native response validation](reference/compatibility.md#model-response-validation) for failed-response recording and retry behavior.
