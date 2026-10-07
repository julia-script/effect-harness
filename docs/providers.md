# How to connect a model provider

Use this guide to replace a local model with OpenAI or Anthropic while keeping the harness Executor, Registry and durable Workflow composition. You need an API key, a model ID available to that account and that model's context/output limits.

## Install the provider adapter

For OpenAI:

```sh
bun add @effect-harness/provider-openai effect@4.0.1
```

For Anthropic:

```sh
bun add @effect-harness/provider-anthropic effect@4.0.1
```

Each adapter builds native Effect AI LanguageModels. A harness application uses its `Catalog` Layer to select those models by provider/model reference. An application making direct native LanguageModel calls can use the LanguageModel Layer instead; see [provider services](reference/packages.md#providers).

## Build an OpenAI catalogue

Supply the API key and the limits through your ConfigProvider. This example declares one model and uses the native fetch HTTP client:

```ts
import * as Catalog from '@effect-harness/provider-openai/Catalog'
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

Provide `Models` to `harness/Executor.layer` and to the durable executor registration graph. Reuse the same Layer value so descriptors and requests share the captured client.

## Build an Anthropic catalogue

The equivalent Anthropic Layer is:

```ts
import * as Catalog from '@effect-harness/provider-anthropic/Catalog'
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

Commit the matching provider/model reference to the conversation's agent document. This helper updates the root conversation:

```ts
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Session from '@effect-harness/durable/Session'
import type * as Agent from '@effect-harness/harness/Agent'
import * as Effect from 'effect/Effect'

export const selectModel = Effect.fn('selectModel')(function* (model: Agent.ModelRef) {
  const session = yield* Session.Session
  const root = yield* session.root()
  yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
      agent.model = model
    }),
  )
})
```

For the OpenAI catalogue, use provider `openai` and the model ID from `OPENAI_MODEL`. For Anthropic, use `anthropic` and your declared model ID. Successful catalogue construction validates local configuration; inference still checks remote credentials and availability.

You can now submit messages through `Submission.execute`. If the caller should authorize an account instead of supplying an API key, follow [add account sign-in](how-to/account-sign-in.md).

## Tool media and native validation

For mixed text/media results, bind a tool projector that returns native Prompt parts in ToolResult.content. Keep private details and controls in their separate fields. Select a provider/model that accepts those parts and handle typed AI failures for unsupported content. The [tool-media reference](reference/packages.md#tool-media-and-native-validation) lists the translation and validation boundaries.

Unknown tool-call settlement has an additional [Effect compatibility requirement](reference/compatibility.md). The installed Claude Code adapter has different [history and media constraints](reference/packages.md#installed-claude-code).
