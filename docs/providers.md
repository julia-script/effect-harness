# Models and providers

The runtime consumes Effect AI's native `LanguageModel` service. A provider can call an API directly or wrap an SDK, provided it implements that interface and returns model responses with tool-call intents. The harness owns tool execution, so providers must not execute those tools themselves.

For a single model, supply its Layer to `Harness.layerLocal`. The [offline provider](../apps/example/src/DemoModel.ts) demonstrates `LanguageModel.make` without credentials.

The optional `provider-openai/OpenAiLanguageModel` and `provider-anthropic/AnthropicLanguageModel` adapters expose native constructors and Layer composition. For example:

```ts
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'

const ModelLive = OpenAiLanguageModel.layerApiKeyConfig({
  apiKey: Config.redacted('OPENAI_API_KEY'),
  model: Config.string('OPENAI_MODEL'),
}).pipe(Layer.provide(FetchHttpClient.layer))
```

For multiple models, construct `Model.make` descriptors from already acquired language-model instances. Each descriptor has a schema-backed definition (provider/model reference, declared capabilities, optional limits), an options schema, and `configure` to map decoded options into provider request services. Pass descriptors in `Harness.layerLocal({ models })`. Configure a conversation's model reference and options through `Conversation.configure`.

Descriptors retain the provider's already acquired resources. The provider Layer owns those resources; model limits and capabilities are caller declarations. There is no dynamic catalog service in the harness.
