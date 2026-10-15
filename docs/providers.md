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
  apiKey: Config.Redacted('OPENAI_API_KEY'),
  model: Config.String('OPENAI_MODEL'),
}).pipe(Layer.provide(FetchHttpClient.layer))
```

For multiple models, construct `Model.make` descriptors from already acquired language-model instances. Each descriptor has a schema-backed definition (provider/model reference, declared capabilities, optional limits), an options schema, and `configure` to map decoded options into provider request services. Pass descriptors in `Harness.layerLocal({ models })`. Configure a conversation's model reference and options through `Conversation.configure`.

Descriptors retain the provider's already acquired resources. The provider Layer owns those resources; model limits and capabilities are caller declarations. The dynamic provider manager can construct these descriptors from user-configured instances.

For subscription authentication, see [subscription providers](subscription-providers.md): explicit Codex and Claude OAuth, credential persistence/refresh, durable account bindings, runnable examples, and the compatibility checklist. Existing API-key constructors retain their current behavior.

## Definitions and instances

`Provider.define` declares a public `config` codec, a `secrets` codec, a positive
integer `version`, independent `models.list(context, query)` and
`models.get(context, modelId)` Effects, and `createModel(context, resolved)`.
Register definitions once with `Providers.make({ definitions: { codex, claude, gateway } })`.
The registry keys are provider type IDs. User-created instance IDs are generated
separately, so labels never act as account keys.

The manager captures `ProviderStore` and `CredentialStore`. Its operations are:

| Operation                                                      | Behavior                                                                                                      |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `add({ provider, label, config, secrets? })`                   | Validate and encode configuration; allocate an instance and an optional credential reference.                 |
| `list` / `get(id)`                                             | Decode public instances using their registered definition. Never return secrets.                              |
| `update(id, { label?, config?, secrets?, expectedRevision? })` | Replace provided fields. Config is a complete replacement. Omitted secrets are retained; `null` removes them. |
| `remove(id)`                                                   | Remove the public instance, then its credential.                                                              |
| `models.list({ instanceId, cursor?, query?, limit? })`         | Fetch one summary page.                                                                                       |
| `models.get({ instanceId, modelId })`                          | Fetch metadata directly without listing first.                                                                |
| `models.resolve({ instanceId, modelId, options? })`            | Validate portable reasoning controls, then construct a model descriptor.                                      |

The returned instance ID carries its provider type for subsequent config/secret
inference. `get(string)` returns a discriminated union; narrow `instance.provider`
when reading an ID from a route or persisted selection. Definition-specific
errors and service requirements survive list/get/resolve. The manager supplies
its captured credential store; HTTP and custom discovery services remain explicit.

Schema codecs encode on write and decode on read. Invalid public config and secrets
fail without including submitted values in errors. Unknown definitions and mismatched
definition versions fail explicitly. Changing a schema incompatibly requires an
application-owned data migration and version change; the manager does not silently
reinterpret old records. Model option codecs also validate durable configuration
when encoding and restoring requests.

A label change preserves the revision. Config or secret changes create a new
revision; reauthentication also allocates a new credential identity. Previously
resolved models reject further dispatch after an update or removal. Resolve a new
model and explicitly reprepare interrupted work when changing accounts or config.
Refresh retains the credential identity and instance revision. Already dispatched
remote requests cannot be retracted by a local update.

`ProviderStore.layerMemory` and `MemoryCredentialStore.layer` are temporary.
For Node/Bun, `ProviderStoreNode.layer({ path })` persists public instances, while
`provider-auth-node/FileSystemCredentialStore.layer({ path })` persists credentials
at a **different path**. A keychain-backed credential store can be used instead.
See [provider credentials](provider-auth.md) for failure and locking semantics.

## Authentication belongs to the consumer

Add an unauthenticated instance first, or supply secrets during `add`. OAuth login
helpers only return credentials. After an explicit login, call
`providers.update(instance.id, { secrets })`; call the same method with
`secrets: null` for local logout. No manager operation opens a browser.

Provider callbacks receive decoded public config, the instance ID/revision, an
optional credential reference, `context.secrets` to read schema-validated secrets,
and `context.check` to reject stale instances. An unauthenticated context resolves
secrets to `undefined`. Each integration decides which operations need authentication;
static gateway catalogs remain readable before login.

## Catalogs and reusable transports

`provider-openai-codex/Provider.definition` requires `config.clientVersion` and
fetches its authenticated catalog lazily. The upstream catalog is a bulk endpoint;
list filtering/pagination and direct lookup use that response. Claude's
`provider-anthropic/Provider.definition` accepts `{}` and uses the Models API:
one page per list, a direct endpoint per detail lookup. Both reuse serialized OAuth
refresh. Neither guesses missing capabilities or invents pricing.

Their `define({ list, get })` helpers replace metadata discovery while retaining
inference mechanics. Claude accepts a second `{ thinkingPolicy(info) }` argument
for its durable thinking-history policy. Protocol-specific controls are still
validated by each model factory after portable validation.

`provider-openai-compatible/Provider.define({ version, config, connection, models })`
creates a gateway integration. `connection(config)` returns `{ baseUrl, authentication? }`;
`models` supplies independent list/get callbacks with the same typed context.
The shared transport handles Chat Completions and API-key authorization. Set
`authentication: 'optional'` for an anonymous endpoint. The default definition uses:

```ts
const instance =
  yield *
  providers.add({
    provider: 'gateway', // registered as Compatible.definition
    label: 'Private gateway',
    config: {
      baseUrl: 'https://gateway.example/v1',
      models: [
        {
          id: 'example-model',
          name: 'Example model',
          capabilities: { tools: true, images: false, reasoning: false, structuredOutput: false },
        },
      ],
    },
    secrets: { apiKey: Redacted.make(apiKey) },
  })
```

Use the factory for gateways such as OpenRouter whose metadata comes from their
own API. Static metadata is equally valid: `ModelCatalog.fromModels(models)`
creates a source with pagination/search, and `ModelCatalog.fromConfig(select)`
reads metadata from instance config. Metadata includes optional pricing, limits,
and per-model effort values. The compatible adapter supports `temperature`,
`max_output_tokens`, and `reasoning.effort` as schema-backed model options. Portable
thinking budgets are rejected because this transport has no mapping for them.

`ModelCatalog.cached(source, { capacity, timeToLive })` captures discovery services
without fetching and shares concurrent successful lookups. Create a separate cache
for each instance revision and endpoint; `invalidate` clears pages and details.
Codex `Catalog.cached(options, { timeToLive })` also shares the bulk catalog snapshot.
There is no implicit global cache across accounts.

## Breaking migration

The old per-account `Provider.make`, `makeWithSource`, global `ModelCatalog.make`,
and profile-keyed credentials have been removed. Register a definition once, move
account-specific values into `providers.add`, and use instance IDs for selection.
Login no longer persists credentials. Existing credential files require explicit
migration to the schema-encoded credential envelopes and opaque references; old
prepared account bindings require repreparation. Direct native Effect AI constructors
remain available for consumers that do not need instance management.
