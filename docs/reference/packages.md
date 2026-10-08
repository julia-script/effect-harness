# Modules and services

Install `effect-harness` and `effect`. Runtime-specific platform and storage drivers are application dependencies. Root imports expose concept namespaces; leaf imports select individual modules.

Internal subpaths are excluded from package exports. NodeEnv and NodeNativeFiles are available through explicit leaf imports.

| Module                                                            | Contract                                                                       |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `Harness`                                                         | Scoped owner, admission, task scheduling, recovery and committed observations  |
| `Conversation`, `Submission`                                      | Conversation capabilities and saved submission identities                      |
| `Persistence`                                                     | Record reads, Stream scans and atomic batches                                  |
| `Task`, `TaskRuntime`                                             | Versioned definitions, phase transitions and invocation capabilities           |
| `Record`, `Identity`                                              | Persisted schemas and identifiers                                              |
| `Document`                                                        | Typed documents, scopes, history and fork policies                             |
| `Observation`                                                     | Serializable snapshots and committed changes                                   |
| `Executor`                                                        | Native model/tool execution and compaction preparation                         |
| `Model`, `Agent`                                                  | Native model descriptors and validated settings                                |
| `Registry`, `Extension`, `Hook`                                   | Named tools, prompt sections and captured callbacks                            |
| `ToolRegistration`, `ToolResult`, `Invocation`                    | Toolkit binding, tool-media projection and live invocation services            |
| `Transcript`, `PromptPreparation`, `ResponseAccumulator`, `Usage` | Transcript projection, prompt preparation, response state and usage accounting |
| `Env`, `NativeFiles`, `NodeEnv`, `NodeNativeFiles`                | Application-owned filesystem and process capabilities                          |
| `tools`                                                           | Portable coding tools                                                          |

`Harness.layer(options)` requires Persistence and Executor, and can fail with StorageError or SchemaError. `Executor.layer` requires Registry and Model.Catalog. `Registry.layer` validates extension declarations and can fail with RegistryError; binding captures handlers and host services at Layer construction.

`Model.layer(descriptors)` registers already-acquired native descriptors with no acquisition error. Repeated provider/model references retain the last descriptor. Unknown references fail with `ModelError` carrying `ModelNoModelError`; provider catalogue constructors add their own entry validation.

## Storage adapters

Memory has no durable backing. SQLite adapters store indexed records and hide their driver service. JSONL adapters store framed commits and retain rebuilt indexes in memory. All adapter operations use StorageError, including the rejected/uncertain distinction.

## Providers

| Constructor                                             | Required input         | Output                            |
| ------------------------------------------------------- | ---------------------- | --------------------------------- |
| `provider-openai/OpenAiLanguageModel.layer`             | Native OpenAiClient    | LanguageModel and captured client |
| `provider-openai/OpenAiLanguageModel.layerApiKey`       | HttpClient             | LanguageModel and client          |
| `provider-openai/Catalog.layerApiKey`                   | HttpClient             | Model.Catalog and client          |
| `provider-anthropic/AnthropicLanguageModel.layer`       | Native AnthropicClient | LanguageModel and captured client |
| `provider-anthropic/AnthropicLanguageModel.layerApiKey` | HttpClient             | LanguageModel and client          |
| `provider-anthropic/Catalog.layerApiKey`                | HttpClient             | Model.Catalog and client          |

API-key constructors accept Redacted keys. Config variants resolve Config.Wrap values through the caller's ConfigProvider. Catalogues validate declared IDs, token limits, supported options and prices; remote entitlement is checked by inference.

## Tool media and native validation

Provider adapters translate canonical tool-content envelopes within the original native result item. They preserve call identity and mixed text/media order. Private details, controls and usage remain separate metadata.

Ordinary JSON retains native behavior. Unsupported media or invalid provider options fail with native AiError. The selected model determines which media it accepts. See [compatibility](compatibility.md).
