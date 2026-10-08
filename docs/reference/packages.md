# Modules and services

This reference covers module boundaries, public module entry points and service construction. Individual function signatures and schema fields are documented in the TypeScript API comments.

Install `effect-harness` for the generic harness, durable state, authorization and provider modules. Platform adapters are supplied by the application. Root and directory imports expose concept namespaces; public subpath imports allow direct module access. Internal and retired storage subpaths are excluded from package exports.

## Generic harness

`effect-harness` contains model selection, prompt preparation, extension resolution and model/tool execution. It has no dependency on durable storage or WorkflowEngine.

| Public module                                      | Contract                                                           |
| -------------------------------------------------- | ------------------------------------------------------------------ |
| `Model`                                            | Catalog service and native LanguageModel descriptors               |
| `Agent`                                            | Model references, conversation overrides and host settings         |
| `Registry`, `Extension`                            | Named extensions containing tools, sections, hooks and wrappers    |
| `Executor`                                         | Request preparation, model streams, tool execution and compaction  |
| `ToolRegistration`, `ToolResult`, `Invocation`     | Toolkit binding, result projection and dynamic invocation services |
| `Hook`                                             | Captured host dependencies and declared per-request services       |
| `Transcript`, `ResponseAccumulator`, `Usage`       | Transcript projection, response state and usage accounting         |
| `Env`, `NativeFiles`, `NodeEnv`, `NodeNativeFiles` | Filesystem/process environment and narrow platform capabilities    |
| `MutationLocks`, `tools`                           | Shared mutation coordination and portable coding tools             |
| `testing`                                          | Adapter assertions, conformance cases and test-runner integration  |

`Executor.layer` requires Registry and Model.Catalog. `Registry.layer(extensions)` validates extension names, duplicate tools within an extension and section keys; construction can fail with RegistryError. Later selected extensions can replace tools/sections of the same name. Binding captures handlers and host services at Layer construction.

`Model.layer(descriptors)` provides Catalog with no acquisition error. Repeated provider/model references retain the last descriptor. Unknown references fail with ModelError carrying ModelNoModel. Provider catalogue constructors add their own entry validation.

## Durable state and execution

`effect-harness/durable` adds committed state and executor registration for ordinary Effect Workflows.

| Layer                                      | Outputs                                                  | Required inputs                                                            | Acquisition error |
| ------------------------------------------ | -------------------------------------------------------- | -------------------------------------------------------------------------- | ----------------- |
| `Store.layerMemory`                        | Store                                                    | None                                                                       | None              |
| `storage/JsonlStore.layer(options)`        | Store                                                    | FileSystem, Path                                                           | StorageError      |
| `storage/SnapshotStore.layerWith(options)` | Store                                                    | KeyValueStore, EventJournal                                                | StorageError      |
| `Session.layer`                            | Session                                                  | Store; optional captured CreationHook                                      | None              |
| `Conversation.layer(options)`              | Configuration, CreationHook                              | Crypto; optional captured Registry                                         | SchemaError       |
| `Conversation.layerFromSession`            | Conversation                                             | Session                                                                    | None              |
| `Conversation.layerConfiguration(options)` | Configuration                                            | None                                                                       | SchemaError       |
| `Conversation.layerCreation`               | CreationHook                                             | Configuration, Crypto; optional captured Registry                          | None              |
| `SessionDirectory.layerSingle(id)`         | SessionDirectory                                         | Session                                                                    | None              |
| `View.layer`                               | View                                                     | Store                                                                      | None              |
| `Event.layer`                              | Event                                                    | View                                                                       | None              |
| `Executor.layer`                           | Cancellation, DrainConversations, Ownership.Declarations | SessionDirectory, Configuration, harness Executor, Catalog, WorkflowEngine | None              |
| `Executor.layerExecutors`                  | Cancellation, DrainConversations                         | The above inputs plus Ownership.Declarations                               | None              |

"None" in the error column means no typed acquisition error; operations can still fail through their documented channels. Scoped resources are acquired and released in the Layer's lifetime.

`SessionDirectory.layer` requires explicit `SessionDirectory.Registrations`, a map of identities to already scoped Session services. It snapshots registrations when built and does not acquire or extend their lifetimes. Unregistered identities fail with NotFound.

Public domain modules include `Record`, `Identity`, `Entry`, `Document`, `Conversation`, `Inbox`, `Usage`, `Ownership`, `Inspection`, `View` and `Event`. Storage constructors live under `storage`; native declarations and handler helpers live under `workflow`. `testing` exposes Store conformance helpers.

## Providers

Provider catalogue Layers output harness Model.Catalog and, where documented, the exact captured native client. They describe caller-declared models; construction does not establish remote entitlement.

| Module / constructor                                     | Required inputs        | Outputs                                 |
| -------------------------------------------------------- | ---------------------- | --------------------------------------- |
| `provider-openai/OpenAiLanguageModel.layer`              | Native OpenAiClient    | LanguageModel, captured OpenAiClient    |
| `provider-openai/OpenAiLanguageModel.layerApiKey`        | HttpClient             | LanguageModel, OpenAiClient             |
| `provider-openai/Catalog.layerApiKey`                    | HttpClient             | Catalog, OpenAiClient                   |
| `provider-openai/ChatGptLanguageModel.layer`             | ChatGpt, HttpClient    | LanguageModel, OpenAiClient             |
| `provider-openai/Catalog.layerChatGpt`                   | ChatGpt, HttpClient    | Catalog, OpenAiClient                   |
| `provider-anthropic/AnthropicLanguageModel.layer`        | Native AnthropicClient | LanguageModel, captured AnthropicClient |
| `provider-anthropic/AnthropicLanguageModel.layerApiKey`  | HttpClient             | LanguageModel, AnthropicClient          |
| `provider-anthropic/Catalog.layerApiKey`                 | HttpClient             | Catalog, AnthropicClient                |
| `provider-anthropic/AnthropicAccountClient.layer`        | OAuth, HttpClient      | AnthropicClient                         |
| `provider-anthropic/AnthropicAccountLanguageModel.layer` | OAuth, HttpClient      | LanguageModel, AnthropicClient          |
| `provider-anthropic/Catalog.layer`                       | Native AnthropicClient | Catalog                                 |

These module paths have the prefix `effect-harness/`. API-key constructors accept Redacted keys. Config variants resolve Config.Wrap options at Layer construction and add ConfigError to their error channels. OpenAI uses Responses; Anthropic uses Messages. Direct transports preserve structured Prompt history and perform tool-media conversion at the captured client boundary.

## Authorization

`effect-harness/auth` supplies Credential, CredentialStore, Pkce, Token and Jwt modules. `CredentialStore.layerMemory` requires Crypto. `layerProtectedFile({ path, lockRetries? })` requires FileSystem, Path and Crypto and can fail with AuthError. It stores plaintext secrets behind owner-only permissions, atomic replacement and cross-process locks; it provides no encryption.

`JoseJwt.layer` requires HttpClient and provides Jwt verification. It validates signatures, issuer, audience, required claims, expiry and a supplied nonce before constructing identity.

`provider-openai/ChatGpt.layer` requires CredentialStore, Jwt, HttpClient and Crypto. `provider-openai/Callback.layer` requires ChatGpt and a scoped HttpServer. ChatGPT account keys derive from verified provider/issuer/client/subject identity via `Credential.accountKey`, not email.

`provider-anthropic/OAuth.layer` requires CredentialStore, HttpClient and Crypto. `OAuth.layerCallback` additionally uses the OAuth service and scoped HttpServer. Anthropic OpaqueOAuth account keys are selected by the host and make no OIDC identity claim. Both providers maintain pending consent in their service lifetime and serialize credential updates. Providers do not import other applications' credential files.

## Installed Claude Code

`effect-harness/provider-claude-code` exports Cli, ClaudeCodeLanguageModel, Catalog, IntentServer, Prompt and RequestOptions. `Cli.layer` requires ChildProcessSpawner. Its defaults are executable `claude` and maximum output of 16 MiB. Requests require `policyTrust: 'trusted-installed-cli'`; without it they fail. Authentication belongs to the installed CLI.

`ClaudeCodeLanguageModel.layer({ model, cwd?, effort?, historyMode? })` requires Cli and IntentServer, and exposes LanguageModel plus the captured Cli. IntentServer uses a scoped loopback HttpServer; `layerDisabled` supports requests without tools. Native AiError describes unsupported, authentication, subprocess and protocol failures.

The default history mode rejects multi-turn/assistant/tool history that cannot be faithfully imported. `historyMode: 'transcript'` renders canonical history as input data and does not resume a native CLI conversation. Remote file URLs, unsupported media/options, structured-object generation and incremental provider response IDs fail explicitly. Inline image/PDF support also depends on the selected model. Tool intents are routed to the harness; the adapter restricts built-in CLI tool execution and clears alternate provider credentials. The host owns executable and managed-policy trust.

## Tool media and native validation

OpenAI and Anthropic adapters translate validated harness tool-content envelopes within the original native tool-result item. Call identity and mixed text/media order are preserved. Private details, controls and usage are separate committed metadata.

Ordinary JSON results retain native JSON behavior. A valid envelope containing unsupported media or invalid provider options fails with a native AiError. Anthropic supports its native image/PDF/text-document representations and applicable cache/title/context/citation options; OpenAI supports native Responses text/image/file representations. The selected model still determines which media are accepted.
