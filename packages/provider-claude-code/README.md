# @effect-harness/provider-claude-code

Optional native LanguageModel boundary for an installed Claude CLI. Public subpaths are `/Cli`, `/LanguageModel`, `/Catalog`, `/IntentServer` and `/Prompt`. This package consumes the external CLI's account authentication; it does not obtain consent for the direct Anthropic OAuth transport or copy credentials into the application's CredentialStore.

`Cli.layer` requires the native ChildProcessSpawner. The host selects an executable and output limit, and must explicitly set `policyTrust: 'trusted-installed-cli'` after auditing its installed CLI policy to enable requests. The adapter checks account authentication, clears provider override credentials, disables built-in tool execution and exposes framework tools through a scoped intent-only MCP boundary. The host still owns the installed executable and its managed policy.

`LanguageModel.layer({ model, cwd?, effort?, historyMode? })` consumes Cli and IntentServer. Supply `IntentServer.layer` with a scoped loopback HttpServer for tool intents, or `layerDisabled` for requests without tools. `Catalog.layer` supplies harness descriptors from declared models and limits. Processes and servers remain scoped and failures use native AiError.

The default history mode rejects multi-turn/assistant/tool history that cannot be faithfully imported into a CLI session. Explicit `historyMode: 'transcript'` renders canonical history as input data; it does not restore a native CLI conversation. Remote file URLs, unsupported media, structured-object generation, incremental provider response IDs and unsupported options fail rather than silently changing semantics. Supported inline image/PDF inputs still depend on the selected model.

Fake subprocess and loopback protocol tests cover the adapter. The offline public example does not execute an installed CLI. See [providers.md](../../docs/providers.md) before choosing between the direct Anthropic account transport and this optional boundary.
