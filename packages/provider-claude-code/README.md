# @effect-harness/provider-claude-code

Native LanguageModel boundary for an installed, independently authenticated Claude Code CLI.

```sh
bun add @effect-harness/provider-claude-code effect@4.0.1 @effect/platform-node@4.0.1
```

Public subpaths include `Cli`, `ClaudeCodeLanguageModel`, `LanguageModel`, `Catalog`, `IntentServer`, `Prompt` and `RequestOptions`.

Cli.layer consumes ChildProcessSpawner. Requests require explicit `policyTrust: 'trusted-installed-cli'` after the host audits its executable and managed policy. Model construction requires Cli and IntentServer; use a scoped loopback server for tool intents or IntentServer.layerDisabled without tools.

The default history policy rejects history it cannot faithfully import. Transcript mode renders saved history as input data and does not restore a CLI conversation. Authentication belongs to the CLI; the adapter does not copy its credentials.

Follow [the installed CLI guide](../../docs/how-to/account-sign-in.md#use-an-installed-claude-code-cli) and read [history/media constraints](../../docs/reference/packages.md#installed-claude-code) before selecting this transport.
