# @effect-harness/provider-claude-code

Use an installed Claude Code CLI as a native Effect AI LanguageModel.

This adapter runs a CLI that the host has installed and authenticated independently. It supplies native model and harness catalogue Layers, with an optional loopback intent server for harness tools. Authentication stays with the CLI.

## Install

```sh
npm install @effect-harness/provider-claude-code effect @effect/platform-node
```

Use the equivalent `pnpm add`, `yarn add` or `bun add` command if you prefer. Install and sign in to Claude Code separately before making requests.

## Compose the model Layer

After auditing the installed executable and managed policy, opt into that CLI explicitly. This composition disables harness tool intents:

```ts
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as ClaudeCodeLanguageModel from '@effect-harness/provider-claude-code/ClaudeCodeLanguageModel'
import * as Cli from '@effect-harness/provider-claude-code/Cli'
import * as IntentServer from '@effect-harness/provider-claude-code/IntentServer'
import * as Config from 'effect/Config'
import * as Layer from 'effect/Layer'

const InstalledCli = Cli.layer({ policyTrust: 'trusted-installed-cli' })

export const Model = ClaudeCodeLanguageModel.layerConfig({
  model: Config.String('CLAUDE_CODE_MODEL'),
}).pipe(
  Layer.provide(Layer.mergeAll(InstalledCli, IntentServer.layerDisabled)),
  Layer.provide(NodeServices.layer),
)
```

Set `CLAUDE_CODE_MODEL` to a model your CLI can use. Provide `Model` to native LanguageModel requests. For harness execution, use `Catalog.layer` with declared model limits and the same Cli and IntentServer services.

To expose harness tools, replace the disabled intent server with `IntentServer.layer` and a scoped loopback HttpServer. Keep that Scope alive for the requests it serves.

## Understand saved history

The CLI's public interface cannot faithfully import arbitrary assistant/tool history. The default policy rejects unsupported history. Explicit transcript mode renders saved messages as input data; it does not restore a native CLI conversation.

Read [history and media constraints](../../docs/reference/packages.md#installed-claude-code) before choosing this transport for persisted conversations. The adapter does not copy CLI credentials into the application's CredentialStore.

## Find the right API

| Module                                     | Purpose                                              |
| ------------------------------------------ | ---------------------------------------------------- |
| `Cli`                                      | Installed process boundary and explicit trust policy |
| `ClaudeCodeLanguageModel`, `LanguageModel` | Native model construction                            |
| `Catalog`                                  | Harness model declarations and configuration         |
| `IntentServer`                             | Scoped tool intent transport                         |
| `Prompt`, `RequestOptions`                 | History policy and per-request options               |

[Use an installed Claude Code CLI](../../docs/how-to/account-sign-in.md#use-an-installed-claude-code-cli) describes the authorization path. [Connect a provider](../../docs/providers.md) explains how catalogues fit into the harness.
