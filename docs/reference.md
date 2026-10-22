# 📚 Reference

- [Imports](#imports)
- [Settings](#settings)
- [Agent state](#agent-state)
- [Built-in names](#built-in-names)

## Imports

Import concept namespaces from lowercase subpaths. Integrations with optional peers have their own subpaths, so importing the core never loads them.

```ts
import { HarnessClient, HarnessTransport } from 'effect-harness/client'
import { Extension, HarnessServer, Initializer } from 'effect-harness/harness'
import { Init } from 'effect-harness/protocol'
```

| Subpath (`effect-harness/…`)           | Exports                                                                                                                                           | Runs on       |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `client`                               | `HarnessClient`, `HarnessTransport`                                                                                                               | client        |
| `protocol`                             | `HarnessRpcs`, `Init`, `Submission`, `HarnessError`, `AgentState`, `ViewState`, `AgentEvent`                                                      | both          |
| `harness`                              | `Harness`, `HarnessServer`, `Conversation`, `Extension(s)`, `Settings`, `Generation`, `Compaction`, `ToolSearch`, `Initializer`, `Agent`, `Usage` | loop          |
| `model`                                | `Model`, `Message`, `Transcript`, `Faux`, `LanguageModelProvider`, `ModelError`                                                                   | both          |
| `tool`                                 | `Tool`, `Toolkit`, `ToolResult`, `ToolTask`, `Call`, `ToolError`                                                                                  | both          |
| `storage`, `storage/schema`            | `Storage` layers; stored record Schemas                                                                                                           | loop          |
| `session`                              | `Session`, `Document`, `Observation`                                                                                                              | loop          |
| `task`                                 | `Task`, `Registry`, `Scheduler`, `Hook`                                                                                                           | loop          |
| `delta`                                | `Tracker`, `Ops`: immutable JSON revisions                                                                                                        | loop          |
| `credentials`, `credentials/bun`       | `CredentialStore`; `BunCredentials` (OS keychain)                                                                                                 | both          |
| `accounts`                             | `Adapter`, `Account`, `Accounts`, `AccountsRpcs`                                                                                                  | both          |
| `openai`, `openai/adapter`             | `OpenAi`, `OpenAiCodex`; their adapter declarations                                                                                               | loop · client |
| `anthropic`, `anthropic/adapter`       | `Anthropic`, `AnthropicOAuth`; their adapter declarations                                                                                         | loop · client |
| `coding`, `coding/bun`, `coding/sharp` | `CodingTools`, `CodingHandlers`, `ImageProcessor`; Bun and sharp image codecs                                                                     | loop          |
| `codemode`                             | `Codemode`, `CodemodeHandlers`                                                                                                                    | loop          |
| `cloudflare`                           | `HarnessObject`, `HarnessWorkflow`                                                                                                                | Workers       |

The root import, `effect-harness`, groups the portable core as `Accounts`, `Client`, `Credentials`, `Delta`, `Harness`, `Model`, `Protocol`, `Session`, `Storage`, `Task` and `Tool`.

## Settings

Pass `settings` to `HarnessServer.layer` (or `Settings.layer`). Every field is optional; an Effect is read at every use, so settings can change while the loop runs.

| Setting             | Default                                               | Meaning                                                                                                                 |
| ------------------- | ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `model`             | none                                                  | Model of conversations whose `$agent` sets none.                                                                        |
| `reasoning`         | `'off'`                                               | Reasoning level of conversations whose `$agent` sets none.                                                              |
| `extensions`        | every installed extension                             | The default extension selection, by name.                                                                               |
| `steeringMode`      | `'one-at-a-time'`                                     | How many queued steers a boundary places: one, or `'all'`.                                                              |
| `followUpMode`      | `'one-at-a-time'`                                     | The same for follow-ups at the final boundary.                                                                          |
| `toolExecution`     | `'parallel'`                                          | Run a round's tool calls together, or `'sequential'`.                                                                   |
| `retry`             | on · 3 retries · 2 s → 60 s                           | Durable exponential backoff for retryable model failures.                                                               |
| `stream`            | `{}`                                                  | Request options for every generation: `maxTokens`, `temperature`, `cacheRetention`, `timeoutMs`, `headers`, `deferred`… |
| `partialIntervalMs` | `100`                                                 | Minimum pause between commits of a streaming answer.                                                                    |
| `compaction`        | on · reserve 16,384 · keep 20,000 · background 32,768 | When and how much to summarize. Override per model with `modelOverrides: { 'provider/modelId': { … } }`.                |

## Agent state

What `conversation.configure(change)` sets in `$agent`. A given field replaces the stored one; `null` clears it.

| Field          | Type                                                                    | Meaning                                                              |
| -------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `model`        | `{ provider, modelId }`                                                 | The model to ask.                                                    |
| `reasoning`    | `'off'`, `'minimal'`, `'low'`, `'medium'`, `'high'`, `'xhigh'`, `'max'` | Reasoning effort.                                                    |
| `extensions`   | `string[]` or `{ add?, remove? }`                                       | Exactly these, or the default selection edited.                      |
| `tools`        | `string[]` or `{ remove }`                                              | Which of the selected tools are enabled.                             |
| `modelTools`   | `string[]` or `{ add?, remove? }`                                       | Which enabled tools the model is offered; `add` loads deferred ones. |
| `instructions` | `string`                                                                | Appended to the system prompt as the `instructions` section.         |
| `config`       | `{ [tool]: config }`                                                    | Tool config, over the config the extension installs it with.         |

## Built-in names

| Kind of thing | Names                                                                     |
| ------------- | ------------------------------------------------------------------------- |
| Entries       | `$user`, `$system`, `$assistant`, `$tool-result`, `$compaction`, `$reset` |
| Documents     | `$agent`, `$inbox`, `$live`, `$usage`                                     |
| Tasks         | `$generation`, `$tool`, `$compaction`                                     |
| Extensions    | `coding-tools`, `codemode`, `tool-search`                                 |
| Tools         | `read`, `write`, `edit`, `bash`, `codemode`, `tool_search`                |
| Result type   | `$tool-output` (the `ToolResult.Output` discriminant)                     |

Anything starting with `$` is reserved for the harness.
