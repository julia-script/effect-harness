# Effect Harness

Durable AI conversations, built with Effect.

Compose native Effect AI models and Toolkits, keep conversation state in typed documents, and recover execution with ordinary Effect Workflows. Your application owns the Layer graph: providers, tools, storage and the WorkflowEngine all remain explicit dependencies.

## Why Effect Harness?

- **Native AI primitives.** Use Effect's LanguageModel, Prompt, Tool and Toolkit APIs, with provider adapters for OpenAI and Anthropic.
- **Native Workflows.** Declare your own Workflows with `Workflow.make` and register them alongside the harness executor Layers. Use the engine's normal execute, poll and resume APIs.
- **Committed conversation state.** Sessions hold history, inboxes, agent settings and application documents. Keyed transactions return saved receipts when the same request is retried.
- **Tools with explicit recovery policy.** Bind handlers through Layers, retain invocation context, report progress and decide whether a tool body may run again after interruption.
- **Storage through Effect services.** Compose domain persistence from KeyValueStore and EventJournal, or use a single-writer JSONL Store. Persist the native engine too when execution must survive a restart.

## Start here

[Run your first durable conversation](https://github.com/julia-script/effect-harness/blob/main/docs/tutorials/first-conversation.md): build a local model, bind a tool, submit a message and replay its receipt. No API key or account is needed.

Install `effect-harness` and Effect with your preferred package manager:

```sh
npm install effect-harness effect
pnpm add effect-harness effect
yarn add effect-harness effect
bun add effect-harness effect
```

Run one of these commands. The package includes the harness, durable state, authorization and provider adapters. The docs assume you already know Effect; the [documentation index](https://github.com/julia-script/effect-harness/blob/main/docs/README.md) offers tutorials, task guides, reference and design explanations.

## Choose your imports

Root and subpath imports expose concept namespaces. Leaf imports select individual modules:

```ts
import * as Harness from 'effect-harness'
import * as Durable from 'effect-harness/durable'
import * as OpenAI from 'effect-harness/provider-openai'

import * as Model from 'effect-harness/Model'
import * as Session from 'effect-harness/durable/Session'
```

| Import                                                                                                                               | Use it for                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| [effect-harness](https://github.com/julia-script/effect-harness/blob/main/docs/modules/harness.md)                                   | Model selection, prompt preparation, extensions, hooks and native Toolkit execution                      |
| [effect-harness/durable](https://github.com/julia-script/effect-harness/blob/main/docs/modules/durable.md)                           | Sessions, typed documents, conversation state, committed observations and Workflow executor registration |
| [effect-harness/auth](https://github.com/julia-script/effect-harness/blob/main/docs/modules/auth.md)                                 | Credential storage, PKCE, token handling and JWT verification for application-owned account flows        |
| [effect-harness/provider-openai](https://github.com/julia-script/effect-harness/blob/main/docs/modules/provider-openai.md)           | OpenAI API keys and explicit ChatGPT account authorization                                               |
| [effect-harness/provider-anthropic](https://github.com/julia-script/effect-harness/blob/main/docs/modules/provider-anthropic.md)     | Anthropic API keys and explicit account authorization                                                    |
| [effect-harness/provider-claude-code](https://github.com/julia-script/effect-harness/blob/main/docs/modules/provider-claude-code.md) | An installed Claude Code CLI using its own login                                                         |

The generic harness handles model and tool execution. The `durable` modules add domain state and native Workflow registration. Provider and authorization modules have their own entry points so you can compose the services your application needs. Platform and storage adapters remain application dependencies.

## Build your application

| Next step                               | Read                                                                                                           |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Replace the local model                 | [Connect a provider](https://github.com/julia-script/effect-harness/blob/main/docs/providers.md)               |
| Give the model application capabilities | [Register tools](https://github.com/julia-script/effect-harness/blob/main/docs/tools.md)                       |
| Run your own durable jobs               | [Compose native Workflows](https://github.com/julia-script/effect-harness/blob/main/docs/workflows.md)         |
| Save state and recover execution        | [Persist conversations](https://github.com/julia-script/effect-harness/blob/main/docs/persistence.md)          |
| Show conversation progress in a UI      | [Observe committed changes](https://github.com/julia-script/effect-harness/blob/main/docs/observations.md)     |
| Let users authorize their accounts      | [Add account sign-in](https://github.com/julia-script/effect-harness/blob/main/docs/how-to/account-sign-in.md) |

Read [replay and recovery](https://github.com/julia-script/effect-harness/blob/main/docs/explanation/recovery.md) before enabling tools with external side effects. Saved receipts protect committed domain changes; recovery of an external action depends on that tool's policy. [Effect compatibility](https://github.com/julia-script/effect-harness/blob/main/docs/reference/compatibility.md) describes native response validation and recovery behavior.

## Work on the repository

This monorepo uses Bun for development:

```sh
bun install
bun run check
bun run test
```

`check` verifies formatting, strict lint, source and test types, and public type assertions. The [repository example](https://github.com/julia-script/effect-harness/blob/main/apps/example/README.md) exercises an offline model, a Toolkit, a custom Workflow and a real SQLite-backed engine. Consumer tutorials use Node.js; runtime services are supplied through Layers.

## Thanks

Thanks to [Pi](https://github.com/earendil-works/pi) and the Earendil team for their work on durable agents, which helped shape this project.
