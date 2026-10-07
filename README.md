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

[Run your first durable conversation](docs/tutorials/first-conversation.md): build a local model, bind a tool, submit a message and replay its receipt. No API key or account is needed.

Install the core packages with your preferred package manager:

```sh
npm install @effect-harness/harness @effect-harness/durable effect
pnpm add @effect-harness/harness @effect-harness/durable effect
yarn add @effect-harness/harness @effect-harness/durable effect
bun add @effect-harness/harness @effect-harness/durable effect
```

Run one of these commands. Add a provider adapter when you are ready to use a remote model. The docs assume you already know Effect; the [documentation index](docs/README.md) offers tutorials, task guides, reference and design explanations.

## Choose your packages

| Package                                                                         | Use it for                                                                                               |
| ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| [@effect-harness/harness](packages/harness/README.md)                           | Model selection, prompt preparation, extensions, hooks and native Toolkit execution                      |
| [@effect-harness/durable](packages/durable/README.md)                           | Sessions, typed documents, conversation state, committed observations and Workflow executor registration |
| [@effect-harness/auth](packages/auth/README.md)                                 | Credential storage, PKCE, token handling and JWT verification for application-owned account flows        |
| [@effect-harness/provider-openai](packages/provider-openai/README.md)           | OpenAI API keys and explicit ChatGPT account authorization                                               |
| [@effect-harness/provider-anthropic](packages/provider-anthropic/README.md)     | Anthropic API keys and explicit account authorization                                                    |
| [@effect-harness/provider-claude-code](packages/provider-claude-code/README.md) | An installed Claude Code CLI using its own login                                                         |

The generic harness handles model and tool execution. The durable package adds domain state and native Workflow registration. Provider and authorization packages are separate so you can compose only the services your application needs.

## Build your application

| Next step                               | Read                                                  |
| --------------------------------------- | ----------------------------------------------------- |
| Replace the local model                 | [Connect a provider](docs/providers.md)               |
| Give the model application capabilities | [Register tools](docs/tools.md)                       |
| Run your own durable jobs               | [Compose native Workflows](docs/workflows.md)         |
| Save state and recover execution        | [Persist conversations](docs/persistence.md)          |
| Show conversation progress in a UI      | [Observe committed changes](docs/observations.md)     |
| Let users authorize their accounts      | [Add account sign-in](docs/how-to/account-sign-in.md) |

Read [replay and recovery](docs/explanation/recovery.md) before enabling tools with external side effects. Saved receipts protect committed domain changes; recovery of an external action depends on that tool's policy. [Effect compatibility](docs/reference/compatibility.md) describes the current patch requirements and how consumers apply them.

## Work on the repository

This monorepo uses Bun for development:

```sh
bun install
bun run check
bun run test
```

`check` verifies formatting, strict lint, source and test types, and public type assertions. The [repository example](apps/example/README.md) exercises an offline model, a Toolkit, a custom Workflow and a real SQLite-backed engine. Consumer tutorials use Node.js; runtime services are supplied through Layers.

## Thanks

Thanks to [Pi](https://github.com/earendil-works/pi) and the Earendil team for their work on durable agents, which helped shape this project.
