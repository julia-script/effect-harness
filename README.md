# Effect Harness

Durable AI conversations for TypeScript applications built with Effect v4. Use native Effect AI models and Toolkits, keep conversation facts in typed Session documents, and run execution through ordinary Effect Workflows.

Your application chooses its providers, platform services and persistence Layers. The generic harness handles model/tool execution; the durable package supplies committed state and executor registration.

## Get started

```sh
bun add @effect-harness/harness @effect-harness/durable effect@4.0.1
```

[Run your first durable conversation](docs/tutorials/first-conversation.md) with a local model and a tool, then replay the same request. No account or API key is required. [Save application state across restarts](docs/tutorials/persistent-state.md) introduces documents and persisted transaction results.

The docs assume familiarity with Effect. [Browse the documentation](docs/README.md) for tutorials, integration guides, reference and architecture explanations.

## Packages

| Package                                                                         | Responsibility                                                                                      |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| [@effect-harness/harness](packages/harness/README.md)                           | Model catalogue, extensions, prompt preparation, hooks, Toolkit execution and coding tools          |
| [@effect-harness/durable](packages/durable/README.md)                           | Sessions, documents, conversation/inbox state, committed observations and native Workflow executors |
| [@effect-harness/auth](packages/auth/README.md)                                 | Application-owned credentials, PKCE, JWT verification and protected credential storage              |
| [@effect-harness/provider-openai](packages/provider-openai/README.md)           | OpenAI API-key and ChatGPT account transports                                                       |
| [@effect-harness/provider-anthropic](packages/provider-anthropic/README.md)     | Anthropic API-key and account transports                                                            |
| [@effect-harness/provider-claude-code](packages/provider-claude-code/README.md) | Installed Claude Code CLI model boundary                                                            |

## Integration guides

[Connect a provider](docs/providers.md), [register tools](docs/tools.md), [compose custom Workflows](docs/workflows.md), [persist conversations](docs/persistence.md), [observe committed progress](docs/observations.md), or [add account sign-in](docs/how-to/account-sign-in.md).

The [service reference](docs/reference/packages.md) lists Layer dependencies. [Replay and recovery](docs/explanation/recovery.md) explains the guarantees around committed results and external side effects. The full contract on Effect 4.0.1 uses a [compatibility patch](docs/reference/compatibility.md); consumer installations must apply it explicitly.

## Develop from source

Use Bun in this monorepo:

```sh
bun install
bun run check
bun run test
```

`check` covers formatting, strict lint, source/test types and public type assertions. The [offline integration example](apps/example/README.md) combines a real SQLite-backed native engine, domain persistence, a local model and a Toolkit.

Thanks to [Pi](https://github.com/earendil-works/pi) and the Earendil team for their work on durable agents, which helped shape this project.
