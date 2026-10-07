# Effect Harness

Libraries for TypeScript applications that use Effect v4, native Effect AI models and tools, and native Effect Workflow for durable conversation execution. This workspace pins Effect `4.0.1`; the packages are under development.

| Package                                | Responsibility                                                                                                                   |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `@effect-harness/harness`              | Model catalogue, extension registry, prompt preparation, hooks, AI Toolkit execution and portable coding tools                   |
| `@effect-harness/durable`              | Session records, documents, storage, conversation/inbox state, committed observations and executor Layers for ordinary Workflows |
| `@effect-harness/auth`                 | Application-owned credentials, PKCE, JWT verification and protected credential storage                                           |
| `@effect-harness/provider-openai`      | Native OpenAI models with API keys or explicit ChatGPT account authorization                                                     |
| `@effect-harness/provider-anthropic`   | Native Anthropic models with API keys or explicit account authorization                                                          |
| `@effect-harness/provider-claude-code` | Optional installed Claude CLI model boundary                                                                                     |

Run the [public integration example](apps/example/README.md) from this checkout:

```sh
bun install
bun run --cwd apps/example typecheck
bun run --cwd apps/example build
bun run --cwd apps/example test
```

It uses a deterministic native LanguageModel and AI Toolkit, all harness Workflow executors, and a real SQLite-backed native Workflow engine. It prints `native-workflow-example-ok` without credentials or inference requests. Keeping `EXAMPLE_DB` across runs demonstrates persisted receipt replay. Domain persistence uses Effect's KeyValueStore and EventJournal services; Effect owns database tables, CRUD and coordination. The harness keeps domain validation and replay receipts. Use a fresh example database when switching from the previous storage format.

`bun run check` checks formatting, strict lint, source and runtime-test types, and public type assertions against freshly built declarations. Source builds use native TypeScript `7.0.2`; the pinned TSTyche runner uses TypeScript `6.0.3` for its compiler-API assertions. Run `bun run test` for the full behavioral and physical-restart suites.

This checkout requires the [Effect patch](patches/effect@4.0.1.patch), applied by `bun install` through root `patchedDependencies`. It adds native AI unknown-tool-call handling and fixes Cluster Activity recovery ordering. Consumers outside this workspace must also apply the patch to Effect `4.0.1`.

The AI patch preserves undeclared tool names and parameters only when both `allowUnknownToolCalls: true` and `disableToolCallResolution: true` are enabled. Caller-owned settlement requires broad tool names and `unknown` parameter types; declared tools keep their runtime validation, and strict rejection remains the default. The harness Executor enables this behavior to commit unavailable-tool results.

The Cluster patch waits for a recovered Activity's definition before acquiring its SQL transaction, avoiding a deadlock when the resumed Workflow first reads a cached Activity result. Transaction-annotated Activities retain native transactional body/reply persistence. Built-in harness Activities use ordinary native replay and saved domain receipts.

Applications declare Workflows with `Workflow.make`, implement them with `toLayer`, and supply a native WorkflowEngine. The durable package supplies domain state and executor Layers. Effect owns execution identities, activity replay, suspension, timers and workflow results; the harness records conversation facts and controls recovery at external side-effect boundaries.

Start with [composition and custom Workflows](docs/workflows.md), [persistence](docs/persistence.md), [observations](docs/observations.md), [providers and account consent](docs/providers.md), or [coding tools](docs/tools.md). Package READMEs identify the public subpaths and required services.

Thanks to [Pi](https://github.com/earendil-works/pi) and the Earendil team for their work on durable agents, which helped shape this project.
