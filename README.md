# Effect Harness

Libraries for TypeScript applications that use Effect v4, native Effect AI models and tools, and native Effect Workflow for durable conversation execution. This workspace pins Effect `4.0.1`; the packages are under development here, with no npm release assumed by these instructions.

| Package                                | Responsibility                                                                                                                   |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `@effect-harness/harness`              | Model catalogue, extension registry, prompt preparation, hooks, AI Toolkit execution and portable coding tools                   |
| `@effect-harness/durable`              | Session records, documents, storage, conversation/inbox state, committed observations and executor Layers for ordinary Workflows |
| `@effect-harness/auth`                 | Application-owned credentials, PKCE, JWT verification and protected credential storage                                           |
| `@effect-harness/provider-openai`      | Native OpenAI models with API keys or explicit ChatGPT account authorization                                                     |
| `@effect-harness/provider-anthropic`   | Native Anthropic models with API keys or Pi-compatible explicit account authorization                                            |
| `@effect-harness/provider-claude-code` | Optional installed Claude CLI model boundary                                                                                     |

Run the [public integration example](apps/example/README.md) from this checkout:

```sh
bun install
bun run --cwd apps/example typecheck
bun run --cwd apps/example build
bun run --cwd apps/example test
```

It uses a deterministic native LanguageModel and AI Toolkit, all harness Workflow executors, and a real SQLite-backed native Workflow engine. It prints `native-workflow-example-ok` without credentials or inference requests. Keeping `EXAMPLE_DB` across runs demonstrates persisted receipt replay.

This checkout requires the [pinned Effect patch](patches/effect@4.0.1.patch), applied by Bun through root `patchedDependencies`. It adds an opt-in native AI `allowUnknownToolCalls` flag for caller-owned unavailable-tool settlement, and moves native Cluster Activity readiness before SQL transaction acquisition to avoid a reproduced recovery deadlock. Activity body/reply storage remains in the native transaction; ordinary Workflow authoring APIs stay the same. Installing these packages with an unpatched Effect dependency elsewhere does not supply these behaviors; no published npm distribution is assumed. See [dependency requirements and parity scope](docs/parity.md).

Applications declare Workflows with `Workflow.make`, implement them with `toLayer`, and supply a native WorkflowEngine. The durable package supplies domain state and executor Layers. Effect owns execution identities, activity replay, suspension, timers and workflow results; the harness records conversation facts and controls recovery at external side-effect boundaries.

Start with [composition and custom Workflows](docs/workflows.md), [persistence](docs/persistence.md), [observations](docs/observations.md), [providers and account consent](docs/providers.md), or [coding tools](docs/tools.md). Package READMEs identify the public subpaths and required services.

The implementation has been independently reviewed against the pinned pi-durable behavioral contract using executable regression and restart coverage, with the authorized Effect-native substitutions. The [public contract map](docs/parity.md) connects those behavior groups to implementation and tests and states the integration limits. That reviewed scope does not guarantee future upstream scenarios or an external unpatched dependency installation. Local examples and protocol tests use deterministic models, HTTP fixtures and subprocess fixtures. Live OAuth, paid inference and the installed Claude CLI are separate integration boundaries.

Selected components adapt Pi's MIT-licensed source at commit [`636703a0a4f2f4d8558d08f2308cb41109585bf5`](https://github.com/earendil-works/pi/tree/636703a0a4f2f4d8558d08f2308cb41109585bf5). The table identifies current adaptation families; it does not claim that every implementation file was copied.

| Component                                                             | Current source anchors                                                                                                        | Pi provenance                                                                       |
| --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Prompt planning and context projection                                | [Prompt.ts](packages/harness/src/Prompt.ts), [Context.ts](packages/harness/src/Context.ts)                                    | `packages/durable/src/harness/{prompt,context}.ts`                                  |
| Compaction selection and serialization                                | [Compaction.ts](packages/harness/src/Compaction.ts)                                                                           | `packages/durable/src/harness/compaction.ts`                                        |
| Output bounds, text decoding and line scans                           | [Output.ts](packages/harness/src/Output.ts), [env](packages/harness/src/env)                                                  | `packages/durable/src/harness/output.ts`, `src/env/{decode,line-scan}.ts`           |
| File reading, image recognition, truncation and edit diffs            | [tools](packages/harness/src/tools)                                                                                           | `packages/durable/src/tools/{read,image,edit-diff}.ts`, `src/truncate.ts`           |
| Inbox rules and committed structural/semantic observation             | [Inbox.ts](packages/durable/src/Inbox.ts), [View.ts](packages/durable/src/View.ts), [Event.ts](packages/durable/src/Event.ts) | `packages/durable/src/harness/{inbox,view,events}.ts`                               |
| Provider error classification patterns                                | [Model.ts](packages/harness/src/Model.ts)                                                                                     | `packages/ai/src/utils/{retry,overflow}.ts`                                         |
| Anthropic account OAuth constants and transport identity/tool aliases | [OAuth.ts](packages/provider-anthropic/src/OAuth.ts), [Account.ts](packages/provider-anthropic/src/Account.ts)                | `packages/ai/src/auth/oauth/anthropic.ts`, `packages/ai/src/providers/anthropic.ts` |

Storage/session and ownership behavior are also checked against the pinned reference contract. Native Workflow declarations, engine composition and Effect service boundaries are authored for this architecture; they replace Pi's execution machinery. Pi credit in this README remains as permanent project provenance. The adapted packages distribute full MIT attribution in their `NOTICE` files while adapted material remains. When an adaptation is substantially rewritten or removed, review its source comment and this table alongside the actual remaining implementation; update attribution deliberately rather than carrying a stale per-file source ledger forward.
