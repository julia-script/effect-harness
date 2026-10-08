# Effect Harness

Embedded durable AI conversations, built with Effect.

An application opens a scoped `Harness` with a model catalogue, a tool registry and a `Persistence` Layer. Conversations, task checkpoints, tool results and typed documents share atomic commits. One harness process owns each store; clients call that process's conversation APIs.

- **Native Effect AI.** Models use `LanguageModel`, `Prompt`, `Tool` and `Toolkit`.
- **Checkpoint tasks.** Named, versioned task definitions run phases outside transactions and save their next checkpoint or outcome.
- **Atomic application state.** A task can commit its output, document edits and terminal state together.
- **Explicit tool recovery.** Replay policies distinguish safe repeatable work from interrupted external actions.
- **Committed observations.** Watches start with a snapshot and stream subsequent commits.

## Run the example

```sh
bun install
bun run build
bun run --cwd apps/example test
```

The offline example calls an uppercase tool, returns `HELLO`, and runs a custom greeting task. It requires no API key. [Run your first conversation](https://github.com/julia-script/effect-harness/blob/main/docs/tutorials/first-conversation.md) explains the application.

## Choose your imports

```ts
import * as Harness from 'effect-harness/Harness'
import * as Conversation from 'effect-harness/Conversation'
import * as Submission from 'effect-harness/Submission'
import * as SqliteBun from 'effect-harness/storage/SqliteBun'
```

The package root exposes concept namespaces. Leaf imports select individual modules. Storage and provider adapters have dedicated subpaths:

| Import                                            | Purpose                                                    |
| ------------------------------------------------- | ---------------------------------------------------------- |
| `effect-harness`                                  | Harness, conversations, documents, tasks, models and tools |
| `effect-harness/storage/Memory`                   | Process-local persistence                                  |
| `effect-harness/storage/SqliteBun` / `SqliteNode` | Indexed persistent records                                 |
| `effect-harness/storage/JsonlBun` / `JsonlNode`   | Framed JSONL commits                                       |
| `effect-harness/provider-openai`                  | Native API-key OpenAI models and catalogues                |
| `effect-harness/provider-anthropic`               | Native API-key Anthropic models and catalogues             |
| `effect-harness/tools`                            | Portable coding tools                                      |

Your application supplies platform services and the execution environment. Recovery requires that the store and the resources used by tools remain available. See [recovery](https://github.com/julia-script/effect-harness/blob/main/docs/explanation/recovery.md).

## Build an application

[Connect a provider](https://github.com/julia-script/effect-harness/blob/main/docs/providers.md), [register tools](https://github.com/julia-script/effect-harness/blob/main/docs/tools.md), [declare tasks](https://github.com/julia-script/effect-harness/blob/main/docs/tasks.md), [persist state](https://github.com/julia-script/effect-harness/blob/main/docs/persistence.md), or [observe committed changes](https://github.com/julia-script/effect-harness/blob/main/docs/observations.md). The [documentation index](https://github.com/julia-script/effect-harness/blob/main/docs/README.md) links the contracts and examples.

External clients use an application-owned API or transport around the embedded harness. Storage ownership and tool scheduling remain inside the harness process.

## Develop

```sh
bun run check
bun run test
bun run build
```

`check` verifies formatting, lint, source and test types, and public type assertions. [The repository example](https://github.com/julia-script/effect-harness/blob/main/apps/example/README.md) exercises the published package exports with SQLite. Package changes use [Changesets](https://github.com/julia-script/effect-harness/blob/main/.changeset/README.md).

## Thanks

This project adapts [Pi Durable](https://github.com/earendil-works/pi)'s embedded conversation and task design using Effect services and scoped fibers. Upstream attribution is retained in [NOTICE](https://github.com/julia-script/effect-harness/blob/main/packages/effect-harness/NOTICE).
