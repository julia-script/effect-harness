# Native Workflow example

An offline application combining a native Effect Workflow, a model/tool conversation and SQLite-backed recovery.

Start with [src/main.ts](src/main.ts). It selects the model, executes a custom greeting Workflow, submits a conversation input through the native Submission Workflow, and prints the committed answer. The application supplies its services once through Layers.

For a fresh application installed from npm, follow [your first durable conversation](../../docs/tutorials/first-conversation.md). This directory is a repository integration example: it uses the monorepo's Bun tooling and Bun SQLite adapter.

## Run it

From the repository root:

```sh
bun install
bun run typecheck
bun run build
bun run --cwd apps/example test
```

The application prints:

```text
Hello, Effect
HELLO
native-workflow-example-ok
```

No account, remote inference or model download is needed. The workspace build graph compiles library dependencies before the example and checks their public declarations. Tests import the compiled application through package exports, then run its compiled entrypoint. Effect dependencies are unmodified; see [Effect compatibility](../../docs/reference/compatibility.md) for validation and recovery behavior.

## Reproduce an unknown tool response

[src/ReproduceUnknownTool.ts](src/ReproduceUnknownTool.ts) submits a conversation through the public API with a local model that calls `upper_case`, while only `uppercase` is registered. Run it from the repository root:

```sh
bun run --cwd apps/example reproduce:unknown-tool
```

Native validation rejects the undeclared name. The program prints `InputUnanswered`, reason `model_error`, and confirms that `InvalidOutputError` was saved on the failed generation entry. Retries are disabled so it finishes after one attempt. No credentials are required.

To let the model recover using generic validation feedback:

```sh
bun run --cwd apps/example reproduce:unknown-tool --retry
```

This run allows one retry. The model sees that its previous response could not be validated, returns a valid answer, and the program prints `InputDone`. The original error remains saved; the rejected call is unavailable and no tool result is fabricated. Each run uses a temporary SQLite database by default; leave `EXAMPLE_DB` unset to avoid replaying an earlier submission.

## Follow the composition

| Module                               | What it demonstrates                                                                                          |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| [main.ts](src/main.ts)               | Application operations with services supplied at the boundary                                                 |
| [Greeting.ts](src/Greeting.ts)       | `Workflow.make`, an Activity and an ordinary `toLayer` handler                                                |
| [Uppercase.ts](src/Uppercase.ts)     | A native AI Tool, Toolkit handler and explicit recovery policy                                                |
| [DemoModel.ts](src/DemoModel.ts)     | An offline LanguageModel that responds to tool results in its prompt                                          |
| [Application.ts](src/Application.ts) | Session, native engine and durable executor Layers; `layerNoDeps` accepts the caller's catalogue and registry |
| [Database.ts](src/Database.ts)       | Configured SQLite resources and a scoped temporary directory                                                  |

The model requests `uppercase`, receives `HELLO` from the tool and emits the answer. The handler reports progress through `Invocation.ToolCall`. Its replay policy is safe because uppercasing a string has no external side effect. All built-in Workflow executor Layers are registered; this path exercises submission, generation and tool execution.

## Verify recovery across processes

The default database lives in a scoped temporary directory and is removed on shutdown. To retain it, create a writable parent directory and choose an absolute filename:

```sh
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun run --cwd apps/example test
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun run --cwd apps/example test
```

The second process replays request `uppercase-v1` without calling the local model or tool. Use a fresh database to run that path again.

A shared native SQLite client supplies both the ClusterWorkflowEngine and Effect's SQL KeyValueStore/EventJournal Layers. SnapshotStore receives the latter services for domain state; Effect owns the SQL tables and coordination. `runnerStorage: 'memory'` controls runner membership, while native messages and Activity results still persist in SQLite. Built-in Activities use domain receipts and ordinary native replay.

Session construction retains `Conversation.layer(options)` for shared configuration, initialization and recovery. Platform Layers supply filesystem, path and crypto services. Scope closure pauses recoverable work, joins finalizers and releases the engine and database. It does not produce an Abort receipt.

## Continue

[Persist conversations](../../docs/persistence.md) adapts this storage composition to a Node application. [Compose native Workflows](../../docs/workflows.md) adds custom jobs, and [replay and recovery](../../docs/explanation/recovery.md) explains the commit boundaries exercised here.
