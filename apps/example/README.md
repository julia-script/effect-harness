# Native Workflow example

For Effect v4 library developers and application integrators. Start with [src/main.ts](src/main.ts): it selects the agent's model, executes a custom Workflow, submits a conversation input through the native Submission Workflow, and prints the committed answer. The services are supplied once at the application boundary.

The other modules each show one part of that composition:

- [Greeting.ts](src/Greeting.ts) declares a custom Workflow with `Workflow.make` and registers its ordinary `toLayer` handler.
- [Uppercase.ts](src/Uppercase.ts) declares a native AI Tool, implements its Toolkit handler and binds its replay policy into the harness registry.
- [DemoModel.ts](src/DemoModel.ts) supplies an offline native LanguageModel and its catalogue entry. It responds to tool results in the prompt; it has no mutable turn counter.
- [Application.ts](src/Application.ts) composes the Session, native SQL Workflow engine and durable executor Layers. Its `layerNoDeps` accepts a caller-supplied catalogue and registry.
- [Database.ts](src/Database.ts) reads configuration and scopes the SQLite client and optional temporary directory.

From the repository root:

```sh
bun install
bun run --cwd apps/example typecheck
bun run --cwd apps/example build
bun run --cwd apps/example test
```

Success prints `Hello, Effect`, `HELLO` and `native-workflow-example-ok`. Typecheck and build first compile the two library dependencies, so the example checks their public declaration files rather than test-only source aliases. Tests run under Bun, import the compiled application and use actual package exports without source aliases. The test command also runs the compiled entrypoint. No account, network inference or model download is required.

`bun install` applies the required [Effect patch](../../README.md) for native Cluster Activity recovery and AI unknown-call handling.

The model requests one `uppercase` tool call and then emits `HELLO`. The handler uses `Invocation.ToolCall` to publish progress; its replay policy is `safe` because uppercasing a string has no external side effect. All five built-in Workflow executor Layers are registered, although this example exercises submission, generation and tool execution directly.

Persistence is real: a native `SqliteClient`, `SingleRunner` and `ClusterWorkflowEngine` store native messages and workflow activity results alongside the domain Store in one database. The domain Store uses `SnapshotStore.layer`, supplied with Effect's SQL KeyValueStore and EventJournal Layers; Effect owns their tables, CRUD and coordination. Built-in Activities use replay receipts without shared transactions. `runnerStorage: 'memory'` controls runner membership; it does not make the SQL messages ephemeral. The default database lives in an automatically removed scoped temporary directory.

To retain the database, create a writable parent directory and choose an absolute filename:

```sh
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun run --cwd apps/example test
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun run --cwd apps/example test
```

The second process replays the fixed request `uppercase-v1` without calling the fake model or tool. Choose a fresh database to execute the model/tool path again. The database also contains native cluster tables. This example now uses the Effect key/value snapshot format; choose a fresh database when upgrading from the old example, whose retired domain format requires an explicit migration using the previous checkout. See [storage formats](../../docs/persistence.md) before switching adapters.

Layer composition retains the actual requirements. The executor Layer consumes a SessionDirectory, conversation Configuration, model Catalogue, generic harness Executor and native WorkflowEngine. The domain Store consumes KeyValueStore and EventJournal. The application builds their native SQL Layers from the same database client. Bun services satisfy filesystem, path and crypto requirements at the application boundary. The program closes the session, engine and temporary directory through Effect scopes.

Configuration construction validates settings and has a typed `SchemaError` channel. Session construction captures the creation Layer, including native Crypto for legacy provider-affinity repair before requests. Session closure pauses recoverable work and joins finalizers before physical backend cleanup; it does not create an abort outcome.
