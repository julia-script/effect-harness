# Native Workflow example

For Effect v4 library developers and application integrators. [src/main.ts](src/main.ts) imports the public package exports, implements a native LanguageModel, binds a native AI Toolkit handler, and registers every built-in durable executor. It also declares a custom Workflow with `Workflow.make` and registers its ordinary `toLayer` handler.

From the repository root:

```sh
bun install
bun run --cwd apps/example typecheck
bun run --cwd apps/example build
bun run --cwd apps/example test
```

Success prints `native-workflow-example-ok`. Typecheck and build first compile the two library dependencies, so the example checks their public declaration files rather than test-only source aliases. The test runs compiled JavaScript. No account, network inference or model download is required.

`bun install` applies the required [Effect rc.118 patch](../../patches/effect@4.0.0-rc.118.patch) via root `patchedDependencies`. Its native Cluster repair waits for recovered Activity readiness before acquiring SQL storage transactions, preserving native transactional body/reply storage. The example checks normal completion/replay; the [SIGKILL fixture](../../packages/durable/test/restart/NativeRestart.test.ts) covers the recovered SQL-prefix/unfinished-transaction window.

The patch also adds native AI unknown-call opt-in behavior. The example executes native generation and streaming checks: undeclared tool calls fail by default, the opt-in alone does not bypass validation, and `allowUnknownToolCalls: true` together with `disableToolCallResolution: true` preserves an undeclared name and its parameters. The probe uses dynamic tool names and `Schema.Unknown` parameters so its public response types permit unknown names and payloads honestly. Ordinary example tools retain their precise schemas. The harness owns settlement under those flags. An external unpatched Effect install cannot provide these patched behaviors; see [the public contract map](../../docs/parity.md).

The model requests one `uppercase` tool call and then emits `HELLO`. The handler uses `Invocation.ToolCall` to publish progress; its replay policy is `safe` because uppercasing a string has no external side effect. The test checks successful submission settlement, ordinary `Conversation.awaitIdle`, native `poll`, native `resume`, stable submission identity, and unchanged invocation counts on replay. All five built-in Workflow executor Layers are registered, although this example exercises submission, generation and tool execution directly.

Persistence is real: a native `SqliteClient`, `SingleRunner` and `ClusterWorkflowEngine` store native messages and workflow activity results alongside the harness SQL domain Store in one database. `runnerStorage: 'memory'` controls runner membership; it does not make the SQL messages ephemeral. The default database lives in an automatically removed scoped temporary directory.

To retain the database, create a writable parent directory and choose an absolute filename:

```sh
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun run --cwd apps/example test
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun run --cwd apps/example test
```

The second process replays the fixed request `uppercase-v1` without calling the fake model or tool. Choose a fresh database to execute the model/tool path again. The database also contains native cluster tables; keep native engine and domain migrations compatible when upgrading dependencies. The [restart tests](../../packages/durable/test/restart/HarnessRestart.test.ts) exercise killed-process recovery; this example checks ordinary completion and fresh-process replay.

Layer composition retains the actual requirements. The executor Layer consumes a SessionDirectory, conversation Configuration, model Catalogue, generic harness Executor and native WorkflowEngine. The SQL domain Store consumes the same SqlClient that the native cluster uses. Bun services satisfy filesystem, path and crypto requirements at the application boundary. The program closes the session, engine and temporary directory through Effect scopes.

Configuration construction validates settings and has a typed `SchemaError` channel. Session construction captures the creation Layer, including native Crypto for legacy provider-affinity repair before requests. Session closure pauses recoverable work and joins finalizers before physical backend cleanup; it does not create an abort outcome.
