# Embedded harness example

The [conversation and task tour](TOUR.md) adds ten runnable examples: recovery, forks, extensions, subagents, checkout, reminders, context, documents and shared observations. Build once, then run `bun run --cwd apps/example tour -- all` from the repository root.

This offline application composes a native Effect AI model, an uppercase Toolkit, checkpoint tasks and SQLite persistence. Its integration test opens the same database twice and verifies that saved model, tool and greeting results are reused.

From the repository root:

```sh
bun run build
bun run --cwd apps/example test
```

Expected output:

```text
Hello, Effect
HELLO
embedded-harness-example-ok
```

Without `EXAMPLE_DB`, the application creates a scoped temporary database. To retain a conversation between process runs:

```sh
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun apps/example/dist/main.js
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun apps/example/dist/main.js
```

`main.ts` reuses the stable `uppercase-v1` request identity. Its greeting document saves the custom task identity in the transaction that admits the task. Reopening calls `Harness.resume` to start unfinished saved work; completed work supplies its stored outcome.

`Application.ts` composes `Harness.layer`, `Executor.layer` and the application-owned model, registry and database Layers. `Database.ts` selects `storage/SqliteBun`. `Greeting.ts` declares a two-phase task. `DemoModel.ts` and `Uppercase.ts` supply native model and Toolkit boundaries.

The unknown-tool reproduction retains native Effect AI validation:

```sh
bun run --cwd apps/example reproduce:unknown-tool
```

The model asks for `upper_case`, while the registry offers `uppercase`. Native validation rejects that response, and the harness settles the failed attempt. The `--retry` option exercises the configured retry policy.

## Portable runtime example

[Portable.ts](src/Portable.ts) combines the eight in-memory tour scenarios. It imports the portable package namespaces, declares a result schema, and uses no platform Layer or host runtime globals. [PortableMain.ts](src/PortableMain.ts) runs that effect using the same entrypoint in each runtime.

From the repository root, after `bun run build`:

```sh
node apps/example/dist/PortableMain.js
bun apps/example/dist/PortableMain.js
```

For Deno, bundle the workspace imports first. This avoids requiring Deno to resolve Bun's workspace catalog aliases:

```sh
bun build --target=browser --outfile=apps/example/dist/portable.browser.js apps/example/dist/PortableMain.js
deno run --no-config --no-prompt apps/example/dist/portable.browser.js
```

The same bundle is a browser ES module; load it from a module script or worker. All eight scenarios use memory persistence, local models and simulated external actions. Their close/reopen demonstrations retain the same memory service inside one process.

CI runs the compiled package with Node, builds a browser bundle and checks it without Node/Bun globals, and runs the bundle with Deno. Runtime-specific persistent adapters remain separate leaf imports, and require host capabilities suitable for that adapter.
