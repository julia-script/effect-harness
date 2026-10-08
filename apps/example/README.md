# Embedded harness example

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
