# Keep state across process runs

Build the [offline example](first-conversation.md) first. By default it uses a temporary database that disappears when its owning Scope closes.

## Select a persistent database

Run the compiled application twice with the same filename:

```sh
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun apps/example/dist/main.js
EXAMPLE_DB=/tmp/effect-harness-example.sqlite bun apps/example/dist/main.js
```

Both runs print the same answer. The second run reopens the saved conversation, submission and task results. The application calls `Harness.resume` to make unfinished tasks eligible; completed tasks remain completed.

## Follow an application document

[Greeting.ts](../../apps/example/src/Greeting.ts) declares `RunDoc`, a typed conversation document with an optional task ID. [main.ts](../../apps/example/src/main.ts) admits the greeting task and saves that ID in one transaction.

On the next process run, the document supplies the existing ID. The application waits for that task instead of admitting another greeting. The task itself commits a `greet` checkpoint before completing with its string result.

The persistent adapter owns database access. The harness depends on its record-oriented `Persistence` service. See [documents and storage](../reference/documents-and-storage.md) for document scopes and transaction rules, and [recovery](../explanation/recovery.md) for interrupted work.
