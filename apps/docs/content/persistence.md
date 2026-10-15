# Preserve conversations across restarts

Use persistent storage so a new runtime can reopen conversation history and unfinished work. This guide assumes a local Node application, a model Layer, and compatible tool handlers.

## Supply SQLite storage

Install the Node adapters with the same Effect version as your application:

```sh
bun add @effect/platform-node@4.0.1 @effect/sql-sqlite-node@4.0.1
```

The complete recovery program below uses the example application's offline model and tool handlers, composed by `Application.layer`. Run it from the repository after building the example app.

<!-- example:recovery -->

## Reopen the same store

Run the recovery example from the repository root:

```sh
bun install
bun run build
bun run --cwd apps/example recovery
bun run --cwd apps/example recovery
```

Both launches use the same database path relative to `apps/example`. The second launch finds the original submission through `job-42`; it does not add another copy of the input. Keep the database file and compatible models and handlers available for subsequent launches.

Use one active local runtime per store. Shut down the old runtime before opening a replacement. Accessing the root automatically starts pending work.

## Persist application documents

Declare a `Document` with a schema, initial value, and scope. Inside a tool handler, use `ToolExecution.commit` to stage changes with `Transaction.ensureDocument` and `Transaction.updateDocument`. The [complete document example](/examples/documents) updates a counter and appends an entry in one transaction.

When using SQL, perform storage initialization and writes outside an application `SqlClient.withTransaction`. If a commit reports an uncertain storage error, stop using that instance and reopen the store before reconciling the outcome.

See [storage layers](/reference/storage) for adapter requirements and [recovery](/explanation/recovery) for the limits around external side effects.

## Image persistence and backups

Built-in stores share image bytes across retained requests, entries and document revisions while returning hydrated values. Native prompt bytes and URL objects survive JSON encoding through `effect-harness/PromptWire`; custom adapters may remain inline if they honor the runtime-12 logical contract. Current stores require format 12/12, SQL persistence schema 3 or JSONL sidecars-v2. Older stores are rejected without migration.

A closed JSONL backup must include the journal and both adjacent `.parts` and `.media` directories. SQL objects and owner manifests live in the database. Historical entries and rewindable revisions retain their images even when omitted from the current context; only removal of the final retained owner permits reclamation.
