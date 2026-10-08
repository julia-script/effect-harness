# Persist conversations

Supply a `Persistence` Layer to `Harness.layer`. The core service reads individual records, exposes scans as Streams and commits batches atomically. Task checkpoints, transcript entries, submissions and document changes use that same batch boundary.

Use `storage/Memory.layer` for process-local experiments. Use `storage/SqliteBun.layer({ filename })` or `storage/SqliteNode.layer({ filename })` for indexed persistent records. The SQLite driver remains inside the adapter; application code does not need SqlClient to use the harness.

`storage/JsonlBun.layer({ directory })` and `storage/JsonlNode.layer({ directory })` store framed commits in `commits.jsonl`, rebuild indexes on opening and use a separate ownership lock. Their filesystem-sync and tail-recovery behavior is described in [the storage reference](reference/documents-and-storage.md).

The [example database Layer](../apps/example/src/Database.ts) reads a configured filename and composes the Bun adapter. Its owning Scope acquires and releases both the database and optional temporary directory.

Opening reconciles interrupted saved tasks. Call `Harness.resume` when the host is ready to start that work. New submissions start normally. Closing interrupts live invocations after sealing scheduling; it leaves unfinished committed tasks recoverable.

Each store has one owning harness process. An application exposing remote clients routes their requests to that owner. Tool workspaces and remote resources have their own lifetimes; retaining the database does not recreate them.
