# Storage layer reference

`effect-harness/Storage` provides atomic write batches through `Storage.layerMemory`, `Storage.layerJsonl({ filePath })` and `Storage.layerSql({ storeId })`. All three use the same state engine and commit format; JSONL and SQL persist across process restarts.

## layerMemory

A Layer value with no arguments. Provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires no platform services. Commits and payload streams are held in Effect `Chunk` and `HashMap` values inside a `Ref`.

Each fresh Layer construction starts empty. Effect's normal Layer memoization shares the service within a build; use `Layer.fresh(Storage.layerMemory)` when you need an independent instance. Reuse a built storage context to keep state across harness restarts in one process. Closing the storage scope ends access, and a new build does not recover its data. Browser reloads and process restarts lose the state.

Memory uses the same validation, atomic batches, history, format markers and retention rules as the persistent adapters. It is suitable for tests and temporary browser sessions.

## layerJsonl

The Layer provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires Effect's `FileSystem.FileSystem`. Supply it from the host, for example with `NodeServices.layer`.

| Field      | Type   | Required |
| ---------- | ------ | -------- |
| `filePath` | string | Yes      |

Create the parent directory before opening the store. The main JSONL file contains one commit per line, with messages, submissions, conversations and completed tasks inline. An adjacent `<filePath>.parts` directory holds `doc-<id>.jsonl` revision streams and `task-<id>.jsonl` active checkpoints. Keep these files together when copying a closed store.

This follows Pi Durable's layout, but uses the harness's own schemas. Previous harness JSONL files are not migrated by this spike.

## layerSql

The Layer provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires `effect/sql/SqlClient`. Currently only SQLite clients are supported; supply `SqliteClient.layer({ filename })` from `@effect/sql-sqlite-node` or `@effect/sql-sqlite-bun`.

| Field     | Type            | Default   |
| --------- | --------------- | --------- |
| `storeId` | nonempty string | `default` |

Distinct store IDs share a database without sharing state. The adapter creates `effect_harness_commits_v1` and `effect_harness_payloads_v1`; they store the same opaque JSON strings as JSONL, with keys for store, stream and position. One transaction appends the payloads and commit frame. Reclamation replaces a payload stream in a separate transaction.

Keep one active writer per database/store ID. A stale-head check rejects writes from an outdated instance, but does not coordinate multiple runtimes. Call storage outside application SQL transactions; these contexts are rejected before mutation so an outer rollback cannot undo an acknowledged harness write. The host owns SQLite configuration and backups. Legacy SQL and EventJournal tables are left untouched; no migration is performed.

## Recovery and ownership

One active local runtime coordinates each store. The host must ensure one writer per file across processes; the adapter does not acquire a distributed lease or file lock.

For JSONL, payload streams are synced before their commit marker. Commit frames require a final newline; incomplete trailing bytes are discarded only after the committed prefix and live payloads have been validated. Missing committed payloads fail recovery.

An uncertain append seals storage. Close its scope and reopen to reconcile the durable result. Scope cleanup drains admitted writes before releasing storage resources. ID allocation alone performs no I/O, so IDs allocated after the last commit may be reused after reopening.

Current-only documents reclaim replaced content, and completed tasks discard their old checkpoint streams. Rewindable document history is retained. The main log is not compacted. Persistent adapters replay their log on startup into a local Effect `HashMap`; all three adapters read from this projection.

## Persistence boundary

The shared store owns encoding, replay and retention. The adapters read, append and replace opaque lines in named streams. JSONL provides filesystem synchronization; SQL provides database transactions. No adapter implements conversation, task or document rules.

The EventJournal storage Layer has been removed. The interactive browser lab uses memory storage for the lifetime of the page.

[Preserve conversations across restarts](/guides/persistence) supplies JSONL and SQLite compositions. [Client and runtime](/explanation/composition) describes the service boundaries.
