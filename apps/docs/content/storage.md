# Storage layer reference

`effect-harness/Storage` owns durable records and atomic write batches. This page covers the three built-in Layer constructors, their inputs, environment requirements, and lifecycle constraints.

## layerMemory

`Storage.layerMemory` provides `Storage` and `StorageFormat` without platform services. Records live for the lifetime of the acquired store. A browser reload or process exit discards them.

Two runtimes can reopen the same acquired store sequentially while its owning scope remains alive. Independently acquiring the Layer creates an independent store.

## layerJournal

`Storage.layerJournal({ storeId? })` provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires `EventJournal.EventJournal` from `effect/eventlog/EventJournal`. `storeId` is a nonempty string, defaults to `default`, and identifies an independent store within the journal.

The application supplies the journal Layer. For SQLite, provide Effect's `SqlEventJournal.layer()` and then your SQLite client. Effect's memory journal is suitable for ephemeral storage. The harness contains no SQL queries.

Each atomic batch, ID reservation and format upgrade is stored as one event. Opening validates a revision/predecessor chain and rebuilds the local projection. Event timestamps and backend return order do not determine commit order. Reads use local state; startup reads the full journal, without checkpointing or compaction.

Own one runtime per store ID, across processes and browser tabs as well as within a process. The adapter rejects duplicate owners of the same injected journal service and store ID. It cannot detect independent journal services targeting the same backend. Do not write or replicate events into an active store externally.

An append failure seals storage with an `uncertain` error. Close its scope and reopen to reconcile the durable result. Session seals itself on an uncertain commit. Scope cleanup waits for an admitted append before releasing ownership.

The journal must settle durable writes before returning. With SQL journals, supply a dedicated client and construct and invoke storage outside ambient application transactions; the generic journal interface cannot detect them. Durability otherwise depends on the journal implementation and backend configuration.

Effect 4.0.1's stock IndexedDB journal acknowledges request success before transaction completion. A subsequent abort can lose an acknowledged commit, so it does not satisfy this adapter's durability contract. Use an IndexedDB implementation that waits for transaction completion and reports aborts.

`layerJournal` replaces `layerSql`. Existing custom SQL tables are not imported into the new journal format.

## layerJsonl

`Storage.layerJsonl({ filePath })` provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires `FileSystem.FileSystem`.

| Field      | Type   | Required |
| ---------- | ------ | -------- |
| `filePath` | string | Yes      |

The writer owns an append-only journal. Commit frames require a final newline; incomplete trailing bytes are discarded on reopen. One writer owns a file, including across processes. Scope cleanup releases the writer.

## Ownership

One active local runtime coordinates each store. Multiple independent active runtimes must not share the same persisted store. The storage layer does not provide a distributed scheduler or recover the external resources used by tools.

[Preserve conversations across restarts](/guides/persistence) supplies a SQLite composition. [Client and runtime](/explanation/composition) describes the service boundaries.
