# Storage layer reference

`effect-harness/Storage` owns durable records and atomic write batches. This page covers the three built-in Layer constructors, their inputs, environment requirements, and lifecycle constraints.

## layerMemory

`Storage.layerMemory` provides `Storage` and `StorageFormat` without platform services. Records live for the lifetime of the acquired store. A browser reload or process exit discards them.

Two runtimes can reopen the same acquired store sequentially while its owning scope remains alive. Independently acquiring the Layer creates an independent store.

## layerSql

`Storage.layerSql` provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires `SqlClient.SqlClient`. The caller supplies the database adapter and connection configuration.

Initialization and mutating operations reject execution inside the supplying client's ambient transaction with an `invalid` storage error. Transaction-control failures report an `uncertain` storage error; that store must be reopened to reconcile the durable result. The corresponding Session seals itself on uncertain commits.

Persistence and failure tolerance depend on the supplied SQL adapter and database configuration.

## layerJsonl

`Storage.layerJsonl({ filePath })` provides `Storage` and `StorageFormat`, can fail with `StorageError`, and requires `FileSystem.FileSystem`.

| Field      | Type   | Required |
| ---------- | ------ | -------- |
| `filePath` | string | Yes      |

The writer owns an append-only journal. Commit frames require a final newline; incomplete trailing bytes are discarded on reopen. One writer owns a file, including across processes. Scope cleanup releases the writer.

## Ownership

One active local runtime coordinates each store. Multiple independent active runtimes must not share the same persisted store. The storage layer does not provide a distributed scheduler or recover the external resources used by tools.

[Preserve conversations across restarts](/guides/persistence) supplies a SQLite composition. [Client and runtime](/explanation/composition) describes the service boundaries.
