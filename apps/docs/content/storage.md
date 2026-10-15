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

## Shared image storage

All built-in adapters store each unique image payload once per storage namespace, shared across conversations, forks, requests and canonical tool results. SHA-256 hashes raw bytes without decoding or recompressing pixels. Native bytes, canonical base64 and equivalent data URLs share one binary object; filename, MIME type, provider options and the original representation remain attached to each occurrence. Other image strings retain their exact spelling in a separate literal domain. External URLs remain references and are never fetched.

`Storage` reads, prepared requests and Session snapshots remain hydrated and self-contained. `ContextProjection.messages(entry)` needs no media service, and returned values remain usable after storage closes. Mutable bytes reconstructed by the prompt codec belong to the caller. Arbitrary JSON such as tool arguments, application documents, details, structured output and provider options is not interned. The native nested-result `valueKind: "content"` channel is a deliberate exception.

Objects stay alive while any retained physical row or document revision references them. Context omission, head changes, compaction and task completion alone do not delete historical images. Pruning current-only revisions or retiring their final owner can reclaim images; rewindable `asOf` history still owns its images after retirement. There is no conversation purge operation.

Native stores require runtime container/minimum-reader **12/12**. SQL uses persistence schema **3**, JSONL uses **sidecars-v3**, and physical packed bodies use version **1**. Earlier containers are rejected before replay or mutation; there is no migration or old prompt-wire fallback.

## Custom adapters

Existing adapters may continue storing hydrated records inline. They must implement the runtime-12 `StorageFormat` contract; shared-object deduplication is not automatic. Use `PromptWire` codecs at typed native prompt boundaries before JSON serialization to preserve `Uint8Array` and `URL` values. Wire controls are interpreted only inside file-data positions; marker-shaped application JSON remains ordinary data.

`StorageMedia.pack(value, context)` returns a versioned `packed` body and distinct `{ digest, bytes }` objects. The context identifies a record category, or a document kind with its preceding materialization for delta updates. `StorageMedia.hydrate(packed, context, read)` verifies and restores the JSON body. `PackedBodySchema`, `ReferenceSchema`, `manifest`, `verify` and `sameObject` support durable validation and exact-byte collision checks. Failures use `StorageError` operations such as `media.read` and `media.integrity`.

An adapter must atomically publish records, object bytes and distinct owner/digest relationships, and coordinate reads with reclamation. Every retained revision is an owner, including historical revisions. Manifest completeness needs an independent durable record (JSONL binds each retained owner manifest to a SHA-256 checksum in its commit marker; SQL cross-checks owner rows). Structural `manifest` validation alone cannot prove that no references were omitted. Manifests are the recoverable authority; reference counts may accelerate reclamation. Validate ownership without requiring malformed temporary document bodies to decode. A generic wrapper cannot provide this atomicity around an arbitrary Storage implementation.

## JSONL backups and recovery

Close the writer, then copy the journal, `<filePath>.parts` and `<filePath>.media` together. A journal or reference-only export without its object closure is incomplete. SQL images and ownership tables are part of the same database backup.

JSONL syncs new objects and directory entries, then row/revision sidecars, before appending and syncing the commit marker. It removes obsolete files only after publication. Reopening validates the selected journal prefix, every retained ownership checksum, manifests and required objects, syncs that prefix, then removes orphans. Missing or corrupt required images fail the open. An uncertain write revokes the writer until reopen. Memory publishes packed rows and their object closure in one immutable state update. SQL publishes all changes in the metadata-locked transaction and reads rows with their objects in one query.
