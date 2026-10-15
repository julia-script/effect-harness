# Storage, sessions, and documents

`Storage` owns durable records and atomic write batches. Choose one implementation:

```ts
Storage.layerMemory
Storage.layerJournal({ storeId: 'agent' })
Storage.layerJsonl({ filePath: './agent.jsonl' })
```

Memory requires no services. Journal storage requires Effect's `EventJournal`; JSONL requires `FileSystem`. The library does not select a database implementation or host runtime. Initialization and cleanup follow the supplying Layer's lifecycle. JSONL commits require a final newline; incomplete trailing bytes are discarded on reopening.

For SQLite, compose `Storage.layerJournal()` with `SqlEventJournal.layer()` from `effect/eventlog/SqlEventJournal`, then provide the application's SQLite client. Effect's memory journal can be supplied for ephemeral storage. `storeId` defaults to `default` and must remain stable across restarts. Different store IDs have independent records and allocation counters.

One runtime owns each journal store, including across processes and browser tabs. Duplicate owners of the same injected journal service and store ID are rejected. Separate journal instances pointing at the same backing store require application-level ownership. External writes and event replication into an active store are unsupported; reopen to rebuild its projection.

Every atomic batch, ID reservation and format upgrade is one journal event. Events carry an explicit revision and predecessor, so replay does not depend on timestamps or backend ordering. Reopening validates the chain, reader requirements and counters before serving state. Reads use the in-memory projection. Startup currently reads the full journal, with no checkpointing or compaction; memory use grows with retained records and document history.

The supplied journal must acknowledge writes independently of application transactions. With `SqlEventJournal`, use a dedicated client and construct and invoke storage outside `SqlClient.withTransaction`. Storage cannot detect backend-specific ambient transactions through the generic journal interface.

Effect 4.0.1's stock IndexedDB journal is unsuitable for durable storage here: it acknowledges the object-store request before the transaction completes, so a later abort can lose an acknowledged commit. An IndexedDB implementation must wait for transaction completion and report aborts before it can satisfy this adapter's contract.

Append failures report an `uncertain` StorageError and seal that Storage instance. Close its scope and reopen it before retrying: the failed append may already be durable. Session also seals itself on an uncertain commit. Scope cleanup settles an admitted append before releasing ownership.

This replaces `Storage.layerSql` and its custom tables. Existing SQL containers are not automatically imported. JSONL remains an independent format and adapter.

`Session` is a scoped instance over Storage, shared by all conversations in that local runtime. It captures the storage service, caches decoded documents, serializes transactions, and publishes committed changes. A session can be used independently of an agent runtime:

```ts
const session = yield * Session.make()

yield *
  session.pipe(
    Session.commit((tx) =>
      Effect.gen(function* () {
        const conversation = yield* Transaction.ensureRoot(tx)
        yield* Transaction.appendEntry(tx, conversation.id, {
          kind: 'app.note',
          data: { text: 'Created' },
        })
      }),
    ),
  )
```

`Session.make()` requires `Storage` and `Scope`; it performs no storage I/O. One active Session coordinates a Storage instance. Scope cleanup seals admission and settles commits before subscriptions end.

`Document.define` declares a singleton document with a bidirectional schema. `Document.family` declares keyed members. A target carries an explicit session, conversation, or task scope and an optional family key.

Use `Transaction.ensureDocument`, `setDocument`, or `updateDocument` inside `Session.commit`. Updates produce new values rather than mutating shared objects. Schema encoding and decoding handle the boundary between runtime values and stored JSON. Callback failure discards staged work; successful batches atomically persist entries and document changes.

Conversation documents declare history (`latest` or `rewindable`) and fork behavior (`current`, `initial`, or `asOf`). Only rewindable documents support `asOf`. Session and task documents do not declare these conversation policies.

The local `HarnessRuntime` owns its Session. Tools access transactions through `ToolExecution.commit`. Applications read through `Conversation.snapshot` and observe through `Conversation.watch`; transaction callbacks remain on the runtime side of the backend boundary.

See the [document example](../apps/example/src/tour/Documents.ts).
