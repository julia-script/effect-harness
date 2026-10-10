# Storage, sessions, and documents

`Storage` owns durable records and atomic write batches. Choose one implementation:

```ts
Storage.layerMemory
Storage.layerSql
Storage.layerJsonl({ filePath: './agent.jsonl' })
```

Memory requires no services. SQL requires Effect's `SqlClient`; JSONL requires `FileSystem`. The library does not select a SQL implementation or host runtime. Initialization and cleanup follow the supplying Layer's lifecycle. JSONL commits require a final newline; incomplete trailing bytes are discarded on reopening.

SQL initialization and mutations (`Storage.commit`, `Storage.mintId`, and Session commits that write) must run outside the supplying client's `SqlClient.withTransaction`. They fail with an `invalid` StorageError inside an ambient transaction: a savepoint cannot guarantee durability before its outer transaction settles. This rejection performs no storage write and does not poison Storage or Session; retry outside the application transaction.

SQL transaction-control failures (including COMMIT and ROLLBACK failures) report an `uncertain` StorageError. Stop using that Storage instance and reopen it to reconcile the durable state before retrying; the failed write may have committed. Session also seals itself on an uncertain commit.

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
