# Storage, sessions, and documents

Memory, JSONL and SQLite storage use the same state engine.
For readable files:

```ts
Storage.layerJsonl({ filePath: './agent.jsonl' })
```

Supply Effect's `FileSystem` from the host, for example `NodeServices.layer`.
For SQLite, supply an Effect SQL client from the host:

```ts
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Layer from 'effect/Layer'
import * as Storage from 'effect-harness/Storage'

const StorageLive = Storage.layerSql({ storeId: 'demo' }).pipe(
  Layer.provide(SqliteClient.layer({ filename: './agent-spike.sqlite' })),
)
```

`storeId` defaults to `default`; distinct IDs select independent stores in one database.
The SQL adapter currently supports SQLite only, using the Node or Bun Effect driver.
For volatile state, use `Storage.layerMemory` (a Layer value, with no arguments).
It requires no platform services and stores opaque commits and payload streams
in Effect `Chunk` and `HashMap` values held by a `Ref`. Each fresh Layer construction
starts empty. Reusing the built context keeps state across harness restarts while
its storage scope remains open; closing that scope ends access. Page reloads and
process restarts lose all memory state. No filesystem or database is involved.

The EventJournal storage Layer has been removed.
The provider-configuration and credential services are separate from conversation storage.

## Responsibility boundary

`Kernel` applies record operations and enforces the Storage API's atomic batches.
`Projection` holds the current rows in an Effect `HashMap` inside a `Ref`.
`Store` owns the commit codec, format checks, replay and retention decisions.
`Memory`, `Jsonl` and `Sql` implement the opaque persistence port: read a log, read named payload
streams, durably append a batch, and replace superseded payloads. None imports
conversation, task, document or record schemas.

The SQL adapter stores the same opaque JSON strings in two tables:

| Table                        | Key                              | Value          |
| ---------------------------- | -------------------------------- | -------------- |
| `effect_harness_commits_v1`  | `store_id`, `position`           | Commit `frame` |
| `effect_harness_payloads_v1` | `store_id`, `stream`, `position` | Payload `line` |

Keys are non-null, positions are positive integers, and primary keys prevent duplicates.
A batch appends its payloads and commit frame in one SQL transaction. Reclamation
replaces a stream in a separate transaction after the commit; a failed replacement
rolls back its deletes and is retried on reopening. The host owns database configuration
and backup policy. There are no domain tables or SQL-specific replay rules.

Persistent adapters replay into memory on opening; memory storage starts empty. SQL does not yet add indexed domain
queries, incremental loading, or distributed coordination.

## Files you can inspect

`agent.jsonl` has one complete JSON object per line:

```json
{
  "format": 1,
  "type": "commit",
  "seq": 1,
  "metadata": { "format": 1, "nextId": 2, "nextSeq": 2 },
  "writes": [{ "type": "conversation", "value": { "id": 1 } }]
}
```

Messages, submissions, conversations and completed tasks are inline under `writes`.
`agent.jsonl.parts/doc-<id>.jsonl` holds document revisions;
`task-<id>.jsonl` holds active-task checkpoints. A payload's `seq` and `ordinal`
identify the commit operation that published it. `document.create`,
`document.change` and `task.sidecar` in the main log refer to these payloads.
This follows Pi Durable's layout, but it is not a compatible interchange format:
our record schemas and document content codecs remain different.

New document revisions are written once. Current-only documents discard replaced
bases after a durable commit. Rewindable history is retained. Completed tasks move
inline and their old checkpoint streams are deleted. Document retirement keeps
its metadata; current-only content is reclaimed. Reclamation failures are retried
on reopening and do not turn an already committed batch into a failure.

The main log still grows with commits; this spike does not add whole-store
snapshots or main-log compaction. Reads scan the local projection; secondary
indexes remain future work.

## Recovery and ownership

SQL also requires one active writer per database/store ID. A head check rejects stale
writers before append or reclamation; it does not coordinate multiple harnesses.
Use storage outside application SQL transactions, including during construction and
ID allocation. Those contexts are rejected so a harness write can only be acknowledged
after its own database commit, never while an outer transaction could still roll back.
Failed or unacknowledged appends seal the writer until it is closed and reopened.
Existing legacy SQL or EventJournal tables are left untouched, with no automatic migration.

For JSONL:

Own one writer per path across processes and runtime instances. Ownership is
the host application's responsibility; the adapter does not provide a lease or lock. Keep the main
log and its `.parts` directory together when backing up a closed store.

The adapter syncs appended payloads and their directory before appending and
syncing the commit marker. The state engine publishes the in-memory update only
after that succeeds. An uncertain append seals the writer; close and reopen it
before retrying. Scope closure drains admitted writes before closing the persistence scope.

Recovery validates all commit headers and live payloads before truncating torn
tails or reclaiming orphan data. An incomplete trailing line is not a commit.
Missing committed payloads and corrupt committed lines fail recovery.

ID allocation alone performs no I/O. IDs become durable in a commit's metadata;
an allocation made after the last successful commit may be reused after restart.
Never use an uncommitted ID as a durable external identity.

This format intentionally rejects the previous `sidecars-v1` files. Keep existing
stores separately; there is no migration in this spike.

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
