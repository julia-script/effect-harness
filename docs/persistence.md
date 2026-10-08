# How to persist conversations across restarts

Use this guide to keep conversation state and native execution history when an application process restarts. Start with a working registration graph, such as [the first conversation tutorial](tutorials/first-conversation.md).

A recoverable application needs persistent storage for **both** the domain Store and the WorkflowEngine. This guide uses Effect's SQLite-backed persistence services and ClusterWorkflowEngine in one application process.

## 1. Install the SQLite adapter

```sh
npm install effect-harness effect @effect/platform-node @effect/sql-sqlite-node
mkdir -p data
```

This recipe uses the Node SQLite adapter, which requires a Node.js runtime with `node:sqlite`. For another runtime, choose its native SQLite and platform Layers.

Keep `data/` on a volume that survives process replacement. Built-in executors use ordinary native Activities and domain receipts. Review [native Workflow recovery](reference/compatibility.md#native-workflow-recovery) before adding transaction-annotated Activities of your own.

## 2. Build shared persistence Layers

Create a module with one database Layer value shared by the domain primitives and native engine:

```ts
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import * as ClusterWorkflowEngine from 'effect/cluster/ClusterWorkflowEngine'
import * as SingleRunner from 'effect/cluster/SingleRunner'

const Database = SqliteClient.layer({ filename: './data/harness.sqlite' })
const Primitives = Layer.mergeAll(KeyValueStore.layerSql(), SqlEventJournal.layer()).pipe(
  Layer.provide(Database),
)

export const StoreLive = SnapshotStore.layerWith({ key: 'app/session/main' }).pipe(
  Layer.provide(Primitives),
)
export const EngineLive = ClusterWorkflowEngine.layer.pipe(
  Layer.provide(SingleRunner.layer({ runnerStorage: 'memory' })),
  Layer.provide(Database),
)
export const Infrastructure = Layer.mergeAll(StoreLive, EngineLive)
```

`runnerStorage: 'memory'` selects process-local runner membership. Native messages and Activity replies still use SQLite. The harness receives KeyValueStore and EventJournal, and Effect owns the database tables and coordination.

## 3. Replace the in-memory Layers

Provide `StoreLive` when constructing `Session.layer`. Provide `EngineLive` to the executor registration graph in place of `WorkflowEngine.layerMemory`. Supply the Node platform services at the outer application boundary. Build those resources within the application's Scope.

Keep `Conversation.layerCreation` in Session construction. It creates the built-in documents and retains the recovery initializer. Register the same Workflow declarations and handlers before resuming their saved executions.

For independent Sessions in one backend, assign each Store a distinct snapshot key and register the already scoped Sessions in `SessionDirectory`. Keep session IDs and key mapping stable across restarts. Equal keys address equal domain state; a different directory registration does not partition storage.

## 4. Resume from saved identities

Retain each submission's request ID. Retry the same payload with the same ID after a caller loses its response. The saved admission and settlement select the original work. A new request ID admits new work.

Retain native execution IDs when using `{ discard: true }`; use the declaration's native `poll` and `resume` APIs. Display the conversation through [committed observations](observations.md). Native execution status and domain observations describe different persisted facts.

A normal shutdown closes the application Scope. An individual Session can be closed and reopened while the engine remains alive in an outer Scope. Closing a Session pauses recoverable work; it does not produce an Abort receipt.

The configuration is complete when a second process opens the same backend and a repeated request returns its existing settlement. The repository's [integration example](../apps/example/README.md) demonstrates this with a local model and SQLite.

## JSONL and other backends

For domain documents without native executions, follow [persistent application state](tutorials/persistent-state.md). JSONL needs a single writer; enable `fsync` for durable flushing. A persistent JSONL Store still needs a persistent WorkflowEngine if the application runs Workflows.

For SnapshotStore, all writers of a key must share compatible journal coordination. SQLite with native SQL KeyValueStore and SqlEventJournal is the validated SQL composition. Other backends need verification of locking, serialization and publication behavior before use. See [storage contracts](reference/documents-and-storage.md#storage-adapters).

## Handle storage failures

Inspect `StorageError.certainty`. A rejected candidate was not published. An uncertain failure means persistence may have happened; the open Store becomes poisoned. Close and reopen it, inspect saved receipts and reconcile the operation before admitting more writes. Blindly retrying the same external side effect cannot resolve that uncertainty.

Keep domain Store operations outside caller-owned database transactions. Use Session transactions for domain changes and native Activities for execution boundaries. [Replay and recovery](explanation/recovery.md) explains the separate commit points.
