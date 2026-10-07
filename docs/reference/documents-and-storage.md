# Documents and storage

This reference covers Session transactions, document definitions and the built-in Store adapters. Native WorkflowEngine persistence is a separate contract; [execution and observations](execution-and-observation.md) describes their relationship.

## Session transactions

`session.transaction(callback, options?)` evaluates an Effect callback against an isolated candidate. Success publishes all validated writes atomically; failure rejects the candidate. Callback error and environment channels are retained alongside StorageError.

Table queries must precede table mutations. A later table read fails with ReadAfterWrite. Document drafts support staged reads and writes within the callback. Transactions and drafts are revoked when that callback ends; access afterward fails with Revoked.

| Options                 | Result contract                           | Replay behavior                                                                                 |
| ----------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Omitted / unkeyed       | Arbitrary callback result, including void | Callback executes each time                                                                     |
| `{ key, fingerprint? }` | JSON-safe result or void                  | Writes and result commit together; same key returns original result without evaluating callback |

A changed fingerprint fails with Conflict. An omitted fingerprint is stored as the empty string. Void has its own receipt representation and is distinct from JSON null. Transaction keys are scoped to the Store.

`Document.copy(draft)` returns a Result with detached data or CloneError. `copyEffect` maps copying into the StorageError channel. `copyUnsafe` throws synchronously. Copies must be taken while the draft is active. External actions performed during a callback are not rolled back by candidate rejection.

## Document definitions

`Document.define(input)` creates a singleton token; `Document.family(input)` creates a keyed family token. Both return Result with DocumentDefinitionError. Their Unsafe variants throw for invalid definitions. A token describes the address/schema; it is not the stored value.

| Definition field                   | Contract                                                                                 |
| ---------------------------------- | ---------------------------------------------------------------------------------------- |
| `kind`                             | Stable nonempty string                                                                   |
| `version`                          | Positive safe integer                                                                    |
| `scope`                            | `session`, `conversation` or `task`                                                      |
| `schema`                           | Codec between decoded object data and JSON-object storage                                |
| `initial(seed?)`                   | Initial value for a new incarnation; optional JSON seed                                  |
| `migrate(value, fromVersion)`      | Optional pure upgrade from an older stored version to the declared model                 |
| `history`                          | Required for conversation scope: `latest` or `rewindable`; absent for other scopes       |
| `fork`                             | Required for conversation scope: `asOf`, `current` or `initial`; absent for other scopes |
| `checkpointWhen(value, ops, info)` | Optional checkpoint decision; info includes deltasSinceBase                              |

`asOf` requires rewindable history. Definition validation checks kind, version and compatible policies. Value codecs and initializers are checked during acquisition/mutation. Initializer failures are translated to rejected StorageError before commit.

Target fields are `owner?`, `key?` and `seed?`. Session documents require no owner; conversation/task documents require the corresponding identity. Families require an explicit key; singletons reject it. Owner, kind and family key determine the logical address.

## Reads and migrations

`tx.doc(token, target?)` acquires or creates a mutable draft. `session.snapshot(token, target?)` returns Option of a detached current snapshot; None means no live incarnation. `snapshotAsOf` reads a conversation document at an entry cutoff.

A snapshot contains `record`, decoded `version`, readonly `value` and `deltasSinceBase`. Reading an older value can project its migration without writing. Mutation persists the upgraded value. Newer stored versions, mismatched definitions and failed migrations reject the operation. Migrations are value transformations and have no Effect environment.

## History, forks and lifetime

`latest` retains present document content; `rewindable` supports entry cutoffs. Fork policy chooses the value in the new conversation:

| Policy    | Forked value                                  |
| --------- | --------------------------------------------- |
| `asOf`    | Historical value at the selected entry cutoff |
| `current` | Present value                                 |
| `initial` | Newly initialized value                       |

Retirement closes an incarnation. Recreating its logical address allocates a new incarnation. Existing watchers remain attached to the old one and end at retirement. Task documents do not implicitly inherit from parent tasks; terminal task projection retires task documents atomically.

## Storage adapters

| Constructor               | Options / defaults                             | Persistence contract                                                                            |
| ------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `Store.layerMemory`       | No options                                     | Scoped process-local state; each independent acquisition starts empty                           |
| `JsonlStore.layer`        | Required directory; fsync defaults false       | Single-writer JSONL recovery; incomplete final line repaired, malformed complete frame rejected |
| `SnapshotStore.layerWith` | key defaults `@effect-harness/durable/session` | Version-1 schema snapshot in KeyValueStore, coordinated through EventJournal.withLock           |

SnapshotStore saves authoritative state, receipts and retained observer frames in one value. Equal keys select the same state and coordination identity. Independent Sessions in one backend need distinct keys. The journal supplies coordination; observer frames remain in the snapshot. Each commit rewrites that retained snapshot.

All writers of a key need compatible coordination. Memory writers share both native service instances. The validated SQLite composition supplies native SQL KeyValueStore and SqlEventJournal from the same client. A memory journal with persistent values coordinates only writers sharing that in-memory instance. Arbitrary backend combinations do not imply equivalent locking.

Store operations are independent of native Workflow reply persistence and must not be wrapped in an external database transaction. Saved domain receipts restore complete Activity results after a commit-to-reply gap. The [persistence guide](../persistence.md) gives the application Layer graph.

JSONL fsync is enabled only when explicitly true; without it, successful writes do not establish crash durability. Directory writer ownership is external to the adapter. Journals retain bounded observation history and are not unlimited audit logs.

## Storage errors

StorageError carries a structured reason, message/cause projections and `certainty: 'rejected' | 'uncertain'`. No reason currently supplies automatic retry policy.

| Reason         | Meaning                                                         |
| -------------- | --------------------------------------------------------------- |
| Invalid        | Invalid domain data or rejected operation                       |
| Conflict       | Incompatible update or receipt fingerprint reuse                |
| NotFound       | Required record absent                                          |
| Closed         | Admission after sealing                                         |
| Poisoned       | Open Store has an unresolved persistence outcome                |
| Corrupt        | Invalid persisted state, receipt or frame                       |
| Io             | Backend failure; certainty states rejected or uncertain outcome |
| ReadAfterWrite | Table query after table mutations in one transaction            |
| Revoked        | Transaction/draft used after its callback ended                 |

Rejected means the candidate was not published. Uncertain means publication may have happened. Uncertain persistence or coordination outcomes poison the open Store; reopening and inspecting receipts is required before continuing writes.

## Format compatibility

Version-1 key/value snapshots retain their envelope. Retired bespoke SQLite tables (`durable_state`, `durable_journal`, `durable_receipt`) are not imported automatically. Their adapters and platform factory subpaths are excluded from exports. An old format needs an explicit export/migration with the software version that understands it. Replacing domain state with an empty snapshot while retaining native engine records is unsupported.
