---
'effect-harness': minor
---

Share record validation, commit encoding, replay and retention across JSONL, SQLite and memory storage. The adapters persist opaque commit frames and named payload streams instead of implementing harness rules.

Replace `Storage.layerJournal` with `Storage.layerSql({ storeId })`, backed directly by a host-supplied SQLite `SqlClient`. Each batch stores its frame and payloads in one transaction; ambient application transactions are rejected and uncertain writes require reopening. `Storage.layerMemory` uses Effect Ref, HashMap and Chunk without platform dependencies.

JSONL now stores readable commits with document revisions and active task checkpoints in adjacent named streams. Current-only content is reclaimed after commit, while rewindable history is retained. Previous JSONL and EventJournal layouts are not migrated. IDs become durable when committed; IDs only allocated since the last commit may be reused after restart.
