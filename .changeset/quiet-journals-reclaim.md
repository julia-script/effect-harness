---
'effect-harness': minor
---

Reclaim current-only document bodies on standalone retirement and persist document revisions separately so small deltas do not rewrite accumulated history. SQL commits prune obsolete revisions transactionally. JSONL uses synced immutable sidecars and a commit journal, automatically reclaiming superseded task checkpoints and document content during normal writes and recovery.

The new physical layouts reject older JSONL files and SQL schemas without migration. Copy the JSONL journal and its adjacent `.parts` directory together while the store is closed. Logical StorageFormat feature versions and the Storage/Document APIs are unchanged.
