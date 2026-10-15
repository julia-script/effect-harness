---
'effect-harness': minor
---

Replace `Storage.layerSql` with `Storage.layerJournal({ storeId? })`, backed by an application-supplied Effect `EventJournal`. The harness no longer contains SQL queries. SQLite applications compose Effect's `SqlEventJournal.layer()` with their database client.

The new adapter requires one owning runtime per store, persists each batch and metadata mutation as one event, and rebuilds local state from a validated revision chain. Uncertain appends seal the instance until a scoped reopen reconciles durable state. Existing custom SQL tables are not automatically imported. Memory and JSONL storage remain available.
