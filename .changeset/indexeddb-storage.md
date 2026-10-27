---
'effect-harness': minor
---

Add an IndexedDB storage backend. `Storage.layerIndexedDb(name, { factory?, durability? })` keeps a Session in an IndexedDB database, so a loop running in a browser survives reloads. It loads the database into memory on open, writes each commit in one transaction, and opens `globalThis.indexedDB` unless you pass another factory, such as `fake-indexeddb`'s.
