---
'effect-harness': minor
---

Add an explicit StorageFormat service and guard that persist container and minimum-reader compatibility before new-format execution. Built-in memory, SQL and JSONL layers supply the capability; custom adapters must explicitly supply a matching capability. Preserve legacy containers until deliberate activation, reject unsupported future readers before replay, and revoke uncertain writers until cold reopen reconciles persisted state. Add optional metadata-only document lookup for fresh schema-version checks without content materialization.
