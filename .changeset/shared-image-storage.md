---
'effect-harness': minor
---

Store image payloads once per native storage namespace using verified content-addressed objects and durable ownership. Keep public reads hydrated, preserve native prompt bytes and URL objects through the new PromptWire codecs, and reclaim only images with no retained owners. Add StorageMedia helpers for atomic custom adapters. Require runtime format 12/12, SQL schema 3 and JSONL sidecars-v2 without legacy migration; JSONL backups must include the journal, .parts and .media directories.
