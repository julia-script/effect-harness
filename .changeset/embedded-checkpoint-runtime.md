---
'effect-harness': minor
---

Introduce an embedded Harness with schema-backed conversation records, checkpoint tasks, submissions, documents and observations. Add atomic Persistence adapters for Memory, SQLite and JSONL, plus explicit model and tool recovery.

Replace the Workflow execution APIs and whole-state storage with the embedded runtime. Remove bundled account sign-in and Claude Code CLI integrations while retaining native API-key providers. This experimental release changes public APIs and stored formats; use fresh stores.
