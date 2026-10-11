---
'effect-harness': minor
---

Add a scoped standalone TaskScheduler for registered custom tasks over Session coordination. Compile typed executable closures during registration, retain leased service contexts, atomically commit phase checkpoints and memos with domain writes, normalize terminal results, resume persisted running checkpoints, and reject stale invocation writes. Require the exact supported StorageFormat capability before creation or dispatch and reconcile commit/registry wakes without blocking on observers.

Expose explicit typed host boundaries for ownership supervision, child joins, cascading abort, restart abandonment, conversation access and hooks. The standalone scheduler does not replace the legacy Harness.run executor; those integrations remain required separately. Include a real two-phase JSONL cold-reopen example and focused runtime and type proofs.
