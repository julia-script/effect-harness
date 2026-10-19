---
'effect-harness': minor
---

Bound Session's idle document materialization retention with a configurable ten-minute default. Reads and committed writes refresh retention, watches and remote mounts pin their incarnation, and expired documents and missing lookups reload from storage. `documentRetentionMs` is available through Session and Harness runtime options, accepts zero or Infinity, and leaves durable history unchanged.
