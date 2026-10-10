---
'effect-harness': minor
---

Add opt-in direct document migrations keyed by persisted source version. Transaction acquisition and updates validate the current encoded schema before staging an atomic upgrade. Reads and historical snapshots retain exact-version behavior, and forks preserve their configured revision policy.
