---
'effect-harness': minor
---

Add explicit persisted restart policies and durable abort intent/reasons with legacy resume defaults, immutable policies/receipts, and preparation helpers. The production scheduler is unchanged; older readers discard additive task fields, so safe activation still requires a storage format downgrade guard.
