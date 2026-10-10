---
'effect-harness': minor
---

Retire active task documents atomically when Transaction.putTask makes their owner terminal. Reject acquisition and replacement for terminal owners, including legacy tasks, before initialization or migration runs. Failed settlement preparation stages no partial retirement, even when caught. Detached snapshots and conversation history retain their existing read semantics; legacy terminal documents can be explicitly retired.
