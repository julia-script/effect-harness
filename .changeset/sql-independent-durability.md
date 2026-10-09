---
'effect-harness': patch
---

Reject SQL storage initialization and mutations inside the supplied client's ambient transaction so Session cannot publish state that an outer rollback later erases.
