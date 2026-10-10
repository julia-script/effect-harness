---
'effect-harness': minor
---

Activate explicitly selected deferred model execution with durable handles, absolute polling deadlines, finite expiry and retry budgets, and captured remote cancellation after the durable abort fence. Recovery polls the saved handle without repeating preparation or creating another response; an interrupted create without a saved handle remains an inspectable unknown external outcome. Deferred work requires storage reader 7; ordinary and legacy missing-selection requests remain native streaming.

Abort saves remote cancellation success, failure or timeout in the task's aborted result and settles locally within the cancellation wait budget. Runtime close joins tracked cleanup before releasing its resources and can wait for an uninterruptible custom callback. Caller-owned model and client Layers must outlive the runtime. Preserve observed usage when a native deferred publication is deterministically rejected.
