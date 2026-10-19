# Restart and recovery

The store contains conversation history, admission records, tool intents/results, application documents, and the current run checkpoint. The runtime persists the assistant's tool intentions before invoking a handler and marks a tool started before its side effects.

After a process dies, build a new runtime against the same store, supply compatible models and handlers, and access `harness.root` or look up a saved conversation. Access automatically starts persisted work. Repeating a submission with the same request ID returns the original durable admission instead of adding another input. [Recovery.ts](../../apps/example/src/tour/Recovery.ts) uses the same program for both launches.

An interrupted started tool has an uncertain external outcome:

- A tool saved as safe, whose current declaration is still safe, may run again.
- An unsafe tool is not repeated. The model receives a failed result explaining the interruption and can decide what to do next.
- Changing a formerly safe tool to unsafe prevents replay.

A completed persisted tool result is reused. A crash after an external action but before its result commit cannot be made atomic merely by persisting local data. Application idempotency keys or reconciliation can address that external boundary.

Normal Scope shutdown interrupts running handlers and leaves resumable checkpoints. Explicit abort settles affected submissions as unanswered. Canceling an application's wait only ends observation.

The local runtime coordinates one store in one process. Do not run multiple independent active runtimes against the same persisted store. Recovery also requires tools to access the resources they relied on before the crash: restoring history alone does not restore files or external systems.
