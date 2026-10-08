# Declare a checkpoint task

Use a custom task for durable application work that belongs to a harness conversation. Define a stable name and version, schemas for input/checkpoint/result, an initial checkpoint and a phase handler.

[Greeting.ts](../apps/example/src/Greeting.ts) is a complete compiled example. Its initial phase returns `Task.continueWith({ phase: 'greet' })`; its next phase returns `Task.complete` with the greeting. `Task.bind` captures host and codec services when the application builds its Layer graph.

Install bound definitions through `Harness.layer({ tasks })` or `Harness.installTasks`. The built-in generation, tool and compaction definitions remain installed alongside custom definitions. Persisted records store names, versions and data; the application reinstalls executable definitions after reopening.

`Harness.spawn` admits a task and wakes its scheduler. To save an application reference atomically with admission, use a transaction as demonstrated by the greeting document in [main.ts](../apps/example/src/main.ts).

A phase receives `TaskRuntime`. Its `transaction` and `commit` methods fence ended invocations. `commit` can save document changes, entries and a task transition together. External model calls, tools and callbacks run outside that transaction.

`Task.wait` saves child task IDs and a join policy. Waiting ends the current invocation. A durable delay must save an absolute deadline before calling `TaskRuntime.sleepUntil`; the current fiber's sleep is disposable.

Changing a task's stored format requires a compatible definition version. Missing or unsupported definitions keep saved work inspectable until the host installs a matching definition.
