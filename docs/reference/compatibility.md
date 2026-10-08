# Effect compatibility

The package peer requirement is Effect `^4.0.1`. The implementation and examples are checked against Effect `4.0.1` and the corresponding `4.0.1` platform/provider adapters. Examples use the released `effect/workflow`, `effect/ai`, `effect/persistence`, `effect/eventlog` and `effect/cluster` module paths.

## Model response validation

The harness uses unmodified Effect dependencies. Native Effect AI validates model responses against the offered Toolkit. Undeclared tool names and invalid arguments fail with `AiError` carrying `InvalidOutputError` before the rejected call reaches the harness.

Failed generation entries retain the native AI error in `Conversation.Metadata.error`. `Conversation.context` exposes it on the projected entry. Already streamed text remains in the saved transcript, but failed assistant messages are excluded from the next model prompt.

Effect AI does not expose the rejected response in this error. The harness cannot identify its call ID, name or arguments reliably. It records the available diagnostic and adds generic corrective feedback to the next request: the previous response could not be validated, and the model should use only offered tools with arguments matching their schemas. It does not create a tool result for the missing call.

Automatic retries follow the configured retry policy and limit. With retries disabled or exhausted, the input settles as `InputUnanswered` with reason `model_error`; the saved error remains available. A later input also receives the corrective feedback while that entry remains in its context. Context edits and compaction can remove it from the model's history.

## Native Workflow recovery

The harness does not patch ClusterWorkflowEngine or its transaction handling. Built-in Activities use ordinary native replay and domain receipts. Their domain commits and the native Activity replies have separate commit points; a saved domain receipt resolves the gap on recovery. See [replay and recovery](../explanation/recovery.md#the-commit-to-reply-gap).

A separate upstream recovery deadlock was reproduced with `ClusterSchema.WithTransaction` Activities and a shared SQLite client. That annotation lets a recovered Activity request acquire the connection before its Workflow has replayed far enough to register its definition. Earlier database access can then block the replay needed to release the connection. Built-in harness Activities do not use that annotation. The library does not supply a workaround for custom transaction-annotated Activities.
