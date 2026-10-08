# Recovery from committed checkpoints

The store is the harness's recovery boundary. Conversations, documents, submission state, task checkpoints and outcomes commit together. A process interruption discards live fibers; reopening reconstructs work from those records.

## Model calls

A generation or summarization task saves its prepared request before inference. Recovery uses that pinned request boundary. A committed response or summary is reused. When generation was interrupted, recovery records an aborted attempt with its last committed partial output, clears that progress and reissues the same pinned request. The aborted output remains inspectable and is excluded from later model context. Closing preserves the submission and does not request an explicit abort.

## External actions

A tool task saves final arguments and replay policy before entering its handler. If its result commits, reopening uses that result. If the process stops during execution, the tool's external action may already have happened.

Safe replay requires both the saved and current registration to permit rerunning. Unsafe work settles with an interrupted result and committed partial output. Exactly-once external effects require guarantees supplied by the external system, such as its own idempotency key.

## Ownership and lifetime

Abort intent is persisted before active fibers are interrupted. Invocation checks fence late writes. Parents hold their outcomes until owned foreground work and cleanup finish. Closing pauses recoverable work and does not manufacture an abort decision.

## Working environments

The host supplies tool workspaces and remote capabilities. A persisted checkpoint cannot recreate uncommitted files lost with a machine. A replacement process needs access to the same required environment or an application-defined restoration procedure.

SQLite and JSONL adapters protect the harness's stored facts. Their durability still depends on the host's storage lifetime. One harness process owns each store; distributed worker placement and failover are outside this embedded runtime.
