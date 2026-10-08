# Execution and observations

A conversation submission has a persisted identity. A stable request ID deduplicates admission within a conversation. `Submission.wait` waits for `done` or `unanswered`; cancelling an individual waiter does not cancel admitted work. Queued inputs can be withdrawn through `Submission.withdraw`; placed inputs require conversation abort instead. Submission modes support queued follow-ups, steering at an active turn boundary, or rejecting a busy conversation.

`Conversation.reset` adds a new context head while preserving the historical entries.

Tasks store definition name/version, input, checkpoint, ownership, replay information and outcome. Their states are pending, running, waiting, completing and terminal. Phase handlers run outside transactions; reservation and state transitions happen on the serialized mutation line.

Opening restores interrupted running tasks to a recoverable boundary. `Harness.resume` starts saved work. Missing definitions and unsupported versions keep tasks inspectable until compatible definitions are installed.

Abort saves intent before interrupting live invocations. Ended invocations cannot commit late output. Owners hold completion while owned children finish. Closing stops live execution while retaining unfinished work for reopening.

Watch streams begin with a snapshot, then committed frames. Reset snapshots restore continuity after buffer overflow. Each snapshot includes the conversation, entries, tasks, submissions, documents and revision. The application owns serialization and remote delivery.
