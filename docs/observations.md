# Observing committed work

`Submission.wait` returns a persisted `InputDone` or `InputUnanswered` record. Reusing a request ID in the same conversation retrieves the existing submission. Interrupting `wait` cancels the observer; it does not cancel durable execution. `Submission.withdraw` removes queued work when it has not yet been placed, and `Conversation.abort` stops work for that conversation.

`Conversation.entries` returns a Stream of persisted entries. It emits existing visible history in ascending order and then follows newly committed entries. Supply `{ after: entryId }` to continue after a known entry.

```ts
const entries = root.pipe(Conversation.entries())
const nextEntries = root.pipe(Conversation.entries({ after: lastSeen }))
```

Entries include user input, assistant messages, tool progress, diagnostics, and results. `InputDone.answer` identifies the final assistant entry; entry `model` values encode native Effect AI prompt messages through schemas.

`Conversation.snapshot(document, target)` returns an optional decoded document revision. `Conversation.watch(document, target)` returns a Stream starting with the current optional revision and then committed changes to that document. Decoding happens on the client, using the supplied schema. The backend carries schema-backed JSON data.

`Session.commits` observes atomic write batches inside a runtime. `Session.watch` observes document revisions when using a Session independently. Scope cleanup ends subscriptions. Progress belongs to persisted history, so reconnecting observers can inspect it after a restart.
