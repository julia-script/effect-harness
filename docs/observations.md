# Observe committed changes

Call `Conversation.watch` to obtain an Effect Stream. The first item is a coherent snapshot; later items contain committed frames. Snapshot acquisition and subscription share the mutation line, so a commit cannot fall between them.

The change schema contains three cases:

- `snapshot`: initial conversation state and revision.
- `commit`: the next persisted frame.
- `reset`: a fresh snapshot after a subscriber loses its revision continuity.

Treat snapshots and resets as replacement state. Apply commit operations only after the preceding revision. The observer buffer is bounded; a slow subscriber can recover through a reset instead of retaining unbounded history in memory.

`Conversation.snapshot` reads current committed state without opening a subscription. Closing the harness or the consuming Scope ends the watch.

The application can send schema-encoded observations over a socket or SSE connection. It owns transport, authentication and routing. Remote clients reconnect to the owning harness and acquire a new snapshot.
