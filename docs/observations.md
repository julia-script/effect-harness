# How to display committed conversation progress

Use this guide to drive a UI, terminal or integration from saved conversation state. It assumes an existing Store and Session. Observations expose committed partial responses and tool progress; they do not expose uncommitted provider tokens.

## Build the observation services

Provide `View.layer` with the same Store as the Session. Provide `Event.layer` with that View:

```ts
import * as Event from '@effect-harness/durable/Event'
import * as View from '@effect-harness/durable/View'
import * as Layer from 'effect/Layer'

export const Observations = Event.layer.pipe(Layer.provideMerge(View.layer))
```

This Layer exposes both services and requires Store. Choose View for structural conversation snapshots and changes, or Event for semantic batches such as message, tool and submission transitions. Their contracts are listed in [execution and observations](reference/execution-and-observation.md#conversation-watches).

## Acquire a watch in the connection's Scope

This listener logs an initial entry count and ordered semantic batches. Provide `Observations` from your application's Store before running it:

```ts
import * as Event from '@effect-harness/durable/Event'
import type * as Record from '@effect-harness/durable/Record'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'

export const watchConversation = Effect.fn('watchConversation')(function* (
  conversationId: Record.ConversationId,
) {
  const events = yield* Event.Event
  const watch = yield* events.watch(conversationId)
  yield* Console.log(`Initial entries: ${watch.snapshot.entries.length}`)
  yield* watch.listen((batch) => Console.log(batch.map((event) => event._tag).join(', ')))
}, Effect.scoped)
```

Use the initial snapshot to populate the UI before consuming updates. `listen` joins the listener lifetime, so run it in the connection's scoped fiber when the rest of your application must continue. Close that Scope when the client disconnects.

A missing conversation fails with `StorageError` carrying `NotFound`. For document `snapshot` and `watchDoc` operations, absence instead returns `Option.none`; handle that before accessing a document value.

## Replace state on a reset

Process batches in order. For an Event watch, a `snapshot` event replaces the prior semantic state. For a View watch, a change with `reset: true` replaces the baseline with `change.value`; use that complete value instead of assuming all earlier operations arrived.

Each watch has a bounded backlog. A slow consumer can receive the newest coherent snapshot in place of intermediate history. Treat these streams as UI synchronization, not an unbounded audit trail. One watch's backlog does not force another watch to reset.

## Observe application documents

Use `session.watchDoc(token, target)` for a specific document and `session.state(token, target)` for a maintained current value. Check the Option returned during acquisition. Keep one consumption per watch and close its Scope when finished.

Document watches stay attached to the acquired incarnation. Retirement ends that watch; creating another document at the same address requires a new acquisition. See [document lifetime](reference/documents-and-storage.md#history-forks-and-lifetime).

## Handle the end of observation

`watch.stop` ends delivery deliberately. `watch.closed` reports the terminal reason. A listener failure ends that watch; session closure ends its observations before handler cleanup necessarily finishes. An observation ending is not evidence that a task succeeded or was aborted.

For ownership diagnostics, use `Inspection.get(session)` or `Inspection.changes(store)`. They read committed domain facts and do not run Workflows. See [lifetime rules](reference/execution-and-observation.md#lifetime).
