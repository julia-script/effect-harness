# effect-harness/durable

Committed conversation state and executor Layers for native Effect Workflow.

Use Sessions to keep typed application documents, conversation history, inboxes and replay receipts. Register the built-in conversation executors with your WorkflowEngine, alongside your own ordinary Effect Workflows.

## Install

```sh
npm install effect-harness effect
```

Use the equivalent `pnpm add`, `yarn add` or `bun add` command if you prefer. Add a platform or storage adapter when your chosen backend needs one.

## Commit an application document

A Session transaction gives you a mutable draft and publishes its changes atomically. A stable transaction key saves the result with those changes:

```ts
import * as Document from 'effect-harness/durable/Document'
import * as Session from 'effect-harness/durable/Session'
import * as Store from 'effect-harness/durable/Store'
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'

const Counter = Document.defineUnsafe({
  kind: 'example.counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
})
const Sessions = Session.layer.pipe(Layer.provide(Store.layerMemory))

const program = Effect.gen(function* () {
  const session = yield* Session.Session
  const increment = session.transaction(
    Effect.fnUntraced(function* (tx) {
      const counter = yield* tx.doc(Counter)
      counter.count += 1
      return counter.count
    }),
    { key: 'increment-once' },
  )
  yield* Console.log(yield* increment)
  yield* Console.log(yield* increment)
})

await Effect.runPromise(program.pipe(Effect.provide(Sessions)))
```

Both calls print `1`. The second reads the saved receipt. This example uses memory storage; [save application state across restarts](../tutorials/persistent-state.md) replaces it with a persistent Store.

## Add durable conversations

Compose a Session with `Conversation.layerCreation`, register it in `SessionDirectory`, and provide conversation Configuration, the generic harness Executor, Model.Catalog and WorkflowEngine to `durable/Executor.layer`. That Layer registers the built-in Workflows. Submit input with `workflow/Submission` and use native execution APIs to poll or resume it.

Persistence has two parts: the domain Store retains conversation facts, and the engine retains Workflow execution history. Supply persistent Layers for both to recover after a process restart.

## Find the right API

| Modules                                       | Purpose                                                       |
| --------------------------------------------- | ------------------------------------------------------------- |
| `Session`, `Document`, `Store`, `storage`     | Transactions, typed documents and storage adapters            |
| `Conversation`, `Inbox`, `Record`, `Identity` | Conversation settings, submissions and saved facts            |
| `Executor`, `workflow`                        | Built-in native Workflow declarations and registration Layers |
| `View`, `Event`, `Inspection`                 | Committed snapshots, watches and inspection                   |
| `Ownership`, `Entry`                          | Execution ownership and conversation history                  |
| `testing`                                     | Storage and lifecycle conformance helpers                     |

## Continue

- [First durable conversation](../tutorials/first-conversation.md): a complete model/tool application.
- [Compose native Workflows](../workflows.md): register custom jobs alongside the harness.
- [Persist conversations](../persistence.md): domain and engine storage.
- [Observe progress](../observations.md): committed state for consumers.
- [Documents and storage reference](../reference/documents-and-storage.md): addresses, migrations and failure certainty.
- [Replay and recovery](../explanation/recovery.md): commit boundaries and external effects.

See [Effect compatibility](../reference/compatibility.md) and [NOTICE](../../packages/effect-harness/NOTICE).
