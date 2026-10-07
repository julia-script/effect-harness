# @effect-harness/durable

Committed conversation state and executor Layers for native Effect Workflow. Applications declare and register ordinary Workflows; this package records domain facts, replay receipts and scoped observations.

```sh
bun add @effect-harness/durable effect@4.0.1
```

Public subpaths include `Session`, `Store`, `Document`, `Conversation`, `Inbox`, `Record`, `Identity`, `Entry`, `Ownership`, `Inspection`, `View`, `Event`, `Executor`, `storage`, `workflow` and `testing`. Internal and retired SQLite adapter subpaths are excluded.

Build Session from Store, providing `Conversation.layerCreation` when using the built-in conversations. Register Sessions in SessionDirectory, then provide that directory, Configuration, harness Executor, Model.Catalog and WorkflowEngine to `Executor.layer`.

Start with [the conversation tutorial](../../docs/tutorials/first-conversation.md) or [persistent application state](../../docs/tutorials/persistent-state.md). Integration guides cover [native Workflows](../../docs/workflows.md), [persistent execution](../../docs/persistence.md) and [observations](../../docs/observations.md).

[Documents and storage](../../docs/reference/documents-and-storage.md) records transaction and adapter contracts. [Execution and observations](../../docs/reference/execution-and-observation.md) covers ownership, receipts and lifetimes. See [Effect compatibility](../../docs/reference/compatibility.md) and [NOTICE](NOTICE).
