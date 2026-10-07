# Compose native Workflows

This guide is for TypeScript developers building Effect v4 applications. Start from the [example](../apps/example/src/main.ts) for public imports, Layer composition and Workflow calls.

A native Workflow declaration contains payload, success/error schemas and an idempotency key. Its `toLayer` registers the handler with a supplied WorkflowEngine. Activities provide native replay boundaries. The example's `Greeting` declaration and handler show this without domain tasks; it is merged with the built-in harness executor Layer and runs on the same engine.

Native SQL Cluster execution also requires the checkout's [pinned readiness repair](../README.md): recovered Activity requests await their definition before acquiring their transaction, avoiding a reproduced SQL-prefix recovery deadlock. Activity body/reply persistence remains native and transactional. This changes the pinned dependency's internal ordering, not the application's Workflow declaration or executor API.

For harness conversations, provide these services:

| Layer                                     | Required inputs                                                                     |
| ----------------------------------------- | ----------------------------------------------------------------------------------- |
| `harness/Executor.layer`                  | Registry and Model.Catalog                                                          |
| `durable/Session.layer`                   | Store; captures optional CreationHook at construction                               |
| `Conversation.layerCreation`              | Configuration and Crypto; captures optional Registry for creation hooks             |
| `SessionDirectory.layerSingle(sessionId)` | A scoped Session                                                                    |
| `durable/Executor.layer`                  | SessionDirectory, Configuration, harness Executor, Model.Catalog and WorkflowEngine |

Keep the same Layer values when sharing resources so Layer memoization can share their instances. Provide native filesystem/path/crypto/HTTP/process/SQL adapters at the application boundary. Let TypeScript expose each actual environment requirement; do not erase `R` with a cast or provide unrelated services to satisfy a guessed signature.

Call the native `Submission` declaration’s `execute` method to admit an input or passive write and await the settled receipt. A payload includes session/conversation identity, a stable request ID, and an input/write discriminant. Busy input behavior can be `steer`, `followUp` or `reject`. Choose the request ID before a request crosses the boundary and reuse it on retry. Same-kind replay returns the original receipt and ignores changed content; reusing an admission key for the other submission kind fails. Use a new request ID for new content.

Owned decoded variants use `_tag`. Public constructors and codecs retain the existing `kind`, `action` or `type` fields in persisted JSON and native Activity payloads. The example submits an `input` variant and narrows the settled receipt to `InputDone`.

Decode external session, request and run strings with the schemas in `durable/Identity` before passing them to the typed domain API. Conversation and entry IDs come from `harness/Identity` and are reexported unchanged by `durable/Record`. These brands retain the existing encoded strings and numbers. Native Workflow execution IDs and provider session IDs retain their own contracts. The example decodes its fixed session and request IDs at the application boundary.

Domain deadlines and entry timestamps use `DateTime.Utc`; duration options accept `Duration.Input`, including literals such as `'100 millis'`. Stored timestamps retain numeric epoch milliseconds and fractional precision; stored spans retain their original numeric units. Retry waits use the native durable Workflow clock and the recorded absolute deadline, so replay retains the original wait decision.

The native `execute(payload, { discard: true })` returns an execution ID. Native `poll(id)` returns an optional completed/suspended result, and `resume(id)` asks the engine to resume an execution. They refer to engine state. Committed domain views and settlement receipts refer to storage state. They are different observations and may become visible at different points.

`Conversation.awaitIdle(session, conversationId?)` is an ordinary Effect that waits for non-background owned work and drives its declared native executions. Omit the ID to wait across the Session's ownerless conversation roots. It requires `Ownership.Declarations` and the native WorkflowEngine, both exposed by the example's composition; it does not create a task or a replacement execution engine. Missing declarations leave work blocked until compatible code is restored. The public example invokes it after settlement.

For owned work, use the helpers in public `durable/Ownership` and `durable/workflow/Structured` from inside ordinary native handlers. Register your declarations together with `Executor.workflows` through `Ownership.layerDeclarations`, and use `Executor.layerExecutors` instead of the built-in-only declarations Layer. A domain binding records native workflow name, execution identity and JSON payload; it does not supply a second task engine.

`Ownership.layerDeclarations` captures the schema services supplied at construction and exposes metadata without requiring an engine. `Ownership.execute(binding)` uses that captured schema context with the caller's native WorkflowEngine and optional WorkflowInstance. `Ownership.layerCurrent(identity)` requires the exact invocation Session. For multiple existing Sessions, provide `SessionDirectory.Registrations` explicitly and build `SessionDirectory.layer`; it snapshots the map at build time while retaining the supplied scoped Session references.

Structured helpers create stable child bindings, execute/join children with all-settled or fail-fast policy, hold an owner in `completing`, and drain non-background children and owned conversations before terminal settlement. A custom owned child must commit its terminal domain projection: returning only a native result leaves the projection incomplete and is rejected by join/drain. Direct child creation in the owner's finishing commit is rejected. Already owned conversations can finish their admitted work while the owner is completing. Background children are fenced from holding normal completion.

`Ownership.Current` provides the scoped domain identity, and memo helpers retain the first committed JSON value for a task lifetime. A producer may run again if it completed an external action before its value committed. Terminal projection clears task memos and retires task documents. Task documents do not implicitly inherit from a parent task.

Use the built-in Abort Workflow for domain cancellation. Committed abort marks fence late work; scoped Cancellation bridges those marks to live handler fibers and joins finalizers. Native engine interruption alone is not a guarantee that an already running handler has stopped. Owners settle only after child work and cleanup are accounted for. Effects that act outside the database still need safe replay or reconciliation policies.

Session closure pauses execution for reopening; it does not admit a durable abort. Close the caller-owned child Scope containing the composed Session and Store Layers to release them early. Session cleanup seals domain admission and ends observers before joining registered handler cleanup; Store release then drains admitted operations and closes backend resources. `session.awaitClosed` observes the persistent typed cleanup result, and cancelling that wait does not cancel cleanup. `Session.onClose` registrations belong to the invocation's Scope. Keep the native engine in an outer Scope when closing/reopening a Session to resume its suspended executions. Closing the application Scope also joins live work; explicit Abort remains a separate domain operation.

Bind hooks with `harness/Hook.bind` when they need host services. It captures those services while binding and retains the dynamic `Invocation` at execution. Declare additional request services in its second argument; for example, `Ownership.Current` is available inside a bound native domain invocation, not automatically during conversation creation. A missing declared service fails as a typed hook error. `afterTools` receives committed `Hook.SettledTool` entries in provider call order, including unavailable calls, with `entryId`, outcome and final result. Parallel completion order does not reorder that batch.

`Conversation.layerConfiguration` validates initial settings and can fail with `SchemaError`. Its `toolConcurrency` option accepts a positive safe integer and defaults to 16; sequential tool mode uses one permit. Parallel results retain provider call order for `afterTools`, while each result commits when its tool completes. `Configuration.updateSettings` validates a replacement host policy and exposes the same typed error; invalid updates retain the prior settings. Resolved policy reads observe the current settings, including retry decisions after a failed request. A pinned request's payload remains pinned while policy can change. Conversation agent configuration is committed separately through `Conversation.configure`.

Retry admission stores its decision and absolute deadline together. Policy changes before the decision can enable or disable retry; once the native timer is admitted, a later disable does not revoke that retry. Recovery uses the stored decision rather than recomputing it.

If preparation commits before its native Activity reply is cached, recovery first checks the domain preparation receipt using the same stable key. It reuses the pinned prompt before resolving changed model selection or rendering sections.

The generic request Executor needs this checkout's [Effect AI patch](../README.md) for undeclared tool calls. It uses native `streamText` with caller-owned tool resolution; the harness validates and commits unavailable settlement rather than asking the model SDK to execute unknown code.

Context estimates prefer the newest measured assistant usage after the active marker and estimate only messages that follow it. A model Descriptor can supply `estimate` for provider-specific tokenization. The fallback counts visible text/reasoning, tool names/JSON parameters and tool-result content at roughly 3.5 characters per token; each native file/image part contributes a fixed 4,800 character-equivalent units before that division. Canonical tool-content envelopes are measured by their visible parts, rather than their serialized byte arrays. Managed system deltas count their visible section text and tool declaration/removal metadata rather than the stored patch envelope. This is a selection/compaction heuristic, not a tokenizer guarantee or a billed-usage substitute.

Context edits also govern managed patches: omission removes the patch, a null section delta deletes its prior projection, and replacement messages remain ordinary native messages. Metadata-only system entries still contribute their semantic section/tool content to estimates even without stored model messages.
