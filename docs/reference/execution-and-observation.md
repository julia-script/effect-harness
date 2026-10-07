# Execution and observations

This reference covers built-in Workflow identities, settled receipts, ownership, observation handles and lifetimes. Native Workflow declaration and Activity APIs retain Effect's own contracts.

## Built-in declarations

`Executor.layer` registers Submission, Generation, ToolCall, Compaction and Abort in the supplied WorkflowEngine. `Executor.workflows` exposes that declaration set. `Executor.layerExecutors` registers the same handlers using caller-supplied Ownership.Declarations.

Native execution owns execution IDs, Activity replay, suspension, timers and results. Domain storage owns conversations, entries, documents, submission receipts and task projections. Native `poll` and a committed View are observations of different state and can become visible at different points.

Public decoded domain variants use `_tag`. Their codecs retain the encoded `kind`, `type` or `action` fields where specified. A nominal `is` guard checks library identity; stored data needs its schema decoder before it is trusted.

## Submissions

Submission.execute payload fields are:

| Field            | Contract                                            |
| ---------------- | --------------------------------------------------- |
| `sessionId`      | Identity.SessionId registered in SessionDirectory   |
| `conversationId` | Existing domain conversation ID                     |
| `requestId`      | Stable Identity.RequestId selected before admission |
| `submission`     | Decoded input or passive-write variant              |

Input fields are `_tag: 'input'`, `type: 'input'`, native Prompt.UserMessage and optional `whenBusy`. Its policies are `steer`, `followUp` and `reject`; omission selects follow-up behavior. Rejection while busy fails with ExecutionError carrying ConversationBusy. Passive writes use `_tag: 'write'`, `type: 'write'` and EntryDraft. They place domain entries without directly requesting a model turn.

Native execution identity combines session, conversation, request ID and submission kind. Domain admission rejects reusing the same conversation/request identity for a different kind. Same-kind replay returns the original receipt and ignores changed content. A new request needs a new ID.

| Settled `_tag`  | Meaning                                 | Additional fields                       |
| --------------- | --------------------------------------- | --------------------------------------- |
| InputDone       | Input has a committed answer            | entry, answer                           |
| InputUnanswered | Input settled without an answer         | optional entry, reason, optional detail |
| WriteDone       | Passive entry placed                    | entry                                   |
| WriteUnanswered | Passive write settled without placement | reason, optional detail                 |

All receipts retain submission identity. Unanswered outcomes are domain results, not necessarily Effect failures. Native execute waits for the settled result by default. `{ discard: true }` returns an execution ID; poll returns optional native result state and resume requests resumption.

Session/request/run brands retain their encoded strings. Conversation/entry identities retain numeric encoding and are shared with harness Identity. Domain instants use DateTime.Utc; duration options accept Duration.Input.

## Owned work

Ownership.Binding records native workflow name, execution ID and JSON payload. Ownership.Declarations captures declaration schema context at construction; it does not acquire a WorkflowEngine. Ownership.execute uses that context with the caller's engine and optional WorkflowInstance. Ownership.Current identifies the scoped domain invocation.

Structured helpers bind stable child identities, execute/join them with all-settled or fail-fast policy, and drain non-background child tasks and owned conversations. An owner in `completing` cannot create another child in its finishing commit. Already owned conversations can finish admitted work. Background children do not hold normal completion.

Custom owned tasks require a committed terminal domain projection in addition to their native result. Missing terminal projection is rejected by join/drain. Missing declarations prevent persisted work from executing until compatible declarations are restored.

Task memos retain the first committed JSON value for that task lifetime. A producer can run again before its result commits. Terminal projection clears task memos and retires task documents. `Conversation.awaitIdle(session, id?)` waits for non-background owned work; omission selects the Session's ownerless conversation roots. It requires declarations and WorkflowEngine.

## Conversation watches

View.layer requires Store; Event.layer requires View. Acquisitions require Scope and fail with StorageError if the conversation is absent.

| Handle                         | Initial state                                       | Subsequent deliveries                                                       |
| ------------------------------ | --------------------------------------------------- | --------------------------------------------------------------------------- |
| `View.watch(id)`               | value: conversation, visible entries, built-in docs | Change with seq, before, value, ops, reset and optional publication/rebased |
| `View.state(id)`               | Maintained value and cursor                         | Value advances while scoped; closed reports end                             |
| `Event.watch(id)`              | snapshot plus initial value batch                   | Ordered semantic event batches; snapshot events replace state               |
| `View.observe(id, projection)` | Custom projection                                   | Incremental projection or reset projection                                  |

Built-in view docs are agent, live execution, inbox, provider affinity and usage. Event snapshots also project ongoing run/generation/tool/compaction and queued inputs. Live provider data appears only after a domain commit. Ordinary failed receipts are expected outcomes; `task_failed` describes faulted/orphaned task outcomes rather than every failed receipt.

A watch's initial state is separate from its changes stream. `changes` is single-consumer; `listen(listener)` joins that consumption and closes the watch if the listener fails. `stop` ends delivery; Scope still owns resource release. `closed` reports stopped, cancelled, session_closed, retired or listener_error as applicable.

View/Event hold at most 100 pending batches per watch, excluding a batch already delivered to a listener. Overflow replaces pending history with the newest coherent reset/snapshot. Each watch has independent backpressure. A conversation's observers share its mount and storage driver; the last release closes that mount.

## Document watches and inspection

`session.watchDoc(token, target)` returns Option of a watch. None means the addressed document is absent at acquisition. Its value follows the acquired incarnation, and changes include seq, value, ops and reset. Retirement yields null and ends that watch. `session.state` maintains the same scoped document lifetime.

`Inspection.get(session)` and `Inspection.changes(store)` expose persisted ownership facts without consulting a scheduler or executing handlers. Retained commit streams are bounded; consumers needing complete current state use snapshot/reset observation.

## Lifetime

Session and Store resources belong to their construction Scope. Session release seals domain admission and observers, pauses native work for resume, joins registered body cleanup and then permits backend cleanup. Store release drains admitted operations before releasing backend resources.

`session.awaitClosed` observes the shared terminal cleanup result with typed StorageError; it does not initiate release. Cancelling that wait does not cancel cleanup. Session.onClose registrations belong to the invocation's Scope.

Abort is a durable domain operation. Committed abort marks fence late work, and Cancellation bridges them to live handler fibers and joins cleanup. Native engine interruption alone does not prove an already running handler has stopped. Session closure does not manufacture an abort outcome. An engine kept in an outer Scope can resume work when a Session is reopened.
