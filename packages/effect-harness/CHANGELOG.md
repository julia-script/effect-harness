# effect-harness

## 0.4.0

### Minor Changes

- 1ee6db2: Add ordered transactional conversation initializers to Session and the local Harness runtime. New roots, independent conversations, forks and configured raw Transaction creation commit their required documents and callback writes together. Failed or interrupted initialization leaves prior transaction drafts intact, including when callers catch the failure. Existing roots and reopened conversations skip initialization; notification hooks retain their post-commit behavior.
- 1ee6db2: Add opt-in direct document migrations keyed by persisted source version. Transaction acquisition and updates validate the current encoded schema before staging an atomic upgrade. Reads and historical snapshots retain exact-version behavior, and forks preserve their configured revision policy.
- 1ee6db2: Persist globally unique conversation affinity through reopen and allocate a fresh identity on forks. Expose request-scoped ProviderAffinity to custom adapters and default supported OpenAI prompt_cache_key without overriding caller options.

  Persist model token usage, caller-declared pricing snapshots and opt-in tool spend with completed entries and recovery checkpoints. Add atomic conversation/session usage queries with fork deduplication, explicit unknown counters and costs, separate currencies, and legacy history reporting. Older transports may omit accounting queries; older decoders discard new usage fields on downgrade.

- 1ee6db2: Publish runner-neutral Effect storage conformance cases through effect-harness/Testing. Scoped adapter factories cover atomicity, identity, ownership, document lifecycle and fork visibility, with opt-in historical document, reopen and SQL transaction recovery checks and typed assertion failures.
- 1ee6db2: Add Harness.submission(id) to reacquire saved input submissions through the existing backend read contract. Acquisition immediately validates the ID, preserves the original conversation ID, and creates no durable records. Reopened clients can read, wait for, and withdraw the original submission.
- 1ee6db2: Add opt-in Tool.makeResult handler envelopes with independently validated structured output, model-visible content and UI details on success and failure. Preserve all channels through hooks and durable JSON recovery while keeping existing Tool.make behavior.
- 1ee6db2: Retire active task documents atomically when Transaction.putTask makes their owner terminal. Reject acquisition and replacement for terminal owners, including legacy tasks, before initialization or migration runs. Failed settlement preparation stages no partial retirement, even when caught. Detached snapshots and conversation history retain their existing read semantics; legacy terminal documents can be explicitly retired.

### Patch Changes

- ca61686: Prepare all fork document copies before staging the child conversation. Catching a fork validation error inside a successful Session callback no longer commits orphan copies, while unrelated callback changes remain commit-able.
- 7c7742f: Project tool results from their post-hook model-visible content, retaining plain strings and using the canonical provider envelope for media and provider options. Preserve media, part options, and hook redactions while retaining typed details separately in durable history.
- a5a2bfc: Keep native HTTP clients, OpenAI embeddings and Anthropic generic streaming when tool-result adapters wrap inherited or non-enumerable service properties. Forwarded methods retain their captured receiver.
- d0c76ac: Preserve the complete scoped tool failure cause when it includes a defect, interruption, or infrastructure error. A declared domain failure no longer hides an accompanying cleanup defect or infrastructure failure, while domain-only failures still become tool results.
- 9ed0877: Resolve duplicate tool declarations once using native Toolkit precedence, keeping the offered definition, extension admission, and persisted recovery policy aligned with the selected handler. Prevent interrupted unsafe overrides from being replayed after restart.
- 09a8c8b: Normalize SQL COMMIT and ROLLBACK errors into uncertain StorageError failures and require reopening direct Storage after these failures.
- 336b0a5: Preserve native tool file bytes across JSON serialization so Anthropic receives decoded text documents rather than base64 strings. Literal string content retains its original meaning.

## 0.3.0

### Minor Changes

- d69c6bc: Promote the schema-backed Storage and Session APIs and separate the Harness client from local execution through HarnessBackend. Add Tool/Toolkit declarations with handler Layers, scoped tool execution, persisted submissions and recovery, and static hooks/extensions/model descriptors. Replace the registry, environment, checkpoint-task, and runtime-specific storage APIs with service requirements and application-supplied Layers. Update public examples and documentation to the new API.

### Patch Changes

- f2cdab6: Reject SQL storage initialization and mutations inside the supplied client's ambient transaction so Session cannot publish state that an outer rollback later erases.

## 0.2.1

### Patch Changes

- 270dfbc: Remove the Bun requirement from npm package preparation and declare the JSONL Bun adapter's platform dependency as an optional peer. The core and portable public imports remain independent of runtime-specific adapters.

## 0.2.0

### Minor Changes

- 0f3f989: Fix durable commit coordination, scoped resource ownership, provider request policies, and credential handling. Use canonical concept modules, tagged errors and codecs, and native duration options; remove deprecated aliases and legacy input adapters.
- 7a32513: Add `Conversation.layer(options)` to provide shared host configuration and conversation creation hooks in one Layer. Expose the Session-backed convenience service through `Conversation.layerFromSession`.
- 3d2f690: Introduce an embedded Harness with schema-backed conversation records, checkpoint tasks, submissions, documents and observations. Add atomic Persistence adapters for Memory, SQLite and JSONL, plus explicit model and tool recovery.

  Replace the Workflow execution APIs and whole-state storage with the embedded runtime. Remove bundled account sign-in and Claude Code CLI integrations while retaining native API-key providers. This experimental release changes public APIs and stored formats; use fresh stores.

## 0.1.0

### Minor Changes

- 28b74dd: Release the unified Effect Harness package with native AI model and tool execution, durable conversations and Workflow integration, account authorization, and OpenAI, Anthropic and Claude Code providers.
