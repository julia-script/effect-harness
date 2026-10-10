# effect-harness

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
