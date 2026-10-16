# effect-harness

## 0.6.0

### Minor Changes

- 43483da: Add BunSecretsCredentialStore under effect-harness/provider-auth-bun for storing provider credentials with Bun Secrets, including cross-process locking, metadata listing, and sanitized storage errors.
- 4ac03aa: Add Claude OAuth login and Messages adapters with reversible tool-name translation, protected authentication headers, and durable configuration bindings. Preserve existing API-key constructors. Document the unofficial OAuth integration and lack of live entitlement verification.
- 9079818: Apply model-specific Claude OAuth thinking replay policies across history and tool declaration changes, and bind the policy and supported generation capabilities into durable request configuration. Verified prefix-bound models request server mismatch handling with adaptive thinking; unverified thinking models fail before dispatch.
- 4b127c1: Add ChatGPT Codex browser/device OAuth, SSE completion and streaming, native tool/media/reasoning translation, and account-bound durable model configuration. WebSocket transport and structured final output remain unsupported by this adapter.
- 6de38ac: Add explicit ToolImage.prepare for custom tools, using the existing image services and request-pinned model policy. Share validated multimodal output and coordinate metadata with image file reads, and document a typechecked screenshot-tool example.
- 76a03bb: Add opt-in deferred tool discovery with native annotations, an invocation-scoped permission-filtered metadata catalog, local BM25 ranking, and a replay-safe model-only tool_search toolkit. Persist ordered model loads and owner/version fences across restart, compaction and historical forks. Replace modelTools arrays with explicit default/explicit selection objects and apply one canonical inventory to offers, admission and activation validation.
- 723464d: Add explicit provider credential schemas, serialized and bounded OAuth refresh, and optional durable file/loopback host integrations. Keep live secrets separate from durable conversations and fence logout/account replacement through the credential store.
- cff402a: Give credential store implementations explicit backend names. Import `FileSystemCredentialStore` from `effect-harness/provider-auth-node` (or its `/FileSystemCredentialStore` subpath), and `MemoryCredentialStore` from `effect-harness/provider-auth` (or its `/MemoryCredentialStore` subpath). Both modules expose `make` and `layer`; `CredentialStore` remains the shared service contract.

  This is a breaking API change: replace `provider-auth-node/CredentialStore` with `provider-auth-node/FileSystemCredentialStore`, and replace `CredentialStore.makeMemory` / `CredentialStore.layerMemory` with `MemoryCredentialStore.make` / `MemoryCredentialStore.layer`.

- 6de38ac: Bound Session's idle document materialization retention with a configurable ten-minute default. Reads and committed writes refresh retention, watches and remote mounts pin their incarnation, and expired documents and missing lookups reload from storage. `documentRetentionMs` is available through Session and Harness runtime options, accepts zero or Infinity, and leaves durable history unchanged.
- 80a937e: Share record validation, commit encoding, replay and retention across JSONL, SQLite and memory storage. The adapters persist opaque commit frames and named payload streams instead of implementing harness rules.

  Replace `Storage.layerJournal` with `Storage.layerSql({ storeId })`, backed directly by a host-supplied SQLite `SqlClient`. Each batch stores its frame and payloads in one transaction; ambient application transactions are rejected and uncertain writes require reopening. `Storage.layerMemory` uses Effect Ref, HashMap and Chunk without platform dependencies.

  JSONL now stores readable commits with document revisions and active task checkpoints in adjacent named streams. Current-only content is reclaimed after commit, while rewindable history is retained. Previous JSONL and EventJournal layouts are not migrated. IDs become durable when committed; IDs only allocated since the last commit may be reused after restart.

- dac5092: Add lazy provider model catalogs with separate list, detail lookup, and construction Effects. Preserve each operation's error and service requirements, expose serializable model and reasoning metadata, and offer account-bound bounded discovery caches.

  Include concrete Codex and Claude OAuth provider factories with native option translation and durable configuration. Codex discovers the authenticated account catalog; Claude OAuth uses bundled definitions, with a separate API-key source for paginated Models API discovery. Custom sources retain their operation-specific requirements.

- 9488dea: Discover Claude models and reasoning capabilities lazily through OAuth-authenticated Models API requests instead of a bundled inventory. Preserve discovery service requirements and use the existing credential refresh lifecycle.

  Allow newly discovered models to use ordinary thinking replay without a model-family allowlist. Keep known server-side mismatch handling and expose per-model replay compatibility overrides, pinned in durable configuration.

  The API-key catalog's `apiUrl` now denotes the API origin, matching OAuth inference and discovery.

- 15235b2: Add a ready-to-use OpenRouter provider with unauthenticated public model listing and direct detail lookup, live capabilities/pricing/reasoning efforts, and credential-managed inference through the shared compatible transport.
- f427442: Replace per-account provider declarations with schema-defined integrations and a dynamic Providers manager. Separate public instance persistence from credential storage, allow unauthenticated instances, and return secrets from explicit login helpers for consumer-owned updates. Carry instance identity and revision through model selection and durable recovery.

  Add a configurable OpenAI-compatible Chat Completions provider factory with independent metadata discovery and a static-catalog option. Codex and Claude definitions support live catalogs and custom metadata callbacks. Remove profile-keyed credentials and the former provider/catalog construction APIs; stored credential envelopes and prepared account bindings require explicit migration or repreparation.

  Map compatible-provider reasoning to standard Chat Completions or explicit OpenRouter format, translate canonical tool images while preserving tool-call ordering, and enable subscription temperature/long-cache declarations through per-model metadata.

- 6de38ac: Reclaim current-only document bodies on standalone retirement and persist document revisions separately so small deltas do not rewrite accumulated history. SQL commits prune obsolete revisions transactionally. JSONL uses synced immutable sidecars and a commit journal, automatically reclaiming superseded task checkpoints and document content during normal writes and recovery.

  The new physical layouts reject older JSONL files and SQL schemas without migration. Copy the JSONL journal and its adjacent `.parts` directory together while the store is closed. Logical StorageFormat feature versions and the Storage/Document APIs are unchanged.

  Automatic runtime activation, compaction waits, cancellation, and native joins preserve scheduler health failures instead of restarting an accepted request after restore fails. Explicit scheduler recovery and cold reopen remain available.

- 6de38ac: Store immutable prepared requests in run-owned documents and reference them from checkpoints and tool children. Retire request state after use and keep completed tool rounds as compact receipts.

  Store tool result content once and reconstruct provider messages and public views from the canonical entry. `ContextProjection.messages` exposes the original messages of a transcript entry for consumers such as the docs app. Nested invocation arguments and result bodies belong to the caller and retire with it; terminal nested observations retain status metadata.

  Activate storage format 12 before runtime admission. The new request, result, checkpoint, and nested invocation shapes replace the previous alpha formats.

- f96658e: Enable durable compaction for Codex subscription and Claude OAuth models, with pinned summary configuration, live credential refresh, narrow input-overflow recovery, reasoning-free summary transcripts, and model-specific Claude thinking replay policies. Measure the full planned prompt and tools, reusing normalized usage only for unchanged request prefixes. Codex summary budgets are explicit local acceptance bounds, not remote output limits.

### Patch Changes

- 01d1042: Default explicitly enabled and adaptive Claude thinking to summarized display for OAuth and API-key requests, while preserving explicit display overrides.
- d6d4b10: Render tool diagnostics with severity and message in model projections while preserving canonical result channels. Add the service-free `ToolResultRecord.read` API for committed tool results, restore storage aliases, and migrate the structured-result example and reading documentation.
- 8816ab0: Trace durable admission, commits, scheduler recovery and task execution, model stream attempts, deferred execution, tools, and compaction with named Effect spans. Include correlation IDs and attempt metadata, preserve Cause identity at fault-provenance boundaries, and preserve invocation tracing across captured task, tool, and streaming-provider contexts.
- d6d4b10: Preserve native Uint8Array and URL file data through prompt admission, context projection, prepared requests, recovery, subagent submissions, and compaction. Export the service-free PromptWire schemas for applications that serialize native prompts. Literal string data, provider options, and application JSON retain their original meaning.

  Summarize tool-content text without serializing image payloads into the text budget. Retain trailing captions, result identity and failure status, and omitted-media metadata. Persist usage baselines with native media so cold automatic compaction honors measured input tokens.

  Require logical runtime format 12 and reject nonempty older stores before runtime mutation. Physical SQLite and JSONL formats are unchanged.

- 7528e63: Disable strict JSON Schema mode by default for Codex and Claude OAuth tools so ordinary schemas with optional fields and open objects remain usable. Explicit per-tool strict-mode annotations still take precedence. Classify invalid function parameters as a non-retryable request error without exposing provider error details.
- d6d4b10: Let otherwise idle processes exit while Session document caches retain entries. Cache eviction preserves supplied clocks and cancellation, and falls back to sweeping during Session operations when the host cannot provide an unreferenced timer.

  Retire ordinary documents and terminal task documents from validated metadata without decoding obsolete bodies. Exact-version and policy checks, atomic settlement, staged changes, and rewindable history remain enforced. Custom storage adapters must provide `documentMetadata` for retirement of unstaged documents; unavailable metadata is rejected without a content fallback.

## 0.5.0

### Minor Changes

- ac61918: Add schema-backed task graphs, coherent conversation views and committed agent events. Native observers attach and publish on the Session line, recover bounded-buffer overflow with full rebases and drain final state on successful close. Retain actual nested tool receipts for cold readonly observation after parent retirement.

  Snapshots hydrate complete current state and briefly hold the native line. Buffers are bounded; total snapshot memory grows with conversation and task history. Events describe committed facts rather than every provider chunk or a replayable event archive.

- 91f1c56: Add runner-neutral scoped conformance cases for bound filesystem, directory/watch, process, spill, image and toolkit isolation services. Factories preserve service and error requirements; optional capabilities require explicit selection and unsupported capabilities fail visibly. Image checks use independent output inspection without importing native codecs into portable testing routes.
- 7b7129f: Add immutable bounded context projection with explicit legacy interpretation, ordered declarative section/tool facts, and projection-only repair of incomplete tool histories.
- 5d0dd15: Add a ready-made UTF-8 read tool with bounded bytes and lines, one-based selection, scoped file identity and visible continuation diagnostics.
- 5d0dd15: Add an optional Bun image preparation Layer with native first-frame decoding, EXIF normalization, exact resizing and encoding. Validate source JPEGs strictly with the optional sharp codec, preserve transparency or explicitly flatten white, and retain borrowed buffers until noncancellable native work completes. Older Bun runtimes return typed unavailable errors.
- fc3e7c6: Add caller permissions, separate model tool filters, explicit inherited toolkit registration selection, and leased tool admission with typed denials. Add optional or strict success channels, media-preserving nested receipts without controls, and validated model-call controls with a deterministic complete-round reducer. Existing value-style and optional result defaults remain compatible. Provider/nested runtime admission and durable run-loop control activation require the later versioned integration.
- 5d0dd15: Add a scoped backend-local file mutation coordinator that serializes canonical path aliases and releases permits on failure and interruption.
- 60aae13: Commit immutable prepared requests before provider calls and restore their saved configuration and binding contracts after recovery. Apply current agent, settings, registry and selected-model image limits at each new request boundary, with a storage reader barrier for the new persisted preparation format.
- cda77c8: Add ready-made file editing tools with validated disjoint replacements, repaired argument forms, byte-preserving exact edits, scoped mutation coordination and optional native rename replacement.
- cda77c8: Add image-aware Read handlers with explicit per-invocation model policy, source allocation limits, bound image preparation and schema-backed image metadata. Preserve portable text-only Read behavior and return a documented placeholder for models without image support.
- 5d0dd15: Add detached document materialization with separate stored and effective versions, current and historical Session reads, and scoped version-aware watches. Local Conversation reads and watches use the owning HarnessRuntime service and preserve target decoder requirements. Materialized reads never persist upgrades; exact reads and transactional acquisition retain their existing behavior. Migration-only watch frames expose changes to storage version even when the decoded value is unchanged.
- fc3e7c6: Add validated encoded document patches, economical update diffs and compact array splices. Document checkpoint predicates select bases using the final decoded candidate, ordered operations and prior persisted delta count. Splice persistence deliberately activates storage format 3 with minimum reader 3; legacy formats remain readable and older readers reject upgraded stores on reopen.
- f154c6a: Add durable manual, threshold and finite overflow compaction with pinned summary configuration, separate status, task handles, usage accounting, and stale-range checks at queued placement. Native OpenAI and Anthropic summary requests enforce the output budget, omit tools and explicit cache directives, and run without deferred execution. These cache controls do not disable automatic remote caching. Valid incompatible compaction facts activate storage reader 8; old processes must be stopped before activation.
- cda77c8: Add a scoped standalone TaskScheduler for registered custom tasks over Session coordination. Compile typed executable closures during registration, retain leased service contexts, atomically commit phase checkpoints and memos with domain writes, normalize terminal results, resume persisted running checkpoints, and reject stale invocation writes. Require the exact supported StorageFormat capability before creation or dispatch and reconcile commit/registry wakes without blocking on observers.

  Expose explicit typed host boundaries for ownership supervision, child joins, cascading abort, restart abandonment, conversation access and hooks. The standalone scheduler does not replace the legacy Harness.run executor; those integrations remain required separately. Include a real two-phase JSONL cold-reopen example and focused runtime and type proofs.

- d8b3b82: Activate explicitly selected deferred model execution with durable handles, absolute polling deadlines, finite expiry and retry budgets, and captured remote cancellation after the durable abort fence. Recovery polls the saved handle without repeating preparation or creating another response; an interrupted create without a saved handle remains an inspectable unknown external outcome. Deferred work requires storage reader 7; ordinary and legacy missing-selection requests remain native streaming.

  Abort saves remote cancellation success, failure or timeout in the task's aborted result and settles locally within the cancellation wait budget. Runtime close joins tracked cleanup before releasing its resources and can wait for an uninterruptible custom callback. Caller-owned model and client Layers must outlive the runtime. Preserve observed usage when a native deferred publication is deterministically rejected.

- 7b7129f: Add schema-backed run membership and private durable inbox boundary helpers. The foundation selects passive writes before independently limited steering and follow-up queues, retains state across reopen, and rolls back caught helper failures. Production admission and scheduling integration follows separately; the existing scheduler remains authoritative.
- 35f0bda: Use native model streams during normal durable execution. Expose the last committed partial through the portable LiveState document, flush progress before atomic response settlement, and preserve interrupted partials during cold recovery without executing incomplete tool parameters. Record known and unknown physical attempt receipts independently of immutable request preparation. Persist finite absolute retry deadlines for typed transient provider failures and apply current retry policy only to new decisions. Streaming and retry phases require storage format 6; earlier readers refuse upgraded stores before replay. Deferred polling remains separate.
- 52d35e2: Add native programmatic tool calls from tools and registered tasks, with schema-backed receipts, owned child cleanup, durable key reattachment and once-only accounting. Nested work activates reader protection 10 before admission. Final-only progress acknowledges local report acceptance; the final invocation receipt acknowledges persistence.
- bf68aa5: Activate durable follow-up and steering batches, passive write/reset submissions, automatic saved-handle resumption, and supervised conversation cancellation. Grouped run membership and canonical context boundaries require storage format4; older readers refuse the container before replay, while legacy single-input checkpoints remain recoverable.
- fc3e7c6: Add scoped durable tool progress with bounded incremental UTF-8 retention, coalesced persistence, byte-rate throttling, bounded detail acknowledgements and atomic receipt settlement through the invocation's gated commit. Expose committed-state recovery and post-hook visible-text retention while preserving media, details and structured output. Legacy runtime reporting integration remains a required follow-up.
- 9ca7c2a: Execute model tool rounds as native durable child tasks, with whole-round sequential policy, parallel all-settled joins, ordered result slots and once-only controls. Settle final progress, results, receipts, usage and child outcomes atomically; retain accepted progress during cancellation and avoid repeating unsafe or committed work during recovery. New tool-round facts activate storage reader 9, so older processes must stop before activation.
- 7b7129f: Add a replaceable ImagePreparation service and scoped codec contract with owned result bytes, validated dimensions and base64 budgets, all-eight-orientation policy, first-frame transforms and explicit alpha handling. Native Bun, Node/sharp and WASM codecs follow separately.
- 5d0dd15: Add a ready-made literal UTF-8 write tool with parent directory creation, typed mutation outcomes and unsafe replay policy.
- 91f1c56: Add optional schema-backed deferred model descriptors and a native OpenAI background Responses adapter using the caller's bound transport. Expose typed start, poll and cancel operations with serializable handles, captured services and strictly increasing absolute poll deadlines. Runtime persistence, retries and cancellation fencing require later integration.
- fc3e7c6: Provide native registered-task ownership, durable joins, completion holds, cascading cancellation, background snapshots, and restart abandonment cleanup. Add stored configuration inheritance for owned conversations, ordered receipts, and bounded task graph observation. External conversation transport and legacy run adoption remain separate integrations.
- 5d0dd15: Add the optional images-node Layer with scoped sharp decoding, portable BMP fallback, bounded pixel allocation, complete GIF/WebP frame validation, EXIF normalization, exact resizing and original-source encoding retries.
- cda77c8: Add explicit transaction retirement from document metadata targets without reading or decoding obsolete content. Address, incarnation, fork policy and fresh version checks preserve exact-version APIs. Retirement shares atomic persistence, savepoints and scoped watch termination; unloaded targets require the storage metadata capability.
- 5d0dd15: Add explicit persisted restart policies and durable abort intent/reasons with legacy resume defaults, immutable policies/receipts, and preparation helpers. The production scheduler is unchanged; older readers discard additive task fields, so safe activation still requires a storage format downgrade guard.
- 5d0dd15: Add the optional images-wasm Layer with packaged Photon WASM and environment-supplied loaders, scoped image handles, PNG/JPEG/GIF/WebP/BMP decoding, transparency preservation and typed failures.
- 91f1c56: Add read-only positional context planning from captured registry bindings, serializable ordered declaration drafts, and explicit provider positioning or fallback. Versioned protocol values preserve known legacy managed self-heads through an activation cutoff and retain that cutoff across forks. Runtime activation and provider transport integration remain separate consumers.
- cd352f8: Add optional captured-backend materialized document reads, historical reads and version-aware watches with a replaceable HTTP transport. Client codecs retain their decoding services; bounded full-state watches report rebase and retirement explicitly. Transport operation leases join connected callback cleanup before acknowledgement and expose expiry, disconnection and framing failures without changing persisted document formats.
- cda77c8: Add bounded incremental UTF-8 output retention and ready-made Bash and PowerShell tools with schema-backed results, literal argv, toolkit-local preparation, progress, typed failures and scoped processes. Add a raw-output spill provider with explicit persistent or toolkit lifetimes and revalidation. Native process cleanup is explicitly best effort; strict owned-tree requirements need an injected capability. Durable progress throttling remains a separate integration.
- bf68aa5: Add bounded compaction range selection, explicit token estimation with optional native tokenization, immutable summary request inputs, and clean summary validation. These policy foundations reuse Settings and canonical context projection; durable compaction execution and placement remain separate integration work.
- 5d0dd15: Add a standalone scoped Registry service with typed toolkit, extension and task acquisition, atomic immutable snapshots, explicit binding selection and invocation leases that retain replaced resources until running calls finish. Harness runtime adoption, permissions and ordered wrappers remain separate consumers.
- fc3e7c6: Add explicitly bound directory paging and file watch services with replaceable adapters. Node directory handles preserve opened identity; Bun uses scoped native libc handles on supported Darwin/Linux ABIs. Portable array enumeration declares its full allocation and snapshot behavior. Watches report ancestor and recursive coverage, apply per-target exclusions, request rescans on bounded queue overflow, and release subscriptions on close or interruption. Native capability failures remain typed; optional Clock-driven polling requests explicit rescans without hidden host fallback.
- 5d0dd15: Add a scoped native filesystem positional reader with bounded allocation, serialized cursor access and semantic errors.
- fc3e7c6: Add independently scoped tool and prompt-section wrappers, validated atomic registry publications, and leased event-specific hook composition with typed task phase observers. Typed wrapper bindings retain transformed codec contracts separately from delegated handlers. New dispatch defaults to chain composition; existing Harness dispatch remains unchanged.
- cda77c8: Add an explicit StorageFormat service and guard that persist container and minimum-reader compatibility before new-format execution. Built-in memory, SQL and JSONL layers supply the capability; custom adapters must explicitly supply a matching capability. Preserve legacy containers until deliberate activation, reject unsupported future readers before replay, and revoke uncertain writers until cold reopen reconciles persisted state. Add optional metadata-only document lookup for fresh schema-version checks without content materialization.
- 3889e3f: Extend public storage conformance with prototype-sensitive JSON, lossless indexed string identities, ordered multi-page streams, multigeneration fork cutoffs, waiting/completing task and passive-write records, and opt-in long replacement history tails.
- 91f1c56: Run conversation submissions and registered custom tasks through one scoped scheduler. Expose the runtime registry and scheduler, retain independently bound services, and adopt existing conversation checkpoints without losing committed tool results or repeating unsafe work.
- 306a8e8: Add supervised foreground and background subagent conversations to native task and tool invocations, with durable handles, inherited stored agent selection, and caller-scoped conversation controls.
- 5d0dd15: Add typed, read-only task definition/phase/codec compatibility inspection and a scheduler-private atomic migration boundary. This prepares canonical registry, invocation gate and memo integration without dispatching generic tasks or claiming complete scheduler parity.
- 7b7129f: Add ToolkitFileSystem and ToolkitProcess Layers that bind native Effect filesystem, path and child process services to an explicit backend root. Preserve scoped cleanup, backend errors, literal argv and caller cwd/environment overrides. Support an explicit lexical root policy; this is not a security sandbox. Canonical per-toolkit registry invocation selection remains a separate integration dependency.
- 7b7129f: Add schema-backed executable task definitions, typed outcomes, scoped Layer provisioning, and invocation-bound TaskExecution contracts. Definition validation, owned JSON codecs and pure version transforms are available independently; scheduler registration and durable dispatch require runtime integration.
- 91f1c56: Add typed durable usage receipts for request attempts and nested calls, with atomic transaction outcome recording and session aggregation independent of transcript entries and task-document retirement. Preserve optional cache-tier, reasoning, total and tool counters, declared tier prices and reported cost components while retaining legacy flat accounting records. Request, nested-call and compaction producers must explicitly record receipts when integrating these APIs.
- 7b7129f: Add a schema-backed Settings service with immutable policy snapshots, atomic validated updates, revision tracking and explicit defaults. Define queue, tool-round, retry, progress, compaction, request-hint and idle-retention policies for subsequent runtime integrations while keeping execution environments bound to toolkit services.

### Patch Changes

- 27b7b97: Keep rejected native write evidence on the escaping checkpoint operation so caught callback errors cannot settle unrelated read or nested failures. Preserve recoverable checkpoints and failure causes while retaining healthy rejected response settlement.
- 4cde46d: Preserve the original context interpretation while compaction prepares a summary, so an unchanged appended suffix does not invalidate the selected range.
- fc3e7c6: Preserve owned conversation admission when its owner enters held completion in the same transaction, while retaining terminal-owner and abort rejection.
- 342d46b: Validate pinned deferred cancellation bindings, restore saved request configuration, and own newly acquired callback and handle-codec resources in the cancellation operation scope.
- 0e86807: Keep installed hooks bound to the callback captured when the hook is created, including mutations during asynchronous registration acquisition.
- 5505085: Retain reported model usage when a deterministic stream checkpoint publication is rejected and failed-attempt settlement succeeds, without repeating the provider call.
- 9a4bf34: Retire obsolete native documents through validated headers without decoding missing or malformed bodies. Preserve stored revisions and atomic retirement while keeping ordinary content reads strict.
- 29a7fbc: Delay native watch startup coverage until the settling interval has elapsed so its rescan covers writes during native watcher startup.
- e0f6e06: Round the strict portable JPEG decoder's secondary resolution allowance outward so valid baseline and progressive images pass at their exact source pixel budget, while retaining the exact header pixel guard and independent decoder memory cap.
- ce2201f: Account for padded JPEG coefficient and component storage during strict portable validation, allowing narrow baseline and progressive images at their exact source pixel limits while retaining decoder memory caps.
- 8f45d70: Capture registration ID, contract version and enabled state before acquisition so caller mutations cannot publish an invalid owner or change reload selection.
- 364b6f3: Retain accepted request checkpoints when configuration restoration fails before provider invocation. Report the failure and stop automatic retries until explicit recovery, preserving the saved prompt and configuration without replaying preparation callbacks.
- c24e633: Validate every retained document revision header and sequence against native allocation bounds before metadata lookup and retirement commit, while preserving opaque obsolete bodies.
- 6f682d5: Read SQLite allocation counters losslessly at exhaustion without changing the stored format. Check repeated safe-integer allocator exhaustion and actual structural storage records after reopening. Add native binding consumers for historical forks and fresh-process host reconstruction.
- 5d0dd15: Keep provided task Layers alive in their registration scope until the final registry lease closes, preserving service requirements and construction errors.
- 8f45d70: Validate every APNG frame and select the first animation frame on its transparent canvas. Reject truncated JPEG entropy with a strict portable validator, and keep sharp handles and borrowed bytes alive until pending work settles during explicit scope closure.
- 539c9e3: Retain committed handler costs when cold tool recovery denies a missing, revoked, or incompatible binding, without repeating execution or exposing the saved result.
- 4ba35c9: Keep abandoned unsafe tool subagent recovery unanswered without requesting a model continuation, preserving the original paid child and interrupted tool accounting.
- b701e78: Validate final request preparations before publishing context facts or activating their storage format. Commit accepted facts and their immutable checkpoint together, and revalidate positional markers against the final hook-modified native prompt.

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
