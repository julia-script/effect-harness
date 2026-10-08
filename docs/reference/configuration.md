# Configuration

`Harness.layer(options)` accepts an initial agent, host settings, default cwd, a reporting callback and installed bound task definitions. Its required model catalogue, registry and Persistence service are supplied through Layers.

The `settings` field accepts `Agent.Settings.Input` and is validated when opening the harness. Invalid settings fail with `SchemaError`.

## Agent configuration

`Conversation.configure` accepts `Agent.State.Change` and updates model reference, thinking mode, selected extensions/tools, instructions and cwd. Undefined fields preserve existing values; null clears supported overrides. Initial agent configuration is applied when a conversation is created. Reopening uses committed agent overrides; host settings are supplied when opening each harness.

Provider catalogue entries declare model identity, context window, maximum output tokens and optional reasoning/cache/price capabilities. Request options are validated against those declarations. Model IDs and limits are application-supplied.

Unknown model references fail with `ModelError` carrying `ModelNoModelError`. Unsupported request options fail with `ModelError` carrying `ModelUnsupportedError`.

## Tool policy

Tool registration declares replay as `safe` or `unsafe`. Recovery reruns only when the saved and currently installed policies permit it. Concurrent tool execution uses the host's toolExecution setting. Coding tools retain explicit filesystem/process environments and output limits.

`ToolRegistration.bind(toolkit, metadata?, requestServices?)` merges native Tool annotations under `ToolRegistration.Metadata` with per-name overrides; explicit binding metadata wins. It captures handlers and host dependencies while Invocation and ToolCall remain dynamic. Missing declared request services fail with `ToolError` carrying `ToolUnavailableError`.

## Retry and compaction

Agent defaults enable retry with three retries, a two-second base delay and a one-minute cap. Retry checkpoints save an absolute deadline before sleeping. The example disables retries for deterministic execution.

Compaction defaults reserve 16,384 tokens, retain 20,000 recent tokens and use a 32,768-token background threshold. When enabled, turn preparation schedules background compaction at the configured threshold and waits for compaction near the model's context limit. A classified context overflow can trigger compaction before another request. `Conversation.compact` admits manual summarization. A saved summary is placed only while its selected cutoff and context head remain valid.

Progress defaults pace partial model and tool output at 100 milliseconds. The example sets both intervals to zero.

The TypeScript API comments and Schema definitions specify individual field types and bounds.
