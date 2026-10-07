# Configuration

This reference covers host conversation policy, persisted agent overrides, tool metadata and model catalogue declarations. Native provider request options retain the provider's own schema and are documented by its Effect AI module.

## Host policy

`Conversation.layerConfiguration(options)` provides Configuration. Its options are:

| Option            | Default              | Contract                                                                |
| ----------------- | -------------------- | ----------------------------------------------------------------------- |
| `settings`        | Agent defaults below | Partial SettingsInput; validated during construction                    |
| `toolConcurrency` | `16`                 | Positive safe integer; sequential rounds use one permit                 |
| `cwd`             | `'.'`                | Default invocation working directory                                    |
| `report`          | No-op Effect         | Host diagnostic callback                                                |
| `created`         | No-op Effect         | Atomic conversation-creation callback with transaction and conversation |

Invalid construction or `Configuration.updateSettings(input)` fails with SchemaError. An invalid update retains the previous policy. An update replaces policy after expanding defaults; it is not an incremental merge with the previous settings. Configuration is host policy; persisted conversation overrides are separate.

| Settings field                | Default                               |
| ----------------------------- | ------------------------------------- |
| `extensions`                  | All installed extensions when omitted |
| `stream`                      | Empty options object                  |
| `retry.enabled`               | `true`                                |
| `retry.maxRetries`            | `3`                                   |
| `retry.baseDelayMs`           | `2 seconds`                           |
| `retry.maxAgentDelayMs`       | `1 minute`                            |
| `compaction.enabled`          | `true`                                |
| `compaction.reserveTokens`    | `16384`                               |
| `compaction.keepRecentTokens` | `20000`                               |
| `compaction.backgroundTokens` | `32768`                               |
| `progress.partialIntervalMs`  | `100 millis`                          |
| `progress.outputIntervalMs`   | `100 millis`                          |
| `toolExecution`               | `'parallel'`                          |
| `steeringMode`                | `'one-at-a-time'`                     |
| `followUpMode`                | `'one-at-a-time'`                     |

Retry counts and token budgets are nonnegative integers. Duration inputs normalize to native Duration values. `toolExecution` also accepts `sequential`; steering/follow-up modes also accept `all`.

A prepared request pins its payload. Live policy changes can affect later retry decisions. Once a retry decision and absolute deadline have been admitted, replay retains them; a later policy change does not revoke that timer.

## Conversation overrides

Agent.State fields are `model`, `thinking`, `extensions`, `tools`, `instructions` and `cwd`. Model references contain `provider` and `modelId`. `Conversation.configure(id, change)` commits the overrides.

In Agent.Change, `undefined` preserves a field and `null` clears it. A supplied field replaces the prior value wholesale. Clearing an override allows the relevant host default to apply.

Extension selection accepts an exact list or `{ add?, remove? }` edits against host defaults. Removal wins; first occurrence order is retained. Missing installed names are ignored during resolution. Tool selection accepts an exact list or `{ remove: [...] }`; omission offers all resolved tools.

Conversation creation/fork inherits agent configuration according to its document history. Provider affinity is initialized separately; its UUID is distinct from authorization account and Workflow execution identities.

## Tool policy

`Tool.bind(toolkit, metadata?, requestServices?)` merges native Tool.Metadata annotations with per-name overrides. Explicit binding metadata wins. It captures handlers and host dependencies; Invocation and ToolCall remain dynamic. Missing declared request services fail at invocation with ToolUnavailable.

| Metadata field | Default / contract                                                                                                            |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `replay`       | `'unsafe'`; `'safe'` permits repeating an unsettled intent's body                                                             |
| `execution`    | Parallel unless a selected registration requests sequential execution; one sequential registration makes the batch sequential |
| `output`       | Overrides Output defaults: 50 KiB, 2,000 lines, head retention                                                                |
| `outputWindow` | Optional bounded-window reporting                                                                                             |
| `repair`       | Optional argument normalization in Invocation context                                                                         |
| `project`      | Optional mapping of native result, encoded value and failure flag to ToolResult                                               |

Parallel tool results commit as calls finish. `afterTools` receives the settled batch in provider call order, including unavailable tools and their committed entry IDs. It does not receive completion order.

ToolResult separates model-facing `content`, private `details`, `diagnostics`, `usage`, `control` and `isError`. Controls can request terminate, reset and addTools after settlement. Encoding includes content and rendered diagnostics in the native Prompt envelope; details, controls and usage remain separate metadata.

## Model configuration

A Descriptor contains its provider/model reference, native model, context window, output limit and `configure` function. Optional fields provide deferred execution, prompt normalization, usage interpretation, token estimation and error classification. Capabilities and prices are declarations, not discovery or entitlement checks.

Provider Catalog entries require a nonempty model ID, positive context/output limits and output limit no larger than the context window. Duplicate model IDs are rejected by provider catalogue constructors. Unknown provider/model references fail with ModelNoModel. Invalid or unsupported request options fail with ModelUnsupported.

OpenAI entries can declare reasoning efforts and prompt-cache options. Anthropic entries can declare adaptive/budget thinking, efforts and caching. Prices are USD per million tokens; unknown price/usage components remain unknown. The adapters retain captured native clients while applying per-request native configuration.

Config constructors evaluate during Layer construction using the active ConfigProvider. `Config.unwrap` combines an options object containing configs; mapping the constructor produces an Effect of a Layer, and `Layer.unwrap` builds that selected Layer. They do not evaluate configuration when the factory is merely declared.

## Context estimates

Context estimation uses the newest measured assistant usage after the active marker and estimates only following messages. Descriptor.estimate can supply provider tokenization. The fallback measures visible text/reasoning, tool names/JSON parameters and tool-result content at roughly 3.5 characters per token; each file/image part contributes 4,800 character-equivalent units first. Tool-content envelopes and managed system patches contribute their semantic visible content rather than serialized byte arrays.

These are selection/compaction estimates, not billed-token measurements. Reset, compaction and context edits determine the active transcript and managed patch projection.

## Coding tool behavior

`read` returns bounded text windows; recognized images report `unsupported_image`. `write` creates parent directories. `edit` requires a unique match and preserves line endings/BOM while recording a diff. Shell tools use bounded output with spill files and support timeout and scoped cancellation.

The model-facing `bash` timeout is numeric seconds. Library timeout/polling options accept `Duration.Input`. Nonzero exits appear in shell results; spawn, timeout and output-callback failures use typed errors.

Text truncation defaults to 2,000 lines and 50 KiB and respects UTF-8 boundaries. Shell output retains bounded windows and uses spill files for full output. Mutation locks serialize each canonical target path and remain leased until an admitted write settles, including cancellation. Recognized images are not decoded or resized by the read tool.
