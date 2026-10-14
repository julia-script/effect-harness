# Deferred tool discovery

Hosts can mark native tool declarations with `deferred: true` in `Tool.make` or `Tool.makeResult`, or annotate an Effect tool with `Tool.Deferred`. The default is false. Install `ToolSearch.toolkit` together with `ToolSearch.layer` to offer the optional `tool_search` function. The [typechecked example](../apps/example/src/tour/ToolSearch.ts) combines search with calendar, document and email tools.

With default selection, the first model request offers permitted nondeferred tools. A model invokes `tool_search` with `{ query, limit? }`; its result lists matches in `structuredOutput.loaded` and `details.loaded`, with a short model-facing description. Successful model-issued results contribute `control.addTools` to the existing complete-round barrier. The next request offers those tools, and a subsequent ordinary tool call executes the operation. Search does not execute matches or write conversation state independently. A host can register tools from its MCP integration through the same declarations; this package does not own the MCP transport.

## Selection and permission

`agent.tools` bounds both model and programmatic callers. `agent.modelTools` controls presentation and has one representation:

- Omitted, or `{ mode: 'default', names: [] }`: offer permitted nondeferred tools, then loaded names.
- `{ mode: 'explicit', names: ['tool_search', 'someTool'] }`: offer only these names in this order, including deferred tools deliberately selected by the host.

Successful additions append to `names` without duplicates and record their owner/version in `modelTools.bindings`. Explicit selections can grow through authorized controls too. Configure a new selection to remove previous offers and their bindings; clearing the override restores the host's default configuration. Programmatic `tools` callers can invoke permitted deferred tools before model discovery. They cannot call model-only `tool_search`, and their results cannot activate tools.

Search examines only deferred declarations that are permitted for the model but not already offered. Disabled or unselected registrations, agent restrictions, caller restrictions and incompatible bindings all apply. An excluded ordinary tool does not become deferred. Knowing an unoffered deferred name does not authorize a model call. Duplicate names resolve to one canonical owner before permissions are checked; denial never reveals a shadowed declaration. `agent.extensions` filters that owner without changing resolution order. Use `toolkits.registrations` or explicit bindings to choose a different registration.

`ToolCatalog.deferred` exposes names, descriptions and parameter schemas for the current invocation. It uses the same leased generation and effective policy as admission, without opening other tools' resources. Access expires when the handler returns. Schema conversion failure is a typed `ToolCatalogError`, not an omitted candidate. The catalog contains no handlers, credentials or service contexts.

## Durability and availability changes

Activation and the applied-round marker commit together. Reopening after result persistence resumes the barrier without repeating a completed search or changing declaration order. Only successful model-issued receipts activate tools; failures, blocked calls, cancelled calls and programmatic/nested calls do not. Hooks must pass final result/control validation.

Selections survive reopen, compaction and history reset. Historical forks inherit the selection at the fork entry and then evolve independently. Explicit configuration changes remain authoritative. A later model selection change invalidates in-flight additions; current agent and registration restrictions are checked again inside the activation transaction.

A search is scoped to its leased generation. If registration availability changes before a resumed barrier, unavailable or differently bound additions are rejected. If replacement happens after a barrier has acquired its lease, any committed addition still records the original owner/version; later requests cannot silently invoke the replacement. Absent, disabled or incompatible loaded tools disappear from offers. Restoring a compatible registration can expose the saved selection again. Changing to another owner or contract version requires explicit configuration.

## Ranking and input

`ToolSearchRanking` is a pure, local BM25 implementation (`k1 = 1.2`, `b = 0.75`) over immutable documents. It splits camel case, lowercases alphanumeric terms, removes English stop words, applies simple singularization, and uses distinct query terms. Names, descriptions, parameter property names and schema descriptions contribute. Positive scores are ordered descending; input order breaks ties. The default limit is eight; limits must be integers from 1 through 64. Blank queries are invalid; stop-word-only queries return no matches. This is English lexical matching, with no semantic or multilingual quality claim.

Schema indexing follows properties, array items, variants and local references, including `$defs`. It visits at most 2,048 nodes, depth 64 and 32,768 text characters per declaration. Cycles terminate, remote references are never fetched, and unreachable definitions are not indexed. `schemaText` reports when these bounds truncate indexing.

## Subscription providers

Codex and Claude OAuth receive search as an ordinary application function, followed by an expanded top-level tool list. No server-side search protocol, `defer_loading`, inline tool changes or tool-reference result format is required. The prepared declaration order is preserved on the wire, including Claude's tool-name normalization.

Changing top-level tools can invalidate a remote cache prefix. Declaration ordering does not guarantee cache reuse. Claude's model-specific retained-thinking policy is shared with subscription compaction and pinned in provider configuration; supporting models use documented server mismatch handling. Stored reasoning and valid tool cycles remain intact. See [subscription providers](subscription-providers.md) for supported policy details. Protocol fixtures verify request construction; they do not establish live OAuth endpoint access.
