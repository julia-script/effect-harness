---
'effect-harness': minor
---

Replace the previous package/application layout with a single publishable root package and concept-oriented public subpaths. This is the breaking pre-1.0 release from 0.5 to 0.6; it supersedes the queued notes for the old architecture.

- Import portable core namespaces from `effect-harness`, or use lowercase subpaths such as `effect-harness/client`, `effect-harness/harness`, `effect-harness/model`, `effect-harness/storage`, and `effect-harness/tool`. The previous uppercase and `provider-*` subpaths are removed, without compatibility re-exports.
- Compose `HarnessServer` and `HarnessClient` through memory or WebSocket RPC transports, with schema-backed tools, extensions, model providers, compaction, views, durable tasks, and request deduplication.
- Use the refactored memory, directory-based JSONL, or host-supplied SQLite/PostgreSQL storage layers. Account and credential services separate provider configuration from secrets; OpenAI, Codex, and Anthropic integrations have dedicated optional subpaths. Cloudflare hosting runs the harness in a Durable Object.
- Rename built-in entry/document/task kinds and tool-output discriminants from `pi.*` to `$*`, including `pi.user` → `$user`, `pi.agent` → `$agent`, and `pi.tool-output` → `$tool-output`. The `$` prefix is reserved for built-ins; custom input-entry kinds must not start with it. Update consumers that inspect transcript kinds, task names, document keys, or tool outputs. Existing `pi.*` stores are not migrated: use a fresh store or perform an explicit application-owned migration before opening them with this release.
- Ship compiled ESM, declarations, source maps with their corresponding source files, and the upstream attribution notice. Optional provider/platform peers are not required to import the portable core. Build clean artifacts during packing and verify the installed tarball with an isolated Node/TypeScript consumer in CI.
- Restore Changesets version PRs and CI-gated npm trusted publishing with provenance for the root package.

Developer compiler/linter patching is now explicit through `bun run setup`, rather than a consumer install lifecycle hook. Nested-call state declarations no longer require consumers to enable `exactOptionalPropertyTypes`; strict consumers can typecheck the installed package without `skipLibCheck`.
