---
'effect-harness': patch
---

OpenAI, Codex and Anthropic API-key requests send tool schemas as non-strict. `@effect/ai-openai` and `@effect/ai-anthropic` default tools to strict mode, which refuses schemas with optional properties, so Codex rejected every request carrying the bundled tools with `Invalid schema for function 'read': ... additionalProperties must be false for a strict schema`. Anthropic OAuth already sent them non-strict.
