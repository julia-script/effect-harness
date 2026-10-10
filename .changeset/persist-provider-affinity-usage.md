---
'effect-harness': minor
---

Persist globally unique conversation affinity through reopen and allocate a fresh identity on forks. Expose request-scoped ProviderAffinity to custom adapters and default supported OpenAI prompt_cache_key without overriding caller options.

Persist model token usage, caller-declared pricing snapshots and opt-in tool spend with completed entries and recovery checkpoints. Add atomic conversation/session usage queries with fork deduplication, explicit unknown counters and costs, separate currencies, and legacy history reporting. Older transports may omit accounting queries; older decoders discard new usage fields on downgrade.
