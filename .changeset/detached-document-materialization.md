---
'effect-harness': minor
---

Add detached document materialization with separate stored and effective versions, current and historical Session reads, and scoped version-aware watches. Local Conversation reads and watches use the owning HarnessRuntime service and preserve target decoder requirements. Materialized reads never persist upgrades; exact reads and transactional acquisition retain their existing behavior. Migration-only watch frames expose changes to storage version even when the decoded value is unchanged.
