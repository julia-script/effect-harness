---
'effect-harness': minor
---

Replace per-account provider declarations with schema-defined integrations and a dynamic Providers manager. Separate public instance persistence from credential storage, allow unauthenticated instances, and return secrets from explicit login helpers for consumer-owned updates. Carry instance identity and revision through model selection and durable recovery.

Add a configurable OpenAI-compatible Chat Completions provider factory with independent metadata discovery and a static-catalog option. Codex and Claude definitions support live catalogs and custom metadata callbacks. Remove profile-keyed credentials and the former provider/catalog construction APIs; stored credential envelopes and prepared account bindings require explicit migration or repreparation.
