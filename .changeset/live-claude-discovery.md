---
'effect-harness': minor
---

Discover Claude models and reasoning capabilities lazily through OAuth-authenticated Models API requests instead of a bundled inventory. Preserve discovery service requirements and use the existing credential refresh lifecycle.

Allow newly discovered models to use ordinary thinking replay without a model-family allowlist. Keep known server-side mismatch handling and expose per-model replay compatibility overrides, pinned in durable configuration.

The API-key catalog's `apiUrl` now denotes the API origin, matching OAuth inference and discovery.
