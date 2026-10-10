---
'effect-harness': minor
---

Add ordered transactional conversation initializers to Session and the local Harness runtime. New roots, independent conversations, forks and configured raw Transaction creation commit their required documents and callback writes together. Failed or interrupted initialization leaves prior transaction drafts intact, including when callers catch the failure. Existing roots and reopened conversations skip initialization; notification hooks retain their post-commit behavior.
