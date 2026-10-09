---
'effect-harness': patch
---

Prepare all fork document copies before staging the child conversation. Catching a fork validation error inside a successful Session callback no longer commits orphan copies, while unrelated callback changes remain commit-able.
