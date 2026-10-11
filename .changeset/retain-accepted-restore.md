---
'effect-harness': patch
---

Retain accepted request checkpoints when configuration restoration fails before provider invocation. Report the failure and stop automatic retries until explicit recovery, preserving the saved prompt and configuration without replaying preparation callbacks.
