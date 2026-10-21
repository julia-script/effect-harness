---
'effect-harness': minor
---

Add `HarnessWorkflow` to `effect-harness/cloudflare`: the loop as durable steps of a Cloudflare Workflow deployed with Alchemy. `make` is a Workflow body that answers each run's input in one retried step, and `turn` is that step on its own, returning the answer text or the reason the run went unanswered, and failing with `TurnFailed` when the loop does.
