---
'effect-harness': patch
---

Keep Claude thinking signatures. Anthropic sends a signature with a thinking delta and redacted thinking with the block's start, but only metadata on a part's end was kept, so Claude's thinking was never sent back, even to the same model.
