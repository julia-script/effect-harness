---
'effect-harness': patch
---

Preserve the complete scoped tool failure cause when it includes a defect, interruption, or infrastructure error. A declared domain failure no longer hides an accompanying cleanup defect or infrastructure failure, while domain-only failures still become tool results.
