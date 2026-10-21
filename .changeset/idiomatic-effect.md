---
'effect-harness': patch
---

Use Effect's own helpers throughout: `Effect.fnUntraced` for functions that only run a generator, `Equal.equals` and `Predicate` guards instead of hand-rolled ones, and error reasons checked by their `_tag`.
