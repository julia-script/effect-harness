---
'effect-harness': patch
---

Offer a tool whose parameters are `Schema.Struct({})` an empty object schema. Its JSON Schema had no `type`, and Anthropic refused every request that carried the tool.
