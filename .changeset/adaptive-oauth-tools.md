---
'effect-harness': patch
---

Anthropic OAuth models listed in `adaptiveModels` send tool schemas non-strict again. 0.9.1 moved the non-strict setting into the shared Anthropic config, and the adaptive-model path rebuilt the config without it, so those requests carried `strict: true` tools.
