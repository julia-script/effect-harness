---
'effect-harness': patch
---

Preserve native tool file bytes across JSON serialization so Anthropic receives decoded text documents rather than base64 strings. Literal string content retains its original meaning.
