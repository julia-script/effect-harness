---
'effect-harness': patch
---

Disable strict JSON Schema mode by default for Codex and Claude OAuth tools so ordinary schemas with optional fields and open objects remain usable. Explicit per-tool strict-mode annotations still take precedence. Classify invalid function parameters as a non-retryable request error without exposing provider error details.
