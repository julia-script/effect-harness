---
'effect-harness': minor
---

Commit immutable prepared requests before provider calls and restore their saved configuration and binding contracts after recovery. Apply current agent, settings, registry and selected-model image limits at each new request boundary, with a storage reader barrier for the new persisted preparation format.
