---
'effect-harness': minor
---

Tool handlers can require the `Harness` service: the tool task provides the Harness it runs under to each call (`Toolkit.Provided`), so a tool can create, submit to and wait on conversations without the Harness in its layer.
