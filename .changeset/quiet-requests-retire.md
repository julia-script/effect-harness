---
'effect-harness': minor
---

Store immutable prepared requests in run-owned documents and reference them from checkpoints and tool children. Retire request state after use and keep completed tool rounds as compact receipts.

Store tool result content once and reconstruct provider messages and public views from the canonical entry. `ContextProjection.messages` exposes the original messages of a transcript entry for consumers such as the docs app. Nested invocation arguments and result bodies belong to the caller and retire with it; terminal nested observations retain status metadata.

Activate storage format 11 before runtime admission. The new request, result, checkpoint, and nested invocation shapes replace the previous alpha formats.
