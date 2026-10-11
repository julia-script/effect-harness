---
'effect-harness': patch
---

Keep rejected native write evidence on the escaping checkpoint operation so caught callback errors cannot settle unrelated read or nested failures. Preserve recoverable checkpoints and failure causes while retaining healthy rejected response settlement.
