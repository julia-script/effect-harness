---
'effect-harness': patch
---

Normalize SQL COMMIT and ROLLBACK errors into uncertain StorageError failures and require reopening direct Storage after these failures.
