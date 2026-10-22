---
'effect-harness': minor
---

Runs move to fallback models. A conversation's `fallbacks` (or `Settings.fallbacks`) take over for the rest of a run when its model is `Unavailable` (out of quota, or its credentials stopped working), unknown, out of retries, or asks to wait longer than `retry.maxDelayMs`. The run appends a `$fallback` entry and sets `$live.run.fallback`; the next run starts on the conversation's model again. Quota and authentication failures, and missing credentials, now fail with the new `Unavailable` reason instead of `Rejected`.
