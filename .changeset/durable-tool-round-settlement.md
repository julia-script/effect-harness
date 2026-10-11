---
'effect-harness': minor
---

Execute model tool rounds as native durable child tasks, with whole-round sequential policy, parallel all-settled joins, ordered result slots and once-only controls. Settle final progress, results, receipts, usage and child outcomes atomically; retain accepted progress during cancellation and avoid repeating unsafe or committed work during recovery. New tool-round facts activate storage reader 9, so older processes must stop before activation.
