---
'effect-harness': patch
---

Remove the Bun requirement from npm package preparation and declare the JSONL Bun adapter's platform dependency as an optional peer. The core and portable public imports remain independent of runtime-specific adapters.
