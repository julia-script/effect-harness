---
'effect-harness': minor
---

Add optional captured-backend materialized document reads, historical reads and version-aware watches with a replaceable HTTP transport. Client codecs retain their decoding services; bounded full-state watches report rebase and retirement explicitly. Transport operation leases join connected callback cleanup before acknowledgement and expose expiry, disconnection and framing failures without changing persisted document formats.
