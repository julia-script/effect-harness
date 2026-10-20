---
'effect-harness': patch
---

Watches over `HarnessTransport.layerWebSocket` open again as soon as a dropped connection is back, instead of waiting out a backoff of up to 5 seconds. The transport now also provides `HarnessTransport.Connections`, the count of its connections.
