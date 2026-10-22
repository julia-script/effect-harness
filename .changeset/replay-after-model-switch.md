---
'effect-harness': patch
---

Replay the transcript for the model a request goes to, so a conversation can switch models. Other models' empty text blocks, provider item IDs, thought signatures and reasoning are left out (Claude thinking still goes to Claude on the same account), tool call IDs another provider refuses are rewritten with their results, and images become a note for text-only models. Adds `Transcript.replay`; stored messages are unchanged.
