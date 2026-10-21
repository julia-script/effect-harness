---
'effect-harness': minor
---

Breaking: the contract between a Harness and its clients moves to a new `effect-harness/protocol` subpath (and `Protocol` on the root import). Import `HarnessRpcs`, `Init`, `Submission`, `HarnessError`, `AgentState`, `ViewState` and `AgentEvent` from there. `effect-harness/client` now exports only `HarnessClient` and `HarnessTransport`, and `effect-harness/harness` no longer exports the five protocol modules it shared with the client. The `HarnessError` tags move from `effect-harness/harness/HarnessError/*` to `effect-harness/protocol/HarnessError/*`.
