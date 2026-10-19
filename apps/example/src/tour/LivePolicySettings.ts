import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Settings from 'effect-harness/Settings'

// After the integration build: node apps/example/dist/tour/LivePolicySettings.js
// A runtime consumer samples these immutable values at its documented boundary.
// Settings itself does not start model requests, tool rounds or durable retries.
export const program = Effect.gen(function* () {
  const before = yield* Settings.get
  const after = yield* Settings.update({
    retry: { maxRetries: 2, baseDelayMs: 500 },
    progress: { outputIntervalMs: 250 },
    steeringMode: 'all',
  })
  yield* Effect.log({
    preparedRevision: before.revision,
    nextDecisionRevision: after.revision,
    preparedRetryBudget: before.retry.maxRetries,
    nextRetryBudget: after.retry.maxRetries,
    toolOutputIntervalMs: after.progress.outputIntervalMs,
  })
}).pipe(Effect.provide(Settings.layerDefault))

NodeRuntime.runMain(program)
