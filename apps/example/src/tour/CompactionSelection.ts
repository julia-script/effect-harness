import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as Response from 'effect/ai/Response'
import * as Compaction from 'effect-harness/Compaction'
import * as Projection from 'effect-harness/ContextProjection'
import * as Identity from 'effect-harness/Identity'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

// After integration build: node apps/example/dist/tour/CompactionSelection.js
// This foundation selects and pins work. Durable execution and summary placement follow later.
NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const session = yield* Session.make()
      const root = yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
      for (const content of [
        'We are implementing safe compaction.',
        'Keep tool groups together.',
        'The next step is durable execution.',
      ])
        yield* Session.commit(session, (tx) =>
          Transaction.appendEntry(tx, root.id, {
            kind: 'input',
            model: [{ role: 'user', content }],
          }),
        )
      const view = yield* Projection.read(session, root.id)
      const result = yield* Compaction.select(
        view,
        { ...Settings.defaults.compaction, enabled: false, keepRecentTokens: 8 },
        { reason: 'manual', instructions: 'Preserve the next step.' },
      )
      if (result._tag === 'Noop') {
        yield* Effect.log({ result })
        return
      }
      const request = yield* Compaction.pinRequest(result, {
        requestId: Identity.RequestId.make('example/compaction:1/config:1'),
        attempt: Usage.AttemptId.make(0),
        model: { ref: { provider: 'example', modelId: 'summary' }, options: {} },
        stream: {},
        maxOutputTokens: 128,
      })
      const summary = yield* Compaction.validateSummary({
        finishReason: 'stop',
        content: [
          Response.makePart('text', {
            text: 'Implementing safe compaction; preserve tool groups. Next: durable execution.',
          }),
        ],
      })
      yield* Effect.log({
        measurement: result.measurement,
        firstKept: request.selection.firstKept,
        tail: request.selection.tail,
        requestId: request.config.requestId,
        attempt: request.config.attempt,
        summary,
      })
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
