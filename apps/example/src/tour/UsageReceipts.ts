import { NodeServices } from '@effect/platform-node'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Identity from 'effect-harness/Identity'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

// After integration build: node apps/example/dist/tour/UsageReceipts.js
// Real request/nested/compaction producers must still activate receipt recording at their settlement boundary.
NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'usage-receipts-example-' })
      const storage = () =>
        Layer.fresh(Storage.layerJsonl({ filePath: `${directory}/state.jsonl` }))
      const root = Record.ROOT_CONVERSATION_ID
      const fact: Usage.Receipt = {
        conversationId: root,
        identity: {
          _tag: 'requestAttempt',
          requestId: yield* Schema.decodeEffect(Identity.RequestId)('task-1/summary-1/config-1'),
          attempt: yield* Schema.decodeEffect(Usage.AttemptId)(0),
          purpose: 'compaction',
        },
        outcome: {
          _tag: 'failed',
          error: { message: 'Summary failed after known usage was reported' },
        },
        usage: yield* Usage.modelTiered(
          {
            inputUncached: 10,
            cacheRead: 20,
            cacheWrite: 6,
            cacheWrite1h: 2,
            output: 9,
            reasoning: 3,
          },
          { provider: 'fixture', modelId: 'summary' },
          {
            inputUncachedToken: 0.001,
            cacheReadToken: 0.0001,
            cacheWriteToken: 0.002,
            cacheWrite1hToken: 0.004,
            outputToken: 0.003,
            reasoningToken: 0.005,
            currency: 'USD',
            source: 'example-v1',
          },
        ),
      }
      let settlements = 0
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          yield* Session.commit(session, (tx) =>
            Usage.settle(tx, fact, () =>
              Effect.sync(() => {
                settlements++
              }),
            ),
          )
          yield* Effect.log({ first: yield* Session.usage(session), settlements })
        }),
      ).pipe(Effect.provide(storage()))
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const replay = yield* Session.commit(session, (tx) =>
            Usage.settle(tx, fact, () =>
              Effect.sync(() => {
                settlements++
              }),
            ),
          )
          if (replay.inserted || settlements !== 1)
            return yield* Effect.die('Receipt replay repeated settlement')
          const total = yield* Session.usage(session)
          if (
            total.models[0]?.responses !== 1 ||
            Math.abs((total.costs[0]?.model ?? 0) - 0.061) > 1e-12
          )
            return yield* Effect.die('Receipt accounting changed on reopen')
          yield* Effect.log({ reopened: total, replay: replay.receipt.outcome, settlements })
        }),
      ).pipe(Effect.provide(storage()))
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
