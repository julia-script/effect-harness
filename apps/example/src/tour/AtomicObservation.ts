import { NodeServices } from '@effect/platform-node'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as View from 'effect-harness/ConversationView'
import * as Graph from 'effect-harness/TaskGraph'
import * as Storage from 'effect-harness/Storage'
import * as Session from 'effect-harness/Session'
import * as Transaction from 'effect-harness/Transaction'

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* LanguageModel.make({
        generateText: () => Effect.die('This example only observes'),
        streamText: () => Stream.die('This example only observes'),
      })
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      const harness = yield* Harness.make.pipe(
        Effect.provideService(HarnessBackend, runtime.backend),
      )
      const conversation = yield* harness.root
      const graph = yield* Graph.snapshot(harness)
      const pull = yield* Stream.toPull(View.watch(conversation))
      const initial = (yield* pull)[0]
      if (initial._tag !== 'Snapshot') return yield* Effect.die('Expected an initial snapshot')
      yield* Session.commit(runtime.session, (tx) =>
        Transaction.appendEntry(tx, conversation.id, {
          kind: 'note',
          data: { text: 'Committed once, observed coherently' },
        }),
      )
      let replica = initial
      for (const frame of yield* pull) replica = yield* View.reduce(replica, frame)
      yield* Effect.log({
        conversations: graph.value.conversations.length,
        epoch: replica.revision.epoch,
        cursor: replica.revision.cursor,
        entries: replica.value.entries.length,
      })
    }),
  ).pipe(Effect.provide(Layer.mergeAll(Storage.layerMemory, NodeServices.layer))),
)
