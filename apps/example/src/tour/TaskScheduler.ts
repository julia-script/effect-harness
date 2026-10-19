import { NodeRuntime, NodeServices } from '@effect/platform-node'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

// After integration build: node apps/example/dist/tour/TaskScheduler.js
// This standalone scheduler executes registered custom tasks. Ownership supervision,
// child joins, cascading abort and legacy Harness.run integration need a supplied host.
const root = Record.ROOT_CONVERSATION_ID
const contract = {
  kind: 'example.durable-counter',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('increment'), count: Schema.Natural }),
    Schema.Struct({ phase: Schema.Literal('complete'), count: Schema.Natural }),
  ]),
  success: Schema.Natural,
  failure: Schema.String,
}
const counter = (beforeCompletion: Effect.Effect<void>) =>
  Task.make({
    ...contract,
    initial: (count: number) => ({ phase: 'increment' as const, count }),
    phases: {
      increment: ({ checkpoint }) =>
        Effect.gen(function* () {
          const execution = yield* TaskExecution
          yield* execution.commit((tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'counter.increment',
                data: checkpoint.count,
              })
              yield* execution.progress(tx, contract, {
                status: 'pending',
                checkpoint: { phase: 'complete', count: checkpoint.count + 1 },
              })
            }),
          )
        }),
      complete: ({ checkpoint }) =>
        Effect.gen(function* () {
          yield* beforeCompletion
          const execution = yield* TaskExecution
          yield* execution.commit((tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'counter.complete',
                data: checkpoint.count,
              })
              yield* execution.progress(tx, contract, {
                status: 'completing',
                outcome: { status: 'completed', result: checkpoint.count },
              })
            }),
          )
        }),
    },
  })
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskScheduler.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const storage = Storage.layerJsonl({ filePath: `${directory}/counter.jsonl` })
      const entered = yield* Deferred.make<void>()
      const stopped = counter(
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
      )
      // Stop in phase two, after phase one's entry and checkpoint committed together.
      const id = yield* Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'counter', version: 1 }, Toolkit.make()),
              stopped,
            ),
          )
          const ref = yield* scheduler.start(stopped, 20, { conversationId: root })
          yield* Deferred.await(entered)
          yield* Effect.log('Before shutdown', (yield* scheduler.inspect(ref.id)).record.state)
          return ref.id
        }),
      ).pipe(Effect.provide(Layer.fresh(storage)))

      // Open a new native JSONL writer and new Session/Registry/scheduler scopes.
      const resumed = counter(Effect.void)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'counter', version: 1 }, Toolkit.make()),
              resumed,
            ),
          )
          yield* Effect.log('After cold reopen', (yield* scheduler.inspect(id)).record.state)
          const result = yield* scheduler.wait({ id, definition: resumed })
          if (result.status !== 'completed' || result.result !== 21)
            return yield* Effect.die('Resumed task did not complete with result 21')
          yield* Effect.log('Result', result)
          const entries = yield* Session.scanEntries(session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          if (
            entries.filter((entry) => entry.kind === 'counter.increment').length !== 1 ||
            entries.filter((entry) => entry.kind === 'counter.complete').length !== 1
          )
            return yield* Effect.die('Cold reopen duplicated or lost a committed phase')
          // The durable increment appears once; only the second phase resumed.
          yield* Effect.log(
            'Committed entries',
            entries.map((entry) => ({ kind: entry.kind, data: entry.data })),
          )
        }),
      ).pipe(Effect.provide(Layer.fresh(storage)))
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
