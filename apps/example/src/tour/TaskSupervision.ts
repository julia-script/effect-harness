import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as TaskSupervision from 'effect-harness/TaskSupervision'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

// After integration build: node apps/example/dist/tour/TaskSupervision.js
const childContract = {
  kind: 'example.supervisedChild',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: Schema.Natural,
  failure: Schema.String,
}
const child = Task.make({
  ...childContract,
  initial: () => ({ phase: 'run' as const }),
  phases: {
    run: ({ input }) =>
      Effect.gen(function* () {
        const access = yield* TaskExecution
        yield* access.commit((tx) =>
          Effect.gen(function* () {
            yield* Transaction.appendEntry(tx, access.conversationId, {
              kind: 'child.result',
              data: input,
            })
            yield* access.progress(tx, childContract, {
              status: 'completing',
              outcome: { status: 'completed', result: input * 2 },
            })
          }),
        )
      }),
  },
})
const parentContract = {
  kind: 'example.supervisedParent',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('spawn') }),
    Schema.Struct({ phase: Schema.Literal('join'), ids: Schema.Array(Record.TaskId) }),
  ]),
  success: Schema.Natural,
  failure: Schema.String,
}
const parent = Task.make({
  ...parentContract,
  initial: () => ({ phase: 'spawn' as const }),
  phases: {
    spawn: ({ input }) =>
      Effect.gen(function* () {
        const access = yield* TaskExecution
        // Children and the durable join checkpoint enter storage in one commit.
        yield* access.commit((tx) =>
          Effect.gen(function* () {
            const first = yield* access.createChild(tx, child, input)
            const second = yield* access.createChild(tx, child, input + 1)
            yield* access.progress(tx, parentContract, {
              status: 'waiting',
              on: [first.id, second.id],
              policy: 'allSettled',
              checkpoint: { phase: 'join', ids: [first.id, second.id] },
            })
          }),
        )
      }),
    join: ({ checkpoint }) =>
      Effect.gen(function* () {
        const access = yield* TaskExecution
        const outcomes = yield* access.outcomes(checkpoint.ids)
        let total = 0
        for (const outcome of outcomes) {
          if (outcome.status !== 'completed') return yield* Effect.fail('Child did not complete')
          total += yield* Schema.decodeUnknownEffect(Schema.Natural)(outcome.result)
        }
        yield* access.commit((tx) =>
          access.progress(tx, parentContract, {
            status: 'completing',
            outcome: { status: 'completed', result: total },
          }),
        )
      }),
  },
})

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const session = yield* Session.make()
      yield* Session.commit(session, Transaction.ensureRoot)
      const registry = yield* Registry.make
      yield* registry.install(
        Registry.withTask(
          Registry.withTask(
            Registry.toolkit({ id: 'supervised-example', version: 1 }, Toolkit.make()),
            child,
          ),
          parent,
        ),
      )
      const supervisor = yield* TaskSupervision.make(session).pipe(
        Effect.provideService(Registry.Registry, registry),
      )
      const ref = yield* supervisor.start(parent, 10, {
        conversationId: Record.ROOT_CONVERSATION_ID,
      })
      const outcome = yield* supervisor.wait(ref)
      if (outcome.status !== 'completed' || outcome.result !== 42)
        return yield* Effect.die('Expected supervised result 42')
      yield* Effect.log('Native parent and two children completed', outcome)
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
