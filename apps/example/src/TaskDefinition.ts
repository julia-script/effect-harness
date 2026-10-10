/** Run after integration build: bun apps/example/src/TaskDefinition.ts. Prepares a definition, not a scheduler. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Transaction from 'effect-harness/Transaction'

const contract = {
  kind: 'example.counter',
  version: 1,
  input: Schema.Struct({ count: Schema.Natural }),
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('first'), count: Schema.Natural }),
    Schema.Struct({ phase: Schema.Literal('second'), total: Schema.Natural }),
  ]),
  success: Schema.Natural,
  failure: Schema.Struct({ reason: Schema.String }),
}

/** Register this definition once the scheduler integration is available. */
export const Counter = Task.make({
  ...contract,
  initial: ({ count }) => ({ phase: 'first' as const, count }),
  phases: {
    first: ({ checkpoint }) =>
      Effect.gen(function* () {
        const execution = yield* TaskExecution
        yield* execution.commit((tx) =>
          Effect.gen(function* () {
            yield* Transaction.appendEntry(tx, execution.conversationId, {
              kind: 'example.counter.increment',
              data: { count: checkpoint.count + 1 },
            })
            yield* execution.progress(tx, contract, {
              status: 'pending',
              checkpoint: { phase: 'second', total: checkpoint.count + 1 },
            })
          }),
        )
      }),
    second: ({ checkpoint }) =>
      Effect.gen(function* () {
        const execution = yield* TaskExecution
        yield* execution.commit((tx) =>
          execution.progress(tx, contract, {
            status: 'completing',
            outcome: { status: 'completed', result: checkpoint.total },
          }),
        )
      }),
  },
})

if (import.meta.main) {
  await Effect.runPromise(
    Effect.gen(function* () {
      const prepared = yield* Task.initialize(Counter, { count: 3 })
      yield* Effect.log(
        'Prepared counter wire values; phase dispatch requires scheduler integration.',
        prepared,
      )
    }),
  )
}
