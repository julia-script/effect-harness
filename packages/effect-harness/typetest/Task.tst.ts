import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Task from 'effect-harness/Task'
import { TaskRuntime } from 'effect-harness/TaskRuntime'

class Counter extends Context.Service<Counter, { readonly value: number }>()('test/Counter') {}

test('binding captures host dependencies while preserving invocation-local task services', () => {
  const definition = Task.define({
    name: 'counter',
    version: 1,
    input: Schema.String,
    checkpoint: Schema.toCodecJson(Schema.Struct({ phase: Schema.Literal('read') })),
    result: Schema.Int,
    initial: () => ({ phase: 'read' as const }),
    run: Effect.fnUntraced(function* () {
      const counter = yield* Counter
      yield* TaskRuntime
      return Task.complete(counter.value)
    }),
  })
  expect(Task.bind(definition)).type.toBe<Effect.Effect<Task.BoundDefinition, never, Counter>>()
  expect(definition.initial('input')).type.toBe<{ readonly phase: 'read' }>()
})
