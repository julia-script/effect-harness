import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Activity from 'effect/workflow/Activity'
import * as Workflow from 'effect/workflow/Workflow'

// User-authored workflows use Effect's ordinary declaration and handler APIs.
export const Greeting = Workflow.make('example/greeting/v1', {
  payload: { name: Schema.String },
  success: Schema.String,
  error: Schema.Never,
  idempotencyKey: ({ name }) => name,
})

export const layer = Greeting.toLayer(({ name }) =>
  Activity.make({
    name: 'greet',
    success: Schema.String,
    execute: Effect.succeed(`Hello, ${name}`),
  }),
)
