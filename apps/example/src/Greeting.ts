import { Effect, type Layer, Schema } from 'effect'
import { Activity, Workflow, type WorkflowEngine } from 'effect/workflow'

// User-authored workflows use Effect's ordinary declaration and handler APIs.
export const Greeting = Workflow.make('example/greeting/v1', {
  payload: { name: Schema.String },
  success: Schema.String,
  error: Schema.Never,
  idempotencyKey: ({ name }) => name,
})

export const layer: Layer.Layer<never, never, WorkflowEngine.WorkflowEngine> = Greeting.toLayer(
  ({ name }) =>
    Activity.make({
      name: 'greet',
      success: Schema.String,
      execute: Effect.succeed(`Hello, ${name}`),
    }),
)
