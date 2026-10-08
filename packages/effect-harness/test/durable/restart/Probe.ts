import * as Schema from 'effect/Schema'
import * as DurableDeferred from 'effect/workflow/DurableDeferred'
import * as Workflow from 'effect/workflow/Workflow'

export const Probe = Workflow.make('effect-harness/test/RestartProbe/v1', {
  payload: { key: Schema.String },
  success: Schema.Int,
  error: Schema.String,
  idempotencyKey: ({ key }) => key,
})
export const Resume = DurableDeferred.make('resume', { success: Schema.Void })
