/**
 * Native generation Workflow declaration and result schema.
 */
import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionError } from './ExecutionError.ts'

/**
 * Schema for answered, tool-round, reset, failed or aborted generation settlement.
 *
 * @category schemas
 */
export const Result = Schema.Struct({
  status: Schema.Literals(['answered', 'tools', 'reset', 'failed', 'aborted']),
  answer: Schema.optionalKey(Record.EntryId),
  detail: Schema.optionalKey(Schema.String),
})
/**
 * Decoded value validated by the `Result` schema.
 *
 * @category models
 */
export type Result = typeof Result.Type

/**
 * Native Workflow running one conversation generation with retries and tool rounds.
 *
 * **Details**
 *
 * Register GenerationExecutor before calling the declaration. The session/task pair
 * determines execution identity; domain receipts preserve committed preparation and terminal
 * settlement across native Activity reply gaps.
 *
 * **Gotchas**
 *
 * A persistent domain Store and persistent WorkflowEngine are both needed for restart
 * recovery.
 *
 * @category schemas
 */
export const Generation = Workflow.make('@effect-harness/durable/Generation/v1', {
  payload: {
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
    runId: Identity.RunId,
    inputs: Schema.Array(Record.SubmissionId),
  },
  success: Result,
  error: ExecutionError,
  idempotencyKey: ({ sessionId, taskId }) => JSON.stringify([sessionId, taskId]),
})

/** Checks the decoded Result contract without decoding or coercing input.
 * @category guards
 */
export const isResult: (u: unknown) => u is Result = Schema.is(Schema.toType(Result))
