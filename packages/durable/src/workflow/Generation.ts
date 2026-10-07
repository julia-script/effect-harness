/**
 * Native generation Workflow declaration and result schema.
 *
 * @since 0.0.0
 */
import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

/**
 * Native workflow result schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Result = Schema.Struct({
  status: Schema.Literals(['answered', 'tools', 'reset', 'failed', 'aborted']),
  answer: Schema.optionalKey(Record.EntryId),
  detail: Schema.optionalKey(Schema.String),
})
/**
 * Native workflow result schema.
 *
 * @category models
 * @since 0.0.0
 */
export type Result = typeof Result.Type

/**
 * Native workflow for a conversation generation, including retries and tool rounds.
 *
 * @category schemas
 * @since 0.0.0
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
  error: ExecutionErrorCodec,
  idempotencyKey: ({ sessionId, taskId }) => JSON.stringify([sessionId, taskId]),
})
