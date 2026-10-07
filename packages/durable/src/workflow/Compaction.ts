/**
 * Native compaction Workflow declaration and result schemas.
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
  entryId: Schema.optionalKey(Record.EntryId),
  submissionId: Schema.optionalKey(Record.SubmissionId),
})
/**
 * Native workflow result schema.
 *
 * @category models
 * @since 0.0.0
 */
export type Result = typeof Result.Type

/**
 * Native workflow for an immutable compaction request and its eventual summary placement.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Compaction = Workflow.make('@effect-harness/durable/Compaction/v1', {
  payload: {
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
    reason: Schema.Literals(['manual', 'threshold', 'overflow', 'background']),
    instructions: Schema.optionalKey(Schema.String),
    blocking: Schema.Boolean,
  },
  success: Result,
  error: ExecutionErrorCodec,
  idempotencyKey: ({ sessionId, taskId }) => JSON.stringify([sessionId, taskId]),
})
