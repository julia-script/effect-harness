/**
 * Native compaction Workflow declaration and result schemas.
 */
import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

/**
 * Schema for optional summary-entry and submission identities from compaction settlement.
 *
 * @category schemas
 */
export const Result = Schema.Struct({
  entryId: Schema.optionalKey(Record.EntryId),
  submissionId: Schema.optionalKey(Record.SubmissionId),
})
/**
 * Decoded value validated by the `Result` schema.
 *
 * @category models
 */
export type Result = typeof Result.Type

/**
 * Native Workflow preparing and placing an immutable conversation summary.
 *
 * **Details**
 *
 * Register CompactionExecutor before executing. Payload identifies the session,
 * conversation, task, selection reason and blocking policy. The session/task pair determines
 * replay identity.
 *
 * @category schemas
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
