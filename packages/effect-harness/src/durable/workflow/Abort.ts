import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionError } from './ExecutionError.ts'

/**
 * Native Workflow persisting cancellation intent and reconciling its owned task tree.
 *
 * **Details**
 *
 * Register AbortExecutor before executing. sessionId/requestId determine replay identity;
 * target selects a conversation or task and background controls traversal.
 *
 * **Gotchas**
 *
 * Native interruption alone is not a durable abort receipt. Settlement accounts for owned
 * work and joined cleanup.
 *
 * @category schemas
 */
export const Abort = Workflow.make('@effect-harness/durable/Abort/v1', {
  payload: {
    sessionId: Identity.SessionId,
    requestId: Identity.RequestId,
    target: Schema.Union([
      Schema.TaggedStruct('conversation', { id: Record.ConversationId }),
      Schema.TaggedStruct('task', { id: Record.TaskId }),
    ]),
    background: Schema.Boolean,
    reason: Schema.optionalKey(Schema.String),
  },
  success: Schema.Struct({ reached: Schema.Array(Record.TaskId) }),
  error: ExecutionError,
  idempotencyKey: ({ sessionId, requestId }) => JSON.stringify([sessionId, requestId]),
})
