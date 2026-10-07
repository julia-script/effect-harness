import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

/** Records cancellation intent and reconciles the owned tree through native interruption. */
export const Abort = Workflow.make('@effect-harness/durable/Abort/v1', {
  payload: {
    sessionId: Identity.SessionId,
    requestId: Identity.RequestId,
    target: Schema.Union([
      Schema.Struct({ type: Schema.Literal('conversation'), id: Record.ConversationId }),
      Schema.Struct({ type: Schema.Literal('task'), id: Record.TaskId }),
    ]),
    background: Schema.Boolean,
    reason: Schema.optionalKey(Schema.String),
  },
  success: Schema.Struct({ reached: Schema.Array(Record.TaskId) }),
  error: ExecutionErrorCodec,
  idempotencyKey: ({ sessionId, requestId }) => JSON.stringify([sessionId, requestId]),
})
