/**
 * Native tool-call Workflow declaration and result schema.
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
  status: Schema.Literals(['completed', 'failed', 'aborted']),
  entryId: Record.EntryId,
  control: Schema.optionalKey(Schema.JsonObject),
})
/**
 * Native workflow result schema.
 *
 * @category models
 * @since 0.0.0
 */
export type Result = typeof Result.Type

/**
 * Native child workflow whose durable intent governs safe and unsafe tool recovery.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolCall = Workflow.make('@effect-harness/durable/ToolCall/v1', {
  payload: {
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
    generationTaskId: Record.TaskId,
    assistantId: Record.EntryId,
    callId: Schema.String,
    name: Schema.String,
    arguments: Schema.Json,
  },
  success: Result,
  error: ExecutionErrorCodec,
  idempotencyKey: ({ sessionId, taskId }) => JSON.stringify([sessionId, taskId]),
})
