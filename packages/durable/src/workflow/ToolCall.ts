import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

export const Result = Schema.Struct({
  status: Schema.Literals(['completed', 'failed', 'aborted']),
  entryId: Record.EntryId,
  control: Schema.optionalKey(Schema.JsonObject),
})

/** Native child workflow whose durable intent governs safe and unsafe tool recovery. */
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
