import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionError } from './ExecutionError.ts'

export const Result = Schema.Struct({
  status: Schema.Literals(['completed', 'failed', 'aborted']),
  entryId: Record.EntryId,
  control: Schema.optionalKey(Schema.JsonObject),
})

/** Native child workflow whose durable intent governs safe and unsafe tool recovery. */
export const ToolCall = Workflow.make('@effect-harness/durable/ToolCall/v1', {
  payload: {
    sessionId: Schema.String,
    conversationId: Record.ConversationId,
    taskId: Record.TaskId,
    generationTaskId: Record.TaskId,
    assistantId: Record.EntryId,
    callId: Schema.String,
    name: Schema.String,
    arguments: Schema.Json,
  },
  success: Result,
  error: ExecutionError,
  idempotencyKey: ({ sessionId, taskId }) => JSON.stringify([sessionId, taskId]),
})
