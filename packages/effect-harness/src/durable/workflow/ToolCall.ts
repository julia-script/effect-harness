/**
 * Native tool-call Workflow declaration and result schema.
 */
import * as Identity from '../Identity.ts'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionError } from './ExecutionError.ts'

/**
 * Schema for terminal tool settlement, committed result entry and optional controls.
 *
 * @category schemas
 */
export const Result = Schema.Struct({
  status: Schema.Literals(['completed', 'failed', 'aborted']),
  entryId: Record.EntryId,
  control: Schema.optionalKey(Schema.JsonObject),
})
/**
 * Decoded value validated by the `Result` schema.
 *
 * @category models
 */
export type Result = typeof Result.Type

/**
 * Native child Workflow settling one tool intent under its replay policy.
 *
 * **Details**
 *
 * Register ToolExecutor before executing. The session/task pair determines replay identity;
 * arguments and call identity are pinned in the durable intent.
 *
 * **Gotchas**
 *
 * Unsafe intents without a saved receipt are interrupted rather than blindly repeated. Safe
 * replay permits repetition and must match the tool’s actual external effects.
 *
 * @category schemas
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
  error: ExecutionError,
  idempotencyKey: ({ sessionId, taskId }) => JSON.stringify([sessionId, taskId]),
})
