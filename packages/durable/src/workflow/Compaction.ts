import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

export const Result = Schema.Struct({
  entryId: Schema.optionalKey(Record.EntryId),
  submissionId: Schema.optionalKey(Record.SubmissionId),
})

/** Native workflow for an immutable compaction request and its eventual summary placement. */
export const Compaction = Workflow.make('@effect-harness/durable/Compaction/v1', {
  payload: {
    sessionId: Schema.String,
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
