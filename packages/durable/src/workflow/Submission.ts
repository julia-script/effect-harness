import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionError } from './ExecutionError.ts'

export const EntryDraft = Schema.Struct({
  kind: Schema.String,
  model: Schema.optionalKey(Schema.Array(Schema.toEncoded(Schema.toCodecJson(Prompt.Message)))),
  data: Schema.optionalKey(Schema.Json),
  head: Schema.optionalKey(Schema.Union([Record.EntryId, Schema.Literal('self')])),
  edits: Schema.optionalKey(Schema.Array(Record.ContextEdit)),
  byTaskId: Schema.optionalKey(Record.TaskId),
})

export const Input = Schema.Union([
  Schema.Struct({
    type: Schema.Literal('input'),
    message: Prompt.UserMessage,
    whenBusy: Schema.optionalKey(Schema.Literals(['steer', 'followUp', 'reject'])),
  }),
  Schema.Struct({ type: Schema.Literal('write'), entry: EntryDraft }),
])

export const Result = Record.SettledSubmission

/**
 * Admits a durable input or passive write and waits for its settled receipt.
 * Use ordinary execute, poll and interrupt methods supplied by Effect Workflow.
 * requestId is also the conversation's persistent admission identity.
 */
export const Submission = Workflow.make('@effect-harness/durable/Submission/v1', {
  payload: {
    sessionId: Schema.String,
    conversationId: Record.ConversationId,
    requestId: Schema.String,
    submission: Input,
  },
  success: Result,
  error: ExecutionError,
  idempotencyKey: ({ sessionId, conversationId, requestId, submission }) =>
    JSON.stringify([sessionId, conversationId, requestId, submission.type]),
})
