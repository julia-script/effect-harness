import * as Identity from '../Identity.ts'
import * as Struct from 'effect/Struct'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

export const EntryDraft = Record.Entry.mapFields((fields) => ({
  ...Struct.omit(fields, ['id', 'conversationId', 'head', 'model']),
  model: Schema.optionalKey(Schema.Array(Schema.toEncoded(Schema.toCodecJson(Prompt.Message)))),
  head: Schema.optionalKey(Schema.Union([Record.EntryId, Schema.Literal('self')])),
}))

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
    sessionId: Identity.SessionId,
    conversationId: Record.ConversationId,
    requestId: Identity.RequestId,
    submission: Input,
  },
  success: Result,
  error: ExecutionErrorCodec,
  idempotencyKey: ({ sessionId, conversationId, requestId, submission }) =>
    JSON.stringify([sessionId, conversationId, requestId, submission.type]),
})
