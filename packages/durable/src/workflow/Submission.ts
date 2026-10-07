/**
 * Native submission Workflow declaration and legacy-compatible payloads.
 *
 * @since 0.0.0
 */
import { tagged } from '../internal/legacyTag.ts'
import * as Identity from '../Identity.ts'
import * as Struct from 'effect/Struct'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Record from '../Record.ts'
import { ExecutionErrorCodec } from './ExecutionError.ts'

/**
 * EntryDraft schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const EntryDraft = Record.Entry.mapFields((fields) => ({
  ...Struct.omit(fields, ['id', 'conversationId', 'head', 'model']),
  model: Schema.optionalKey(Schema.Array(Schema.toEncoded(Schema.toCodecJson(Prompt.Message)))),
  head: Schema.optionalKey(Schema.Union([Record.EntryId, Schema.Literal('self')])),
}))
/**
 * Decoded EntryDraft values.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryDraft = typeof EntryDraft.Type

/**
 * Input schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Input = Schema.Union([
  tagged('input', {
    type: Schema.tag('input'),
    message: Prompt.UserMessage,
    whenBusy: Schema.optionalKey(Schema.Literals(['steer', 'followUp', 'reject'])),
  }),
  tagged('write', { type: Schema.tag('write'), entry: EntryDraft }),
])
/**
 * Decoded Input values.
 *
 * @category models
 * @since 0.0.0
 */
export type Input = typeof Input.Type

/**
 * Native workflow result schema.
 *
 * @category combinators
 * @since 0.0.0
 */
export const Result = Record.SettledSubmission
/**
 * Native workflow result schema.
 *
 * @category models
 * @since 0.0.0
 */
export type Result = typeof Result.Type

/**
 * Admits a durable input or passive write and waits for its settled receipt.
 *
 * **Details**
 *
 * Use ordinary execute, poll and interrupt methods supplied by Effect Workflow. requestId is also the conversation's persistent admission identity.
 *
 * @category combinators
 * @since 0.0.0
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
