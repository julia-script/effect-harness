/**
 * Native submission Workflow declaration and legacy-compatible payloads.
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
 * Schema for passive-write entry content before durable identity allocation.
 *
 * **Details**
 *
 * Native model-message content is encoded for persistence. A self head can mark the new
 * entry as a context boundary.
 *
 * @category schemas
 */
export const EntryDraft = Record.Entry.mapFields((fields) => ({
  ...Struct.omit(fields, ['id', 'conversationId', 'head', 'model']),
  model: Schema.optionalKey(Schema.Array(Schema.toEncoded(Schema.toCodecJson(Prompt.Message)))),
  head: Schema.optionalKey(Schema.Union([Record.EntryId, Schema.Literal('self')])),
}))
/**
 * Decoded value validated by the `EntryDraft` schema.
 *
 * @category models
 */
export type EntryDraft = typeof EntryDraft.Type

/**
 * Schema for user input with busy policy, or a passive entry write.
 *
 * **Details**
 *
 * whenBusy selects steer, followUp or reject for input admission. Decoded variants use _tag
 * while encoded payloads retain type.
 *
 * @category schemas
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
 * Decoded value validated by the `Input` schema.
 *
 * @category models
 */
export type Input = typeof Input.Type

/**
 * Schema for a settled input or passive-write receipt.
 *
 * @category combinators
 */
export const Result = Record.SettledSubmission
/**
 * Decoded value validated by the `Result` schema.
 *
 * @category models
 */
export type Result = typeof Result.Type

/**
 * Native Workflow admitting an input or passive write and returning its settled receipt.
 *
 * **Details**
 *
 * Use the declaration’s execute, poll and resume methods with the registered
 * SubmissionExecutor. Native execution identity combines session, conversation, request ID
 * and submission kind.
 *
 * **Gotchas**
 *
 * Choose requestId before admission and reuse it on retry. Same-kind replay returns the
 * first receipt even if content changed; reusing the admission identity for another kind
 * fails. Use a new request ID for new content.
 *
 * @category combinators
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
