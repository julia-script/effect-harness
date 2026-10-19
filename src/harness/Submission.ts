/**
 * Submissions, ported from pi's `harness/submissions.ts`: user input that may start a run, or a
 * passive entry write, durably admitted to one conversation.
 */
import { Schema } from 'effect'
import type { Effect } from 'effect'
import { EntryDraft } from '../storage/schema/Entry.ts'
import type { CommitError } from '../session/SessionError.ts'
import type { SubmissionId } from '../storage/schema/Ids.ts'
import { JsonObject } from '../storage/schema/Json.ts'
import { SubmissionRecord } from '../storage/schema/Submission.ts'
import type { SubmissionNotFound } from './HarnessError.ts'

/**
 * The content of a user message: text, or a list of content parts. Parts stay opaque JSON
 * objects here; the model layer owns their schema.
 */
export const UserInput = Schema.Union([Schema.String, Schema.Array(JsonObject)])
export type UserInput = typeof UserInput.Type

/**
 * The entry an input is placed as, instead of `$user`: an application kind, and data the
 * transcript shows, such as its author. Its model message is still the input's user message.
 */
export const InputEntry = Schema.Struct({
  /** Must be non-empty and must not start with `$`. */
  kind: Schema.String,
  data: Schema.optionalKey(Schema.Json),
})
export type InputEntry = typeof InputEntry.Type

/** What a busy conversation does with input: queue it as a steer or a follow-up, or reject it. */
export const WhenBusy = Schema.Literals(['steer', 'followUp', 'reject'])
export type WhenBusy = typeof WhenBusy.Type

/** User input that may start a run. */
export const InputDraft = Schema.Struct({
  type: Schema.Literal('input'),
  /** Deduplication key, scoped to the conversation. */
  requestId: Schema.optionalKey(Schema.String),
  content: UserInput,
  /** Default: `followUp`. */
  whenBusy: Schema.optionalKey(WhenBusy),
  /** Default: a `$user` entry without data. */
  entry: Schema.optionalKey(InputEntry),
})
export type InputDraft = typeof InputDraft.Type

/** A passive entry write. */
export const WriteDraft = Schema.Struct({
  type: Schema.Literal('write'),
  requestId: Schema.optionalKey(Schema.String),
  entry: EntryDraft,
})
export type WriteDraft = typeof WriteDraft.Type

/** What a host submits to a conversation. */
export const Draft = Schema.Union([InputDraft, WriteDraft])
export type Draft = typeof Draft.Type

/** A submission record that can change no more. */
export const Settled = SubmissionRecord.pipe(
  Schema.refine(
    (record): record is Extract<SubmissionRecord, { status: 'done' | 'unanswered' }> =>
      record.status === 'done' || record.status === 'unanswered',
  ),
)
export type Settled = typeof Settled.Type

/** What aborting a submission found: it was queued, already placed, or already settled. */
export const AbortResult = Schema.Literals(['aborted', 'already_placed', 'settled'])
export type AbortResult = typeof AbortResult.Type

/** Handle of one durably admitted submission. */
export interface Submission {
  readonly id: SubmissionId
  /** The committed record. */
  readonly status: Effect.Effect<SubmissionRecord, SubmissionNotFound | CommitError>
  /**
   * Wait until the submission is settled. Interrupting the wait leaves the submission as it is;
   * closing the Harness fails it with `SessionClosed`.
   */
  readonly wait: Effect.Effect<Settled, SubmissionNotFound | CommitError>
  /** Withdraw a queued submission; placed and settled ones stay as they are. */
  readonly abort: Effect.Effect<AbortResult, SubmissionNotFound | CommitError>
}
