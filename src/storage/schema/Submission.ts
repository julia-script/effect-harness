import { Schema } from 'effect'
import { ConversationId, EntryId, SubmissionId } from './Ids.ts'

const SubmissionCreateFields = {
  conversationId: ConversationId,
  /** Host-provided deduplication key, scoped to the conversation. */
  requestId: Schema.optionalKey(Schema.String),
}

const SubmissionRecordFields = { id: SubmissionId, ...SubmissionCreateFields }

const Unanswered = {
  status: Schema.Literal('unanswered'),
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
}

/** Every (type, status) pair a submission can be in, without identity fields. */
const SubmissionStates = {
  /** Input admitted but not yet represented in the transcript. */
  inputQueued: { type: Schema.Literal('input'), status: Schema.Literal('queued') },
  /** Input added to the transcript and owned by an active run. */
  inputPlaced: { type: Schema.Literal('input'), status: Schema.Literal('placed'), entry: EntryId },
  /** Successfully answered user input. */
  inputDone: {
    type: Schema.Literal('input'),
    status: Schema.Literal('done'),
    entry: EntryId,
    answer: EntryId,
  },
  /** Terminal input that can no longer receive an answer. */
  inputUnanswered: {
    type: Schema.Literal('input'),
    entry: Schema.optionalKey(EntryId),
    ...Unanswered,
  },
  /** Passive write admitted but not yet appended to the transcript. */
  writeQueued: { type: Schema.Literal('write'), status: Schema.Literal('queued') },
  /** Successfully appended passive entry. */
  writeDone: { type: Schema.Literal('write'), status: Schema.Literal('done'), entry: EntryId },
  /** Terminal passive write that could not be placed. */
  writeUnanswered: { type: Schema.Literal('write'), ...Unanswered },
}

/** Durable lifecycle of one admitted user input or passive entry write. */
export const SubmissionRecord = Schema.Union([
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.inputQueued }),
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.inputPlaced }),
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.inputDone }),
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.inputUnanswered }),
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.writeQueued }),
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.writeDone }),
  Schema.Struct({ ...SubmissionRecordFields, ...SubmissionStates.writeUnanswered }),
])
export type SubmissionRecord = typeof SubmissionRecord.Type

/** Submission fields supplied before an ID is assigned. */
export const SubmissionCreate = Schema.Union([
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.inputQueued }),
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.inputPlaced }),
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.inputDone }),
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.inputUnanswered }),
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.writeQueued }),
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.writeDone }),
  Schema.Struct({ ...SubmissionCreateFields, ...SubmissionStates.writeUnanswered }),
])
export type SubmissionCreate = typeof SubmissionCreate.Type

export const SubmissionStatus = Schema.Literals(['queued', 'placed', 'done', 'unanswered'])
export type SubmissionStatus = typeof SubmissionStatus.Type
