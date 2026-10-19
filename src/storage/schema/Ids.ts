import { Schema } from 'effect'

export const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
export const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

/** Every record kind shares one numeric ID namespace; scans order by ID as creation order. */
export const ConversationId = PositiveInt.pipe(Schema.brand('ConversationId'))
export type ConversationId = typeof ConversationId.Type

export const EntryId = PositiveInt.pipe(Schema.brand('EntryId'))
export type EntryId = typeof EntryId.Type

export const TaskId = PositiveInt.pipe(Schema.brand('TaskId'))
export type TaskId = typeof TaskId.Type

export const SubmissionId = PositiveInt.pipe(Schema.brand('SubmissionId'))
export type SubmissionId = typeof SubmissionId.Type

export const DocumentId = PositiveInt.pipe(Schema.brand('DocumentId'))
export type DocumentId = typeof DocumentId.Type

/** Strictly increasing sequence assigned to one atomic storage commit; gaps are permitted. */
export const Seq = PositiveInt.pipe(Schema.brand('Seq'))
export type Seq = typeof Seq.Type

/** The root conversation always uses this reserved ID. */
export const ROOT_CONVERSATION_ID = ConversationId.make(1)

/** Positive integer version of a stored task or document shape. */
export const Version = PositiveInt
export type Version = typeof Version.Type
