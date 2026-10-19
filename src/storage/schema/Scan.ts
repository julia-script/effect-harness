import { Schema } from 'effect'
import { DocumentPoint, DocumentScope } from './Document.ts'
import { ConversationId, EntryId, TaskId } from './Ids.ts'
import { JsonObject } from './Json.ts'
import { SubmissionStatus } from './Submission.ts'
import { TaskStatus } from './Task.ts'

/**
 * Backend-owned continuation state that callers only round-trip to the same scan. It carries the
 * scan's order: a scan given a cursor continues in that order.
 */
export const Cursor = JsonObject.annotate({ identifier: 'Cursor' })
export type Cursor = typeof Cursor.Type

/** ID order of a scan: `ascending` is oldest first, `descending` newest first. */
export const ScanOrder = Schema.Literals(['ascending', 'descending'])
export type ScanOrder = typeof ScanOrder.Type

/** One ordered scan result and its optional continuation. */
export const Page = <S extends Schema.Top>(item: S) =>
  Schema.Struct({ items: Schema.Array(item), next: Schema.optionalKey(Cursor) })
export interface Page<A> {
  readonly items: ReadonlyArray<A>
  readonly next?: Cursor
}

/** Optional filters for an ordered conversation scan. Default order: ascending. */
export const ConversationQuery = Schema.Struct({
  ownerConversationId: Schema.optionalKey(ConversationId),
  ownerTaskId: Schema.optionalKey(TaskId),
  order: Schema.optionalKey(ScanOrder),
})
export type ConversationQuery = typeof ConversationQuery.Type

/** Inclusive ID bounds for a scan of one conversation's fork-aware history. Default order: descending. */
export const EntryQuery = Schema.Struct({
  conversationId: ConversationId,
  /** Oldest entry ID that may be returned. */
  minEntryId: Schema.optionalKey(EntryId),
  /** Newest entry ID that may be returned. */
  maxEntryId: Schema.optionalKey(EntryId),
  order: Schema.optionalKey(ScanOrder),
})
export type EntryQuery = typeof EntryQuery.Type

/** Optional filters for an ordered scan of task records. Default order: ascending. */
export const TaskQuery = Schema.Struct({
  conversationId: Schema.optionalKey(ConversationId),
  kind: Schema.optionalKey(Schema.String),
  status: Schema.optionalKey(TaskStatus),
  abortRequested: Schema.optionalKey(Schema.Boolean),
  background: Schema.optionalKey(Schema.Boolean),
  order: Schema.optionalKey(ScanOrder),
})
export type TaskQuery = typeof TaskQuery.Type

/** Optional filters for an ordered scan of submission records. Default order: ascending. */
export const SubmissionQuery = Schema.Struct({
  conversationId: Schema.optionalKey(ConversationId),
  status: Schema.optionalKey(SubmissionStatus),
  order: Schema.optionalKey(ScanOrder),
})
export type SubmissionQuery = typeof SubmissionQuery.Type

/** Ordered scan of document incarnations alive in one exact scope at one point. */
export const DocumentQuery = Schema.Struct({
  scope: DocumentScope,
  at: DocumentPoint,
  kind: Schema.optionalKey(Schema.String),
})
export type DocumentQuery = typeof DocumentQuery.Type
