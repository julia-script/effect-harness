import * as Schema from 'effect/Schema'
import * as Record from '../Record.ts'
import * as Sequence from './Sequence.js'

export const StorageIdSchema = Schema.Union([
  Record.ConversationId,
  Record.EntryId,
  Record.TaskId,
  Record.SubmissionId,
  Record.DocumentId,
])
export type StorageId = typeof StorageIdSchema.Type

export const DocumentPointSchema = Schema.Union([
  Sequence.SequenceSchema,
  Schema.Literal('current'),
])
export type DocumentPoint = typeof DocumentPointSchema.Type
export const DocumentAddressSchema = Schema.Struct({
  kind: Schema.String,
  scope: Record.Scope,
  key: Schema.optionalKey(Schema.String),
})
export type DocumentAddress = typeof DocumentAddressSchema.Type
export const DocumentRecordSchema = Record.Document.mapFields((fields) => ({
  ...fields,
  createdAt: Sequence.SequenceSchema,
  retiredAt: Schema.optionalKey(Sequence.SequenceSchema),
}))
export type DocumentRecord = typeof DocumentRecordSchema.Type
export const StoredDocumentSchema = Schema.Struct({
  record: DocumentRecordSchema,
  version: Schema.Int,
  value: Schema.JsonObject,
  deltasSinceBase: Schema.Natural,
})
export type StoredDocument = typeof StoredDocumentSchema.Type
export const StoredEntrySchema = Schema.Struct({
  entry: Record.Entry,
  commitSeq: Sequence.SequenceSchema,
})
export type StoredEntry = typeof StoredEntrySchema.Type
export const HeadMarkerSchema = Record.Entry.mapFields((fields) => ({
  ...fields,
  head: Record.EntryId,
}))
export type HeadMarker = typeof HeadMarkerSchema.Type

export const StorageWriteSchema = Schema.Union([
  Schema.TaggedStruct('conversation', { value: Record.Conversation }),
  Schema.TaggedStruct('entry', { value: Record.Entry }),
  Schema.TaggedStruct('task', { value: Record.Task }),
  Schema.TaggedStruct('submission', { value: Record.Submission }),
  Schema.TaggedStruct('document.create', {
    record: Record.DocumentCreate,
    content: Record.Content,
  }),
  Schema.TaggedStruct('document.copy', {
    record: Record.DocumentCreate,
    source: Schema.Struct({ id: Record.DocumentId, at: DocumentPointSchema }),
  }),
  Schema.TaggedStruct('document.change', {
    id: Record.DocumentId,
    content: Record.Content,
    publicationOps: Schema.optionalKey(Schema.Array(Record.Op)),
  }),
  Schema.TaggedStruct('document.retire', { id: Record.DocumentId }),
])
export type StorageWrite = typeof StorageWriteSchema.Type

export const ScanOrderSchema = Schema.Literals(['ascending', 'descending'])
export type ScanOrder = typeof ScanOrderSchema.Type
export const ConversationQuerySchema = Schema.Struct({
  ownerConversationId: Schema.optionalKey(Record.ConversationId),
  ownerTaskId: Schema.optionalKey(Record.TaskId),
  order: Schema.optionalKey(ScanOrderSchema),
})
export type ConversationQuery = typeof ConversationQuerySchema.Type
export const EntryQuerySchema = Schema.Struct({
  conversationId: Record.ConversationId,
  minEntryId: Schema.optionalKey(Record.EntryId),
  maxEntryId: Schema.optionalKey(Record.EntryId),
  order: Schema.optionalKey(ScanOrderSchema),
})
export type EntryQuery = typeof EntryQuerySchema.Type
export const TaskQuerySchema = Schema.Struct({
  conversationId: Schema.optionalKey(Record.ConversationId),
  kind: Schema.optionalKey(Schema.String),
  status: Schema.optionalKey(
    Schema.Literals(['pending', 'running', 'waiting', 'completing', 'terminal']),
  ),
  abortRequested: Schema.optionalKey(Schema.Boolean),
  background: Schema.optionalKey(Schema.Boolean),
  order: Schema.optionalKey(ScanOrderSchema),
})
export type TaskQuery = typeof TaskQuerySchema.Type
export const SubmissionQuerySchema = Schema.Struct({
  conversationId: Schema.optionalKey(Record.ConversationId),
  status: Schema.optionalKey(Schema.Literals(['queued', 'placed', 'done', 'unanswered'])),
  order: Schema.optionalKey(ScanOrderSchema),
})
export type SubmissionQuery = typeof SubmissionQuerySchema.Type
export const DocumentQuerySchema = Schema.Struct({
  scope: Record.Scope,
  at: Schema.optionalKey(DocumentPointSchema),
  kind: Schema.optionalKey(Schema.String),
  order: Schema.optionalKey(ScanOrderSchema),
})
export type DocumentQuery = typeof DocumentQuerySchema.Type
