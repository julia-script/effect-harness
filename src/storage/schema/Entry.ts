import { Schema } from 'effect'
import { ConversationId, EntryId, Seq, TaskId } from './Ids.ts'
import { ModelMessage } from './Json.ts'

/** An immutable override of one visible entry's contribution to model context. */
export const ContextEdit = Schema.Union([
  Schema.Struct({ target: EntryId, action: Schema.Literal('omit') }),
  Schema.Struct({
    target: EntryId,
    action: Schema.Literal('replace'),
    /** Messages contributed instead of the target entry's model messages. */
    messages: Schema.Array(ModelMessage),
  }),
])
export type ContextEdit = typeof ContextEdit.Type

/** Immutable transcript event with separate model-facing and application-facing payloads. */
export const EntryRecord = Schema.Struct({
  id: EntryId,
  conversationId: ConversationId,
  /** Application-defined entry discriminator. */
  kind: Schema.String,
  /** Messages contributed to model context; absent for display or bookkeeping entries. */
  model: Schema.optionalKey(Schema.Array(ModelMessage)),
  /** JSON payload consumed by views, extensions, or bookkeeping logic. */
  data: Schema.optionalKey(Schema.Json),
  /** First entry in the active context selected by this entry. */
  head: Schema.optionalKey(EntryId),
  /** Context-only overrides of earlier visible entries. */
  edits: Schema.optionalKey(Schema.Array(ContextEdit)),
  /** Task that appended this entry, when it was produced by durable work. */
  byTaskId: Schema.optionalKey(TaskId),
})
export type EntryRecord = typeof EntryRecord.Type

/** An entry together with the sequence of the commit that persisted it. */
export const StoredEntry = Schema.Struct({ entry: EntryRecord, commitSeq: Seq })
export type StoredEntry = typeof StoredEntry.Type

/** Entry content supplied before the Session assigns identity and task attribution. */
export const EntryDraft = Schema.Struct({
  kind: EntryRecord.fields.kind,
  model: EntryRecord.fields.model,
  data: EntryRecord.fields.data,
  edits: EntryRecord.fields.edits,
  /** `'self'` starts active context at the newly assigned entry ID. */
  head: Schema.optionalKey(Schema.Union([EntryId, Schema.Literal('self')])),
})
export type EntryDraft = typeof EntryDraft.Type
