import { Schema } from 'effect'
import { ConversationId, EntryId, TaskId } from './Ids.ts'

/** Immutable identity, history ancestry, and task ownership of a transcript scope. */
export const ConversationRecord = Schema.Struct({
  id: ConversationId,
  /** Fork source and inclusive parent entry through which history is inherited. */
  parent: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, at: EntryId })),
  /** Creator edge used for attribution, subtree abort, and subtree idle waits. */
  owner: Schema.optionalKey(Schema.Struct({ conversationId: ConversationId, taskId: TaskId })),
})
export type ConversationRecord = typeof ConversationRecord.Type

/** Ownership selected explicitly whenever a conversation is created. */
export const ConversationOwnership = Schema.Union([
  Schema.Struct({ kind: Schema.Literal('ownerless') }),
  Schema.Struct({ kind: Schema.Literal('task'), taskId: TaskId }),
])
export type ConversationOwnership = typeof ConversationOwnership.Type
