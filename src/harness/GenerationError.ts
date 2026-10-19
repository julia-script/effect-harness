/**
 * Errors of the generation task. Each faults the task: the run's inputs settle `unanswered`.
 */
import { Schema } from 'effect'
import { ConversationId } from '../storage/schema/Ids.ts'

/** A generation ran in a conversation with no entries, so there is nothing to send. */
export class EmptyConversation extends Schema.TaggedError<
  EmptyConversation,
  { readonly brand: unique symbol }
>('effect-harness/harness/GenerationError/EmptyConversation')('EmptyConversation', {
  message: Schema.String,
  conversationId: ConversationId,
}) {}
