/** A conversation capability owned by one live harness. */
import type * as Agent from './Agent.ts'
import type * as Document from './Document.ts'
import type * as Record from './Record.ts'
import type * as Harness from './Harness.ts'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Task from './Task.ts'
export { AgentDoc, Metadata, Data } from './internal/ConversationState.ts'
export { UsageDoc } from './internal/Usage.ts'
export { Conversation as Record, ConversationId as Id, Entry, EntryId } from './Record.ts'

export interface Conversation {
  readonly id: Record.ConversationId
  readonly harness: Harness.Service
}
export const submit = (
  self: Conversation,
  input: string | ReadonlyArray<Prompt.UserMessagePart>,
  options?: Harness.SubmitOptions,
) => self.harness.submit(self.id, input, options)
export const append = (self: Conversation, draft: Record.Entry.Draft) =>
  self.harness.append(self.id, draft)
/** Start an empty model context while preserving the stored conversation history. */
export const reset = (self: Conversation) =>
  self.harness.transaction((tx) => tx.appendEntry(self.id, { kind: 'harness.reset', head: 'self' }))
export const configure = (self: Conversation, change: Agent.State.Change) =>
  self.harness.configure(self.id, change)
export const fork = (self: Conversation, at: Record.EntryId) => self.harness.fork(self.id, at)
export const abort = (self: Conversation, includeBackground = false) =>
  self.harness.abort(self.id, includeBackground)
export const awaitIdle = (self: Conversation, includeBackground = false) =>
  self.harness.awaitIdle(self.id, includeBackground)
export const compact = (self: Conversation, instructions?: string) =>
  self.harness.compact(self.id, instructions)
export const watch = (self: Conversation) => self.harness.watch(self.id)
export const snapshot = (self: Conversation) => self.harness.snapshot(self.id)
export const spawn = (
  self: Conversation,
  definition: Task.BoundDefinition,
  input: unknown,
  options?: { readonly owner?: Record.TaskId; readonly background?: boolean },
) => self.harness.spawn(definition, input, { ...options, conversationId: self.id })
export const document = <T extends object>(
  self: Conversation,
  token: Document.Document<T>,
  key?: string,
) => self.harness.document(token, { owner: self.id, ...(key === undefined ? {} : { key }) })
