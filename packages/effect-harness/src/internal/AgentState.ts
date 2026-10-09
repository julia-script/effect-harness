/** Runtime-owned persisted conversation configuration. */
import type * as Record from '../Record.js'
import * as Agent from '../Agent.js'
import * as Document from '../Document.js'

export const document = Document.define({
  kind: 'harness.agent',
  scope: 'conversation',
  version: 1,
  history: 'rewindable',
  fork: 'asOf',
  schema: Agent.StateSchema,
  initial: () => ({}),
})
export const target = (conversationId: Record.ConversationId): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
})
