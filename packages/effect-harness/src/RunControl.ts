/** Durable run membership; this foundation does not activate a new scheduler. */
import * as Schema from 'effect/Schema'
import * as Document from './Document.js'
import * as Record from './Record.js'
import * as Transaction from './Transaction.js'
import * as ContextProtocol from './internal/ContextProtocol.js'
import * as CompactionPlan from './internal/CompactionPlan.js'

/** Serializable queue policy; steering never interrupts active tools. */
export const ModeSchema = Schema.Literals(['followUp', 'steer', 'write'])
export const InboxItemSchema = Schema.Struct({
  submissionId: Record.SubmissionId,
  mode: ModeSchema,
  payload: Transaction.EntryDraftSchema,
  /** Internal admission only. Older readers cannot safely ignore this placement condition. */
  compaction: Schema.optionalKey(Schema.toCodecJson(CompactionPlan.FingerprintSchema)),
})
export type InboxItem = typeof InboxItemSchema.Type
export const InboxSchema = Schema.Struct({
  items: Schema.Array(InboxItemSchema).check(
    Schema.makeFilter(
      (items) =>
        items.every((item, index) => item.submissionId > (items[index - 1]?.submissionId ?? 0)),
      { message: 'Inbox items must be unique and ascending' },
    ),
  ),
})
export const inbox = Document.define({
  kind: 'harness.inbox',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: InboxSchema,
  initial: () => ({ items: [] }),
})
export const QueueModesSchema = Schema.Struct({
  followUp: Schema.optionalKey(Schema.Literals(['one', 'all'])),
  steering: Schema.optionalKey(Schema.Literals(['one', 'all'])),
})
export type QueueModes = typeof QueueModesSchema.Type

/** Each input occurs once, in admission ID order, in one active run. */
export const MembershipSchema = Schema.Struct({
  taskId: Record.TaskId,
  submissionIds: Schema.Array(Record.SubmissionId).check(
    Schema.makeFilter((ids) => ids.every((id, index) => id > (ids[index - 1] ?? 0)), {
      message: 'Run inputs must be unique and ascending',
    }),
  ),
})
export type Membership = typeof MembershipSchema.Type

export const StateSchema = Schema.Struct({ active: Schema.optionalKey(MembershipSchema) })
export type State = typeof StateSchema.Type

/** Conversation-owned coordination survives reopen but starts empty on a fork. */
export const document = Document.define({
  kind: 'harness.runControl',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: StateSchema,
  initial: () => ({}),
})

export const target = (conversationId: Record.ConversationId): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
})

/** Immutable legacy interpretation cutoff. Forks copy it instead of recapturing a tail. */
export const contextProtocol = Document.define({
  kind: 'harness.contextProtocol',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'current',
  schema: ContextProtocol.ProtocolSchema,
  initial: (): ContextProtocol.Protocol => ({
    version: 1,
    origin: { conversationId: Record.ROOT_CONVERSATION_ID },
    legacyMode: 'preserveManaged',
  }),
})
