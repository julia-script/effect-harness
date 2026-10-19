/** Durable summary work lives separately from generation's replaceable live document. */
import * as Schema from 'effect/Schema'
import * as Document from './Document.js'
import * as Record from './Record.js'
import * as Compaction from './Compaction.js'

export const ItemSchema = Schema.Struct({
  taskId: Record.TaskId,
  reason: Compaction.ReasonSchema,
  blocking: Schema.Boolean,
  phase: Schema.Literals(['select', 'summarize', 'retry']),
  attempt: Schema.Natural,
  retryAt: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  ),
})
export const SnapshotSchema = Schema.Struct({ items: Schema.Array(ItemSchema) })
export const document = Document.define({
  kind: 'harness.compactionStatus',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: SnapshotSchema,
  initial: () => ({ items: [] }),
})
export const target = (conversationId: Record.ConversationId): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
})
