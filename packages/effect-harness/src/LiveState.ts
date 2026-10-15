/** Last committed model progress; trailing uncommitted chunks can be lost on close. */
import * as PromptWire from './PromptWire.js'
import * as Schema from 'effect/Schema'
import * as Document from './Document.js'
import * as Identity from './Identity.js'
import * as Usage from './Usage.js'

export const PartialSchema = Schema.Struct({
  message: Schema.toCodecJson(PromptWire.Prompt),
  /** Parameter text is observation only, never an executable tool intent. */
  parameters: Schema.Array(
    Schema.Struct({ id: Schema.String, name: Schema.String, text: Schema.String }),
  ),
  usage: Schema.optionalKey(Usage.RecordSchema),
})
export type Partial = typeof PartialSchema.Type
export const GenerationSchema = Schema.Struct({
  requestId: Identity.RequestId,
  /** Physical outbound attempt, independent of the immutable preparation's initial attempt. */
  attempt: Usage.AttemptId,
  revision: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  partial: Schema.optionalKey(PartialSchema),
  retryAt: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  ),
})
export const SnapshotSchema = Schema.Struct({ generation: Schema.optionalKey(GenerationSchema) })
export type Snapshot = typeof SnapshotSchema.Type
/**
 * Read/watch with Session.snapshot/watch on this conversation target. This is durable
 * progress, not every provider chunk. Terminal response, accounting, tool intents and
 * clearing this slot share one commit. Forks start empty. Recovery preserves the last
 * partial as one interrupted artifact and resends the exact pinned request; external
 * requests and unreported charges are not exactly once. Deferred polling is separate.
 */
export const document = Document.define<typeof SnapshotSchema>({
  kind: 'harness.model.live',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: SnapshotSchema,
  initial: () => ({}),
})
export const target = (conversationId: Identity.ConversationId): Document.Target => ({
  scope: { _tag: 'conversation', conversationId },
})
