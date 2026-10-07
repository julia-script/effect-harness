/**
 * Schema-derived conversation entry data and typed entry tokens.
 *
 * @since 0.0.0
 */
import * as Time from '@effect-harness/harness/Time'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Tool from '@effect-harness/harness/Tool'
import * as Conversation from './Conversation.ts'
import * as Record from './Record.ts'

/**
 * Stored input timestamps are absent on caller-authored and onYield entries.
 *
 * @category schemas
 * @since 0.0.0
 */
export const UserData = Schema.Struct({ timestamp: Schema.optionalKey(Time.EpochMillis) })
/**
 * Decoded UserData values.
 *
 * @category models
 * @since 0.0.0
 */
export type UserData = typeof UserData.Type

/**
 * AssistantData schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const AssistantData = Schema.Struct({
  timestamp: Schema.optionalKey(Time.EpochMillis),
  harness: Conversation.Metadata,
})
/**
 * Decoded AssistantData values.
 *
 * @category models
 * @since 0.0.0
 */
export type AssistantData = typeof AssistantData.Type

/**
 * SystemData schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SystemData = Schema.Struct({
  harness: Schema.Struct({ system: Conversation.SystemPatch }),
})
/**
 * Decoded SystemData values.
 *
 * @category models
 * @since 0.0.0
 */
export type SystemData = typeof SystemData.Type

/**
 * Durable execution metadata includes both executed and immediately unavailable calls.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolResultData = Schema.Struct({
  timestamp: Time.EpochMillis,
  assistantId: Record.EntryId,
  callId: Schema.String,
  name: Schema.String,
  execution: Schema.toCodecJson(Tool.Execution),
})
/**
 * Decoded ToolResultData values.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolResultData = typeof ToolResultData.Type

/**
 * CompactionData schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const CompactionData = Schema.Struct({
  reason: Schema.Literals(['manual', 'threshold', 'overflow']),
})
/**
 * Decoded CompactionData values.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionData = typeof CompactionData.Type

/** Checks identity only; the attached decoder validates stored JSON before exposing native parts. */
const token = <K extends string, S extends Schema.Top>(
  kind: K,
  schema: S,
): Record.DecodedEntryToken<K, S> => Record.defineEntryUnsafe(kind, schema)
const user = Schema.Tuple([Schema.toCodecJson(Prompt.UserMessage)])

/**
 * UserEntry schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const UserEntry = token(
  'harness.user',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.user'),
    model: user,
    data: Schema.optionalKey(UserData),
  }),
)
/**
 * AssistantEntry schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const AssistantEntry = token(
  'harness.assistant',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.assistant'),
    model: Schema.Tuple([Schema.toCodecJson(Prompt.AssistantMessage)]),
    data: AssistantData,
  }),
)
/**
 * SystemEntry schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SystemEntry = token(
  'harness.system',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.system'),
    model: Schema.optionalKey(Schema.Tuple([Schema.toCodecJson(Prompt.SystemMessage)])),
    data: SystemData,
  }),
)
/**
 * ToolResultEntry schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolResultEntry = token(
  'harness.tool',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.tool'),
    model: Schema.Tuple([Schema.toCodecJson(Prompt.ToolMessage)]),
    data: ToolResultData,
  }),
)
/**
 * ResetEntry schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ResetEntry = token(
  'harness.reset',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.reset'),
    head: Record.EntryId,
    model: Schema.optionalKey(user),
  }).check(Schema.makeFilter((entry) => entry.head === entry.id)),
)
/**
 * CompactionEntry schema.
 *
 * @category schemas
 * @since 0.0.0
 */
export const CompactionEntry = token(
  'harness.compaction',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.compaction'),
    head: Record.EntryId,
    model: user,
    data: CompactionData,
  }),
)
