/**
 * Schema-derived conversation entry data and typed entry tokens.
 */
import * as Time from 'effect-harness/Time'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Conversation from './Conversation.ts'
import * as Record from './Record.ts'

/**
 * Stored input timestamps are absent on caller-authored and onYield entries.
 *
 * @category schemas
 */
export const UserData = Schema.Struct({
  timestamp: Schema.optionalKey(Time.DateTimeUtcFromEpochMillis),
})
/**
 * Optional stored timestamp attached to a user entry.
 *
 * @category models
 */
export type UserData = typeof UserData.Type

/**
 * Schema for optional timestamp and committed generation metadata for an assistant entry.
 *
 * @category schemas
 */
export const AssistantData = Schema.Struct({
  ...UserData.fields,
  harness: Conversation.Metadata,
})
/**
 * Optional timestamp and committed generation metadata for an assistant entry.
 *
 * @category models
 */
export type AssistantData = typeof AssistantData.Type

/**
 * Schema for managed system-section and tool-declaration patch recorded in history.
 *
 * @category schemas
 */
export const SystemData = Schema.Struct({
  harness: Schema.Struct({ system: SystemPatch.SystemPatch }),
})
/**
 * Managed system-section and tool-declaration patch recorded in history.
 *
 * @category models
 */
export type SystemData = typeof SystemData.Type

/**
 * Schema for timestamp, call identity and encoded execution result of a settled tool.
 *
 * @category schemas
 */
export const ToolResultData = Schema.Struct({
  timestamp: Time.DateTimeUtcFromEpochMillis,
  assistantId: Record.EntryId,
  callId: Schema.String,
  name: Schema.String,
  execution: Schema.toCodecJson(ToolRegistration.Execution),
})
/**
 * Timestamp, call identity and encoded execution result of a settled tool.
 *
 * @category models
 */
export type ToolResultData = typeof ToolResultData.Type

/**
 * Schema for manual, threshold or overflow reason recorded with a compaction entry.
 *
 * @category schemas
 */
export const CompactionData = Schema.Struct({
  reason: Schema.Literals(['manual', 'threshold', 'overflow']),
})
/**
 * Manual, threshold or overflow reason recorded with a compaction entry.
 *
 * @category models
 */
export type CompactionData = typeof CompactionData.Type

/** Creates a decoded entry token through its unchecked synchronous definition boundary. */
const tokenUnsafe = <K extends string, S extends Schema.Top>(
  kind: K,
  schema: S,
): Record.DecodedEntryToken<K, S> => Record.defineEntryUnsafe(kind, schema)
const user = Schema.Tuple([Schema.toCodecJson(Prompt.UserMessage)])

/**
 * Typed token for user-message entries.
 *
 * **Details**
 *
 * kind identifies the stored entry family. is checks identity only; decode validates stored
 * data and native model content before exposing decoded values.
 *
 * **Gotchas**
 *
 * Use decode at a persisted or external data boundary; an identity match does not validate
 * an entry’s payload.
 *
 * @category models
 */
export const UserEntry = tokenUnsafe(
  'harness.user',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.user'),
    model: user,
    data: Schema.optionalKey(UserData),
  }),
)
/**
 * Typed token for assistant-response entries.
 *
 * **Details**
 *
 * kind identifies the stored entry family. is checks identity only; decode validates stored
 * data and native model content before exposing decoded values.
 *
 * **Gotchas**
 *
 * Use decode at a persisted or external data boundary; an identity match does not validate
 * an entry’s payload.
 *
 * @category models
 */
export const AssistantEntry = tokenUnsafe(
  'harness.assistant',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.assistant'),
    model: Schema.Tuple([Schema.toCodecJson(Prompt.AssistantMessage)]),
    data: AssistantData,
  }),
)
/**
 * Typed token for managed system-patch entries.
 *
 * **Details**
 *
 * kind identifies the stored entry family. is checks identity only; decode validates stored
 * data and native model content before exposing decoded values.
 *
 * **Gotchas**
 *
 * Use decode at a persisted or external data boundary; an identity match does not validate
 * an entry’s payload.
 *
 * @category models
 */
export const SystemEntry = tokenUnsafe(
  'harness.system',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.system'),
    model: Schema.optionalKey(Schema.Tuple([Schema.toCodecJson(Prompt.SystemMessage)])),
    data: SystemData,
  }),
)
/**
 * Typed token for committed tool-result entries.
 *
 * **Details**
 *
 * kind identifies the stored entry family. is checks identity only; decode validates stored
 * data and native model content before exposing decoded values.
 *
 * **Gotchas**
 *
 * Use decode at a persisted or external data boundary; an identity match does not validate
 * an entry’s payload.
 *
 * @category models
 */
export const ToolResultEntry = tokenUnsafe(
  'harness.tool',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.tool'),
    model: Schema.Tuple([Schema.toCodecJson(Prompt.ToolMessage)]),
    data: ToolResultData,
  }),
)
/**
 * Typed token for reset entries establishing a new active context head.
 *
 * **Details**
 *
 * kind identifies the stored entry family. is checks identity only; decode validates stored
 * data and native model content before exposing decoded values.
 *
 * **Gotchas**
 *
 * Use decode at a persisted or external data boundary; an identity match does not validate
 * an entry’s payload.
 *
 * @category models
 */
export const ResetEntry = tokenUnsafe(
  'harness.reset',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.reset'),
    head: Record.EntryId,
    model: Schema.optionalKey(user),
  }).check(Schema.makeFilter((entry) => entry.head === entry.id)),
)
/**
 * Typed token for summary entries establishing a compaction boundary.
 *
 * **Details**
 *
 * kind identifies the stored entry family. is checks identity only; decode validates stored
 * data and native model content before exposing decoded values.
 *
 * **Gotchas**
 *
 * Use decode at a persisted or external data boundary; an identity match does not validate
 * an entry’s payload.
 *
 * @category models
 */
export const CompactionEntry = tokenUnsafe(
  'harness.compaction',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.tag('harness.compaction'),
    head: Record.EntryId,
    model: user,
    data: CompactionData,
  }),
)

/** Checks the decoded UserData contract without decoding or coercing input.
 * @category guards
 */
export const isUserData: (u: unknown) => u is UserData = Schema.is(Schema.toType(UserData))

/** Checks the decoded AssistantData contract without decoding or coercing input.
 * @category guards
 */
export const isAssistantData: (u: unknown) => u is AssistantData = Schema.is(
  Schema.toType(AssistantData),
)

/** Checks the decoded SystemData contract without decoding or coercing input.
 * @category guards
 */
export const isSystemData: (u: unknown) => u is SystemData = Schema.is(Schema.toType(SystemData))

/** Checks the decoded ToolResultData contract without decoding or coercing input.
 * @category guards
 */
export const isToolResultData: (u: unknown) => u is ToolResultData = Schema.is(
  Schema.toType(ToolResultData),
)

/** Checks the decoded CompactionData contract without decoding or coercing input.
 * @category guards
 */
export const isCompactionData: (u: unknown) => u is CompactionData = Schema.is(
  Schema.toType(CompactionData),
)

import * as SystemPatch from 'effect-harness/SystemPatch'
