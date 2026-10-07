import * as Time from '@effect-harness/harness/Time'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Tool from '@effect-harness/harness/Tool'
import * as Conversation from './Conversation.ts'
import * as Record from './Record.ts'

/** Stored input timestamps are absent on caller-authored and onYield entries. */
export const UserData = Schema.Struct({ timestamp: Schema.optionalKey(Time.EpochMillis) })
export const AssistantData = Schema.Struct({
  timestamp: Schema.optionalKey(Time.EpochMillis),
  harness: Conversation.Metadata,
})
export const SystemData = Schema.Struct({
  harness: Schema.Struct({ system: Conversation.SystemPatch }),
})
/** Durable execution metadata includes both executed and immediately unavailable calls. */
export const ToolResultData = Schema.Struct({
  timestamp: Time.EpochMillis,
  assistantId: Record.EntryId,
  callId: Schema.String,
  name: Schema.String,
  execution: Schema.toCodecJson(Tool.Execution),
})
export const CompactionData = Schema.Struct({
  reason: Schema.Literals(['manual', 'threshold', 'overflow']),
})

/** Checks identity only; the attached decoder validates stored JSON before exposing native parts. */
const token = <K extends string, S extends Schema.Top>(
  kind: K,
  schema: S,
): Record.DecodedEntryToken<K, S> => Record.defineEntryUnsafe(kind, schema)
const user = Schema.Tuple([Schema.toCodecJson(Prompt.UserMessage)])

export const UserEntry = token(
  'harness.user',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.Literal('harness.user'),
    model: user,
    data: Schema.optionalKey(UserData),
  }),
)
export const AssistantEntry = token(
  'harness.assistant',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.Literal('harness.assistant'),
    model: Schema.Tuple([Schema.toCodecJson(Prompt.AssistantMessage)]),
    data: AssistantData,
  }),
)
export const SystemEntry = token(
  'harness.system',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.Literal('harness.system'),
    model: Schema.optionalKey(Schema.Tuple([Schema.toCodecJson(Prompt.SystemMessage)])),
    data: SystemData,
  }),
)
export const ToolResultEntry = token(
  'harness.tool',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.Literal('harness.tool'),
    model: Schema.Tuple([Schema.toCodecJson(Prompt.ToolMessage)]),
    data: ToolResultData,
  }),
)
export const ResetEntry = token(
  'harness.reset',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.Literal('harness.reset'),
    head: Record.EntryId,
    model: Schema.optionalKey(user),
  }).check(Schema.makeFilter((entry) => entry.head === entry.id)),
)
export const CompactionEntry = token(
  'harness.compaction',
  Schema.Struct({
    ...Record.Entry.fields,
    kind: Schema.Literal('harness.compaction'),
    head: Record.EntryId,
    model: user,
    data: CompactionData,
  }),
)
