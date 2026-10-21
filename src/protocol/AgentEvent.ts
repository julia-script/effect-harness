/**
 * Agent events, ported from pi's experimental `harness/events.ts` (spec §9.4): one conversation's
 * activity shaped like a coding agent's session events, as Schemas both the client and the loop
 * use. A watch starts with a `snapshot` and then delivers one batch of events per commit.
 *
 * Not ported yet: `tool_execution_update` and nested calls, which need live tool progress.
 */
import { Schema } from 'effect'
import type { Stream } from 'effect'
import { AssistantContent, AssistantMessage, Usage } from '../model/Message.ts'
import { EntryRecord } from '../storage/schema/Entry.ts'
import { SubmissionId, TaskId } from '../storage/schema/Ids.ts'
import { JsonObject, ModelMessage } from '../storage/schema/Json.ts'
import { SubmissionRecord } from '../storage/schema/Submission.ts'
import { State as AgentState } from './AgentState.ts'
import {
  CompactionFailure,
  CompactionReason,
  CompactionStatus,
  Generation,
  ToolSlot,
  UsageState,
} from './ViewState.ts'

const contentIndex = Schema.Int

/** One change to the in-flight assistant message. */
export const MessageChange = Schema.Union([
  Schema.Struct({
    type: Schema.Literals(['text_start', 'thinking_start', 'toolcall_start']),
    contentIndex,
    block: AssistantContent,
  }),
  Schema.Struct({
    type: Schema.Literals(['text_delta', 'thinking_delta']),
    contentIndex,
    delta: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal('toolcall_delta'),
    contentIndex,
    path: Schema.Array(Schema.Union([Schema.String, Schema.Int])),
    delta: Schema.String,
  }),
  /** The block replaced whole. */
  Schema.Struct({ type: Schema.Literal('block'), contentIndex, block: AssistantContent }),
  /** The message replaced whole. */
  Schema.Struct({ type: Schema.Literal('message'), message: AssistantMessage }),
])
export type MessageChange = typeof MessageChange.Type

/** A queued submission, as `$inbox` holds it. */
export const QueuedItem = Schema.Struct({
  id: SubmissionId,
  mode: Schema.Literals(['steer', 'followUp', 'write']),
})
export type QueuedItem = typeof QueuedItem.Type

/** The state a watch starts from. */
export const Snapshot = Schema.Struct({
  type: Schema.Literal('snapshot'),
  entries: Schema.Array(EntryRecord),
  run: Schema.optionalKey(Schema.Struct({ inputs: Schema.Array(SubmissionId) })),
  /** The current generation attempt: its partial, retry backoff, or deferred poll. */
  generation: Schema.optionalKey(Generation),
  tools: Schema.Array(ToolSlot),
  compactions: Schema.Array(CompactionStatus),
  compactionFailure: Schema.optionalKey(CompactionFailure),
  inbox: Schema.Array(QueuedItem),
  /** `$agent`; `{}` when absent. */
  agent: AgentState,
  usage: UsageState,
})
export type Snapshot = typeof Snapshot.Type

/**
 * Which call a tool event is about: the provider's call ID, and the call's tool task, absent for
 * a call that never got one.
 */
const call = {
  toolCallId: Schema.String,
  toolName: Schema.String,
  taskId: Schema.optionalKey(TaskId),
}

const event = <const Type extends string, const Fields extends Schema.Struct.Fields>(
  type: Type,
  fields: Fields,
) => Schema.Struct({ type: Schema.Literal(type), ...fields })

const inputs = { inputs: Schema.Array(SubmissionId) }
const compaction = { taskId: TaskId, reason: CompactionReason }

export const AgentEvent = Schema.Union([
  Snapshot,
  event('run_start', inputs),
  event('run_end', inputs),
  event('turn_start', {}),
  event('turn_end', {}),
  event('message_start', { message: ModelMessage }),
  /** `usage` is the partial's current usage. */
  event('message_update', { usage: Usage, changes: Schema.Array(MessageChange) }),
  event('message_end', { entry: EntryRecord }),
  event('tool_execution_start', { ...call, args: JsonObject }),
  /** Ends without `entry` when the tool task faulted or was orphaned, or its run ended first. */
  event('tool_execution_end', { ...call, entry: Schema.optionalKey(EntryRecord) }),
  event('inbox_update', { items: Schema.Array(QueuedItem) }),
  event('submission', { record: SubmissionRecord }),
  event('auto_retry_start', {
    attempt: Schema.Finite,
    at: Schema.Finite,
    errorMessage: Schema.String,
  }),
  event('auto_retry_end', { attempt: Schema.Finite }),
  event('deferred_poll', { pollAt: Schema.Finite }),
  /** An entry without a model message. */
  event('entry_appended', { entry: EntryRecord }),
  event('agent_changed', { agent: AgentState }),
  event('usage_changed', { usage: UsageState }),
  event('task_failed', { taskId: TaskId, kind: Schema.String, message: Schema.String }),
  event('compaction_start', { ...compaction, blocking: Schema.Boolean }),
  /** `error` is set when the compaction failed, faulted, or was orphaned. */
  event('compaction_end', { ...compaction, error: Schema.optionalKey(Schema.String) }),
])
export type AgentEvent = typeof AgentEvent.Type

/**
 * A watch of one conversation's events. `batches` can be run once: one batch per commit that
 * caused any, ending when the loop closes and failing with `E` when it fails. When more than 100
 * batches are pending, they are replaced with one holding a fresh `snapshot`.
 */
export interface Events<E> {
  readonly snapshot: Snapshot
  readonly batches: Stream.Stream<ReadonlyArray<AgentEvent>, E>
}
