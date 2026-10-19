/**
 * What a client sees of the loop, as Schemas both sides share: the states of the built-in
 * `pi.live` and `pi.usage` documents, one conversation's view and model context, and the task graph, ported from
 * pi's `harness/view.ts` and `harness/task-graph.ts` (spec §9.3, §9.5).
 *
 * A view is watched: its value when the watch began, then each commit's change of it with the
 * operations that turn the previous value into the next.
 */
import { Schema } from 'effect'
import type { Stream } from 'effect'
import { AssistantMessage, Message, Usage } from '../model/Message.ts'
import { ConversationRecord } from '../storage/schema/Conversation.ts'
import { Op } from '../storage/schema/Document.ts'
import { EntryRecord } from '../storage/schema/Entry.ts'
import { ConversationId, EntryId, SubmissionId, TaskId } from '../storage/schema/Ids.ts'
import { JsonObject } from '../storage/schema/Json.ts'
import { JoinPolicy } from '../storage/schema/Task.ts'

/** The task that settles a run's inputs, and those inputs. */
export const Run = Schema.Struct({ taskId: TaskId, inputs: Schema.Array(SubmissionId) })
export type Run = typeof Run.Type

/** The current generation attempt. */
export const Generation = Schema.Struct({
  attempt: Schema.Finite,
  /** The committed, throttled partial of the response being generated. */
  message: Schema.optionalKey(AssistantMessage),
  /** Durable backoff before the next attempt. */
  retry: Schema.optionalKey(Schema.Struct({ at: Schema.Finite, error: Schema.String })),
  /** A deferred response being polled. */
  deferred: Schema.optionalKey(Schema.Struct({ pollAt: Schema.Finite })),
})
export type Generation = typeof Generation.Type

/** One call of the current tool round. */
export const ToolSlot = Schema.Struct({
  callId: Schema.String,
  name: Schema.String,
  status: Schema.Literals(['pending', 'running', 'done']),
  /**
   * Absent for a call not started yet (sequential round), and for a call its request did not
   * offer, which starts `done` with the result the generation wrote.
   */
  taskId: Schema.optionalKey(TaskId),
  /** The result entry once done; absent when the tool task faulted or was orphaned. */
  entry: Schema.optionalKey(EntryId),
})
export type ToolSlot = typeof ToolSlot.Type

/** Why a compaction runs: `compact()`, a threshold in preparation, or a context overflow. */
export const CompactionReason = Schema.Literals(['manual', 'threshold', 'overflow'])
export type CompactionReason = typeof CompactionReason.Type

/** One live compaction task. */
export const CompactionStatus = Schema.Struct({
  taskId: TaskId,
  reason: CompactionReason,
  /** Whether a generation waits for it: one the generation owns. */
  blocking: Schema.Boolean,
  attempt: Schema.Finite,
  /** Durable backoff before the next summarization attempt. */
  retry: Schema.optionalKey(Schema.Struct({ at: Schema.Finite, error: Schema.String })),
})
export type CompactionStatus = typeof CompactionStatus.Type

/** The newest compaction that failed since one last produced a summary. */
export const CompactionFailure = Schema.Struct({
  taskId: TaskId,
  reason: CompactionReason,
  error: Schema.String,
})
export type CompactionFailure = typeof CompactionFailure.Type

export const LiveState = Schema.Struct({
  /** Present exactly while the conversation is busy. */
  run: Schema.optionalKey(Run),
  generation: Schema.optionalKey(Generation),
  /** The current tool round in call order, from the tool-calling answer until the round ends. */
  tools: Schema.optionalKey(Schema.Array(ToolSlot)),
  /** Live compaction tasks in task ID order; absent when none. */
  compactions: Schema.optionalKey(Schema.Array(CompactionStatus)),
  /**
   * The newest compaction failure, kept after its task ended. A compaction that produces a summary
   * removes it; one that is aborted or finds nothing to compact leaves it.
   */
  compactionFailure: Schema.optionalKey(CompactionFailure),
})
export type LiveState = typeof LiveState.Type

/** The `pi.usage` ledger of one conversation's own spend. */
export const UsageState = Schema.Struct({
  /** Assistant entries and summarization attempts, keyed `provider/modelId`. */
  models: Schema.Record(Schema.String, Usage),
  /** Tool results, keyed by tool name. */
  tools: Schema.Record(Schema.String, Usage),
})
export type UsageState = typeof UsageState.Type

/** The built-in conversation documents a view shows, by kind. */
export const VIEW_KINDS = ['pi.agent', 'pi.live', 'pi.inbox', 'pi.usage'] as const

/** One conversation's active transcript and built-in documents (spec §9.3). */
export const ConversationView = Schema.Struct({
  conversation: ConversationRecord,
  /** The active entries: the head marker, then the entries from its head that are not markers. */
  entries: Schema.Array(EntryRecord),
  /** The built-in documents of `VIEW_KINDS` by kind; an absent document is absent. */
  docs: Schema.Record(Schema.String, JsonObject),
})
export type ConversationView = typeof ConversationView.Type

/**
 * What the model sees of a conversation (spec §2.1): the messages its next request would send, and
 * the active entries they came from.
 */
export const ContextView = Schema.Struct({
  /** The newest applicable head marker, if any. */
  head: Schema.optionalKey(EntryRecord),
  /** The active entries, oldest first: the head marker, then the entries from its head that are not markers. */
  entries: Schema.Array(EntryRecord),
  /** Per entry of `entries`, its messages after context edits and excluded stop reasons, before tool result ordering. */
  contributions: Schema.Array(Schema.Array(Message)),
  /** The model context of the next request. */
  messages: Schema.Array(Message),
})
export type ContextView = typeof ContextView.Type

/** A live task's durable status, without its checkpoint and outcome payloads. */
export const TaskGraphState = Schema.Union([
  Schema.Struct({ status: Schema.Literals(['pending', 'running']), phase: Schema.String }),
  Schema.Struct({
    status: Schema.Literal('waiting'),
    phase: Schema.String,
    on: Schema.Array(TaskId),
    policy: JoinPolicy,
  }),
  /** An outcome held until the task's ordinary owned work drains. */
  Schema.Struct({
    status: Schema.Literal('completing'),
    outcome: Schema.Literals(['completed', 'failed', 'aborted', 'orphaned', 'faulted']),
  }),
])
export type TaskGraphState = typeof TaskGraphState.Type

export const TaskGraphNode = Schema.Struct({
  id: TaskId,
  kind: Schema.String,
  conversationId: ConversationId,
  /** The owner task; absent for a task its conversation owns. */
  owner: Schema.optionalKey(TaskId),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
  state: TaskGraphState,
  /** The conversations this task owns, in ID order. */
  conversations: Schema.Array(ConversationId),
})
export type TaskGraphNode = typeof TaskGraphNode.Type

/** Every live task of the Session, keyed by its decimal ID (spec §9.5). */
export const TaskGraph = Schema.Struct({ tasks: Schema.Record(Schema.String, TaskGraphNode) })
export type TaskGraph = typeof TaskGraph.Type

/** One frame of a watched view on the wire: its value when the watch began, then changes. */
export const Frame = <S extends Schema.Top>(value: S) =>
  Schema.Union([
    Schema.Struct({ type: Schema.Literal('value'), value }),
    Schema.Struct({ type: Schema.Literal('change'), ops: Schema.Array(Op) }),
  ])

/** One commit's change of a watched view. */
export interface Change<T> {
  readonly value: T
  /** Operations that turn the previous value into `value`; `[['r', value]]` replaces it whole. */
  readonly ops: ReadonlyArray<Op>
}

/**
 * A watch of a view. `changes` can be run once; it ends when the loop closes, and fails with `E`
 * when it fails. When more than 100 changes are pending, they collapse into one replacement.
 */
export interface Watch<T, E> {
  readonly value: T
  readonly changes: Stream.Stream<Change<T>, E>
}
