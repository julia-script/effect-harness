/** Ordered committed transitions, with complete snapshot/rebase recovery rather than replay promises. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Stream from 'effect/Stream'
import * as Observation from './Observation.js'
import * as View from './ConversationView.js'
import * as Record from './Record.js'
import * as Sequence from './Sequence.js'
import * as Live from './LiveState.js'
import * as Run from './RunControl.js'
import * as Progress from './ToolProgress.js'
import * as Agent from './Agent.js'
import * as Compaction from './CompactionStatus.js'
import * as Graph from './TaskGraph.js'
import * as Conversation from './Conversation.js'
import { ObservationError } from './ObservationError.js'
import * as DocumentValue from './internal/DocumentValue.js'

export const EventSchema = Schema.Union([
  Schema.TaggedStruct('Message', { entry: Record.Entry }),
  Schema.TaggedStruct('RunStart', { membership: Run.MembershipSchema }),
  Schema.TaggedStruct('RunEnd', {
    membership: Run.MembershipSchema,
    outcome: Schema.optionalKey(Schema.suspend(() => View.TerminalSchema)),
  }),
  Schema.TaggedStruct('TurnStart', { generation: Schema.suspend(() => View.GenerationSchema) }),
  Schema.TaggedStruct('TurnEnd', { generation: Schema.suspend(() => View.GenerationSchema) }),
  Schema.TaggedStruct('GenerationStart', { generation: Live.GenerationSchema }),
  Schema.TaggedStruct('GenerationEnd', { generation: Live.GenerationSchema }),
  Schema.TaggedStruct('Phase', { generation: Schema.suspend(() => View.GenerationSchema) }),
  Schema.TaggedStruct('Partial', { generation: Live.GenerationSchema }),
  Schema.TaggedStruct('ToolStart', {
    ownerTaskId: Record.TaskId,
    slot: Schema.suspend(() => View.RoundSlotSchema),
  }),
  Schema.TaggedStruct('ToolEnd', {
    ownerTaskId: Record.TaskId,
    slot: Schema.suspend(() => View.RoundSlotSchema),
    outcome: Schema.optionalKey(Schema.suspend(() => View.TerminalSchema)),
  }),
  Schema.TaggedStruct('NestedStart', { nested: Schema.suspend(() => View.NestedSchema) }),
  Schema.TaggedStruct('NestedEnd', { nested: Schema.suspend(() => View.NestedSchema) }),
  Schema.TaggedStruct('Progress', { key: Schema.String, value: Progress.Snapshot }),
  Schema.TaggedStruct('CompactionStart', { item: Compaction.ItemSchema }),
  Schema.TaggedStruct('CompactionUpdate', { item: Compaction.ItemSchema }),
  Schema.TaggedStruct('CompactionEnd', {
    taskId: Record.TaskId,
    outcome: Schema.optionalKey(Schema.suspend(() => View.TerminalSchema)),
  }),
  Schema.TaggedStruct('Task', { node: Graph.NodeSchema }),
  Schema.TaggedStruct('TaskEnd', { outcome: Schema.suspend(() => View.TerminalSchema) }),
  Schema.TaggedStruct('Submission', { submission: Record.Submission }),
  Schema.TaggedStruct('Inbox', { value: Run.InboxSchema }),
  Schema.TaggedStruct('Agent', { value: Agent.StateSchema }),
  Schema.TaggedStruct('Usage', { value: Schema.suspend(() => View.UsageSchema) }),
  Schema.TaggedStruct('Conversation', { conversation: Record.Conversation }),
  Schema.TaggedStruct('Subagent', { value: Schema.suspend(() => View.SubagentSchema) }),
])
export type Event = typeof EventSchema.Type
export const FrameSchema = Schema.Union([
  Schema.suspend(() => View.SnapshotSchema),
  Schema.TaggedStruct('Rebase', {
    protocol: Schema.Literal(1),
    target: Schema.suspend(() => View.TargetSchema),
    revision: Observation.RevisionSchema,
    commitSeq: Schema.optionalKey(Sequence.SequenceSchema),
    value: Schema.suspend(() => View.ValueSchema),
  }),
  Schema.TaggedStruct('Batch', {
    protocol: Schema.Literal(1),
    target: Schema.suspend(() => View.TargetSchema),
    epoch: Schema.NonEmptyString,
    from: Observation.CounterSchema,
    to: Observation.CounterSchema,
    commitSeq: Schema.optionalKey(Sequence.SequenceSchema),
    events: Schema.Array(EventSchema),
    value: Schema.suspend(() => View.ValueSchema),
  }),
])
export type Frame = typeof FrameSchema.Type
export const SnapshotSchema = Schema.suspend(() => View.SnapshotSchema)
export type Snapshot = View.Snapshot
export const reduce = Effect.fnUntraced(function* (
  current: Snapshot,
  frame: Frame,
): Effect.fn.Return<Snapshot, ObservationError> {
  const invalid = (cause: Schema.SchemaError) =>
    new ObservationError({
      reason: 'invalid',
      operation: 'events.reduce',
      message: cause.message,
      cause,
    })
  const self = yield* DocumentValue.copy(SnapshotSchema, current).pipe(Effect.mapError(invalid))
  const next = yield* DocumentValue.copy(FrameSchema, frame).pipe(Effect.mapError(invalid))
  const revision = yield* Observation.check(self, next)
  return {
    _tag: 'Snapshot',
    protocol: 1,
    target: self.target,
    revision,
    ...(next.commitSeq === undefined ? {} : { commitSeq: next.commitSeq }),
    value: next.value,
  }
})
export const watch = (
  self: Conversation.Conversation,
  options?: Observation.WatchOptions,
): Stream.Stream<Frame, Observation.Failure> => Conversation.events(self, options)
export const watchWith = <A, E, R>(
  self: Conversation.Conversation,
  callback: (frame: Frame) => Effect.Effect<A, E, R>,
  options?: Observation.WatchOptions,
): Stream.Stream<A, E | Observation.Failure, R> => Conversation.eventsWith(self, callback, options)
