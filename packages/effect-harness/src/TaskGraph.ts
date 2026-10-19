/** Payload-free ownership facts. Terminal nodes leave this graph; native task receipts remain. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Record from './Record.js'
import * as Sequence from './Sequence.js'
import * as Observation from './Observation.js'
import { ObservationError } from './ObservationError.js'
import type { HarnessService } from './Harness.js'
import * as DocumentValue from './internal/DocumentValue.js'

export const OutcomeStatusSchema = Schema.Literals([
  'completed',
  'failed',
  'aborted',
  'orphaned',
  'faulted',
])
export const NodeSchema = Schema.Struct({
  id: Record.TaskId,
  conversationId: Record.ConversationId,
  kind: Schema.NonEmptyString,
  version: Observation.CounterSchema,
  owner: Schema.optionalKey(Record.TaskId),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
  status: Schema.Literals(['pending', 'running', 'waiting', 'completing']),
  phase: Schema.optionalKey(Schema.String),
  on: Schema.Array(Record.TaskId),
  policy: Schema.optionalKey(Schema.Literals(['allSettled', 'failFast'])),
  heldOutcomeStatus: Schema.optionalKey(OutcomeStatusSchema),
  ownedConversations: Schema.Array(Record.ConversationId),
})
export type Node = typeof NodeSchema.Type
export const ValueSchema = Schema.Struct({
  tasks: Schema.Array(NodeSchema),
  conversations: Schema.Array(Record.Conversation),
})
export type Value = typeof ValueSchema.Type
export const TargetSchema = Schema.TaggedStruct('sessionGraph', {})
const fields = {
  protocol: Schema.Literal(1),
  target: TargetSchema,
  revision: Observation.RevisionSchema,
  commitSeq: Schema.optionalKey(Sequence.SequenceSchema),
  value: ValueSchema,
}
export const SnapshotSchema = Schema.TaggedStruct('Snapshot', fields)
export type Snapshot = typeof SnapshotSchema.Type
export const OperationSchema = Schema.TaggedStruct('Replace', { value: ValueSchema })
export const FrameSchema = Schema.Union([
  SnapshotSchema,
  Schema.TaggedStruct('Rebase', fields),
  Schema.TaggedStruct('Delta', {
    protocol: Schema.Literal(1),
    target: TargetSchema,
    epoch: Schema.NonEmptyString,
    from: Observation.CounterSchema,
    to: Observation.CounterSchema,
    commitSeq: Schema.optionalKey(Sequence.SequenceSchema),
    ops: Schema.Tuple([OperationSchema]),
  }),
])
export type Frame = typeof FrameSchema.Type
const invalid = (cause: Schema.SchemaError) =>
  new ObservationError({
    reason: 'invalid',
    operation: 'graph.reduce',
    message: cause.message,
    cause,
  })
export const reduce = Effect.fnUntraced(function* (
  current: Snapshot,
  frame: Frame,
): Effect.fn.Return<Snapshot, ObservationError> {
  const self = yield* DocumentValue.copy(SnapshotSchema, current).pipe(Effect.mapError(invalid))
  const next = yield* DocumentValue.copy(FrameSchema, frame).pipe(Effect.mapError(invalid))
  const revision = yield* Observation.check(self, next)
  return {
    _tag: 'Snapshot',
    protocol: 1,
    target: self.target,
    revision,
    ...(next.commitSeq === undefined ? {} : { commitSeq: next.commitSeq }),
    value: next._tag === 'Delta' ? next.ops[0].value : next.value,
  }
})
export const snapshot = (self: HarnessService): Effect.Effect<Snapshot, Observation.Failure> =>
  Effect.suspend(
    () => self.observations?.graphSnapshot?.() ?? Observation.unsupported('graph.snapshot'),
  )
export const watch = (
  self: HarnessService,
  options?: Observation.WatchOptions,
): Stream.Stream<Frame, Observation.Failure> =>
  Stream.unwrap(
    Effect.suspend(() =>
      self.observations?.watchGraph === undefined
        ? Observation.unsupported('graph.watch')
        : Effect.succeed(self.observations.watchGraph(options)),
    ),
  )
