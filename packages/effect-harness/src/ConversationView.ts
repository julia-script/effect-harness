/** A fixed read-only projection of committed built-ins; acquisition never resumes work. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import * as Record from './Record.js'
import * as Sequence from './Sequence.js'
import * as Observation from './Observation.js'
import * as Graph from './TaskGraph.js'
import * as Projection from './ContextProjection.js'
import * as Agent from './Agent.js'
import * as Live from './LiveState.js'
import * as Progress from './ToolProgress.js'
import * as Compaction from './CompactionStatus.js'
import * as Run from './RunControl.js'
import * as Settings from './Settings.js'
import * as Prepared from './PreparedRequest.js'
import * as Invocation from './ToolInvocation.js'
import * as ToolResult from './ToolResult.js'
import * as ToolControl from './ToolControl.js'
import * as Subagent from './Subagent.js'
import * as Usage from './Usage.js'
import * as Conversation from './Conversation.js'
import { ObservationError } from './ObservationError.js'
import * as DocumentValue from './internal/DocumentValue.js'

const coverage = Schema.Struct({ known: Schema.Finite, unknown: Schema.Finite })
export const UsageSchema = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      model: Schema.optionalKey(Schema.Struct({ provider: Schema.String, modelId: Schema.String })),
      responses: Schema.Finite,
      tokens: Usage.TokensSchema,
      tokenCoverage: Schema.optionalKey(Schema.Record(Schema.String, coverage)),
    }),
  ),
  tools: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      results: Schema.Finite,
      tokens: Schema.optionalKey(Usage.TokensSchema),
      tokenCoverage: Schema.optionalKey(Schema.Record(Schema.String, coverage)),
    }),
  ),
  costs: Schema.Array(
    Schema.Struct({
      currency: Schema.String,
      model: Schema.Finite,
      tool: Schema.Finite,
      modelComponents: Schema.optionalKey(Usage.ComponentsSchema),
      toolComponents: Schema.optionalKey(Usage.ComponentsSchema),
    }),
  ),
  unpricedModels: Schema.Finite,
  unpricedTools: Schema.Finite,
  legacyRecords: Schema.Finite,
  unknownAttempts: Schema.optionalKey(Schema.Finite),
})
export const TerminalSchema = Schema.Struct({
  taskId: Record.TaskId,
  status: Graph.OutcomeStatusSchema,
  reason: Schema.optionalKey(Schema.String),
  error: Schema.optionalKey(Schema.Struct({ message: Schema.String })),
})
export const SelectionSchema = Schema.Struct({
  agent: Agent.StateSchema,
  settings: Settings.SnapshotSchema,
  modelBinding: Prepared.BindingSchema,
  configurationBinding: Prepared.BindingSchema,
  registryGeneration: Observation.CounterSchema,
})
export const GenerationSchema = Schema.Struct({
  taskId: Record.TaskId,
  phase: Schema.Literals([
    'queued',
    'request',
    'compaction',
    'deferred-start',
    'deferred',
    'streaming',
    'retry',
    'tools',
    'toolRound',
  ]),
  requestId: Schema.optionalKey(Live.GenerationSchema.fields.requestId),
  attempt: Schema.optionalKey(Usage.AttemptId),
  ordinal: Schema.optionalKey(Observation.CounterSchema),
  until: Schema.optionalKey(Observation.CounterSchema),
  pollAt: Schema.optionalKey(Observation.CounterSchema),
  expiresAt: Schema.optionalKey(Observation.CounterSchema),
  child: Schema.optionalKey(Record.TaskId),
  roundKey: Schema.optionalKey(Schema.String),
  selection: Schema.optionalKey(SelectionSchema),
})
export const RoundSlotSchema = Schema.Struct({
  callId: Schema.String,
  name: Schema.String,
  taskId: Schema.optionalKey(Record.TaskId),
  entryId: Schema.optionalKey(Record.EntryId),
  status: Schema.optionalKey(ToolControl.ReceiptSchema.fields.status),
  result: Schema.optionalKey(Schema.toCodecJson(ToolResult.NestedSchema)),
})
export const RoundSchema = Schema.Struct({
  key: Schema.String,
  ownerTaskId: Record.TaskId,
  assistant: Record.EntryId,
  mode: Schema.Literals(['parallel', 'sequential']),
  applied: Schema.Boolean,
  slots: Schema.Array(RoundSlotSchema),
})
/** Original private legacyValue is deliberately not a new conversation result channel. */
export const ReceiptSchema = Schema.toCodecJson(
  Invocation.WireReceiptSchema.mapFields(Struct.omit(['legacyValue'])),
)
export const NestedSchema = Schema.Struct({
  taskId: Record.TaskId,
  parentTaskId: Record.TaskId,
  key: Schema.String,
  callId: Usage.NestedCallId,
  name: Schema.String,
  availability: Schema.Literals(['pending', 'available', 'unavailable']),
  receipts: Schema.Array(ReceiptSchema),
})
export const SubagentSchema = Schema.Struct({
  handle: Subagent.HandleSchema,
  status: Record.TaskState.fields.status,
  attachment: Schema.optionalKey(Subagent.AttachmentSchema),
})
const mounted = { record: Record.DocumentRecordSchema, version: Schema.Int }
export const DocumentSchema = Schema.Union([
  Schema.TaggedStruct('Agent', { ...mounted, value: Agent.StateSchema }),
  Schema.TaggedStruct('Live', { ...mounted, value: Live.SnapshotSchema }),
  Schema.TaggedStruct('Progress', { ...mounted, value: Progress.Snapshot }),
  Schema.TaggedStruct('Compaction', { ...mounted, value: Compaction.SnapshotSchema }),
  Schema.TaggedStruct('Run', { ...mounted, value: Run.StateSchema }),
  Schema.TaggedStruct('Inbox', { ...mounted, value: Run.InboxSchema }),
])
export const ValueSchema = Schema.Struct({
  conversation: Record.Conversation,
  context: Schema.toCodecJson(Projection.ContextViewSchema),
  entries: Schema.Array(Record.Entry),
  graph: Graph.ValueSchema,
  terminal: Schema.Array(TerminalSchema),
  submissions: Schema.Array(Record.Submission),
  inbox: Run.InboxSchema,
  run: Run.StateSchema,
  agent: Agent.StateSchema,
  live: Live.SnapshotSchema,
  generations: Schema.Array(GenerationSchema),
  rounds: Schema.Array(RoundSchema),
  progress: Schema.Array(Schema.Struct({ key: Schema.String, value: Progress.Snapshot })),
  compactions: Compaction.SnapshotSchema,
  nested: Schema.Array(NestedSchema),
  subagents: Schema.Array(SubagentSchema),
  usage: UsageSchema,
  documents: Schema.Array(DocumentSchema),
})
export type Value = typeof ValueSchema.Type
export const TargetSchema = Schema.TaggedStruct('conversation', {
  conversationId: Record.ConversationId,
})
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
    operation: 'view.reduce',
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
export const snapshot = (
  self: Conversation.Conversation,
): Effect.Effect<Snapshot, Observation.Failure> => Conversation.view(self)
export const watch = (
  self: Conversation.Conversation,
  options?: Observation.WatchOptions,
): Stream.Stream<Frame, Observation.Failure> => Conversation.watchView(self, options)
/** Bound callbacks stay joined to the invocation lease, including asynchronous enrichment. */
export const watchWith = <A, E, R>(
  self: Conversation.Conversation,
  callback: (frame: Frame) => Effect.Effect<A, E, R>,
  options?: Observation.WatchOptions,
): Stream.Stream<A, E | Observation.Failure, R> =>
  Conversation.watchViewWith(self, callback, options)
