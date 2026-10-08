import * as Serialization from './Serialization.ts'
/**
 * Persisted inbox documents and atomic message admission.
 */
import * as Result from 'effect/Result'
import * as Order from 'effect/Order'
import * as Arr from 'effect/Array'
import type { StorageError } from './StorageError.ts'
import * as Option from 'effect/Option'
import * as Time from 'effect-harness/Time'
import * as DateTime from 'effect/DateTime'
// Queue boundary rules adapted from pi-durable (MIT), pinned 636703a0.
import * as Agent from 'effect-harness/Agent'
import * as Invocation from 'effect-harness/Invocation'
import * as Usage from 'effect-harness/Usage'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Document from './Document.ts'
import * as Record from './Record.ts'
import type * as Session from './Session.ts'
import { EntryDraft } from './workflow/Submission.ts'

/**
 * Schema for queued input or passive write with its admission policy.
 *
 * @category schemas
 */
export const Item = Schema.Union([
  Schema.TaggedStruct('input', {
    id: Record.SubmissionId,
    mode: Schema.Literals(['steer', 'followUp']),
    message: Schema.toEncoded(Schema.toCodecJson(Prompt.UserMessage)),
  }),
  Schema.TaggedStruct('write', { id: Record.SubmissionId, entry: EntryDraft }),
])
/**
 * Queued input or passive write with its admission policy.
 *
 * @category models
 */
export type Item = typeof Item.Type
/**
 * Schema for ordered submission queues for a conversation.
 *
 * @category schemas
 */
export const State = Schema.Struct({ items: Schema.Array(Item) })
/**
 * Ordered submission queues for a conversation.
 *
 * @category models
 */
export type State = typeof State.Type

/**
 * Queued submission document definition.
 *
 * @category models
 */
export const InboxDoc = Document.defineUnsafe({
  kind: 'harness.inbox',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Serialization.object(State),
  initial: (): State => ({ items: [] }),
  checkpointWhen: (value) => Arr.isReadonlyArrayEmpty(value.items),
})

/**
 * Schema for tool intent and terminal receipt tracked within a generation boundary.
 *
 * @category schemas
 */
export const ToolSlot = Schema.Struct({
  callId: Schema.String,
  name: Schema.String,
  taskId: Schema.optionalKey(Record.TaskId),
  status: Schema.Literals(['pending', 'running', 'done']),
  output: Schema.optionalKey(Schema.String),
  droppedBytes: Schema.optionalKey(Schema.Finite),
  droppedLines: Schema.optionalKey(Schema.Finite),
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.optionalKey(Schema.Array(Invocation.Diagnostic)),
  entry: Schema.optionalKey(Record.EntryId),
})
/**
 * Tool intent and terminal receipt tracked within a generation boundary.
 *
 * @category models
 */
export type ToolSlot = typeof ToolSlot.Type

/**
 * Schema for committed run, generation and tool state for a conversation.
 *
 * @category schemas
 */
export const LiveState = Schema.Struct({
  run: Schema.optionalKey(
    Schema.Struct({ taskId: Record.TaskId, inputs: Schema.Array(Record.SubmissionId) }),
  ),
  generation: Schema.optionalKey(
    Schema.Struct({
      attempt: Schema.Int,
      model: Schema.optionalKey(Agent.ModelRef),
      usage: Schema.optionalKey(Usage.Usage),
      message: Schema.optionalKey(Schema.toEncoded(Schema.toCodecJson(Prompt.AssistantMessage))),
      retry: Schema.optionalKey(Schema.Struct({ at: Schema.Finite, error: Schema.String })),
      deferred: Schema.optionalKey(Schema.Struct({ pollAt: Schema.Finite })),
    }),
  ),
  tools: Schema.optionalKey(Schema.Array(ToolSlot)),
  compactions: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        taskId: Record.TaskId,
        reason: Schema.Literals(['manual', 'threshold', 'overflow', 'background']),
        blocking: Schema.Boolean,
        attempt: Schema.Int,
        retry: Schema.optionalKey(Schema.Struct({ at: Schema.Finite, error: Schema.String })),
      }),
    ),
  ),
})
/**
 * Decoded deadlines for read/event adapters; LiveDoc and its Proxy drafts retain numeric JSON.
 *
 * @category schemas
 */
export const LiveDomain = LiveState.mapFields((fields) => ({
  ...fields,
  generation: Schema.optionalKey(
    Schema.Struct({
      ...fields.generation.schema.fields,
      retry: Schema.optionalKey(
        Schema.Struct({ at: Time.DateTimeUtcFromEpochMillis, error: Schema.String }),
      ),
      deferred: Schema.optionalKey(Schema.Struct({ pollAt: Time.DateTimeUtcFromEpochMillis })),
    }),
  ),
  compactions: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        ...fields.compactions.schema.value.fields,
        retry: Schema.optionalKey(
          Schema.Struct({ at: Time.DateTimeUtcFromEpochMillis, error: Schema.String }),
        ),
      }),
    ),
  ),
}))
/**
 * Preserves opaque mounted references while adapting only the known numeric time leaves.
 *
 * @category combinators
 */
export const domain = (value: LiveState): LiveDomain => {
  const { generation, compactions, ...rest } = value
  const generationDomain =
    generation === undefined
      ? undefined
      : (() => {
          const { retry, deferred, ...rest } = generation
          return {
            ...rest,
            ...(retry === undefined
              ? {}
              : { retry: { ...retry, at: Time.fromEpochMillis(retry.at) } }),
            ...(deferred === undefined
              ? {}
              : { deferred: { pollAt: Time.fromEpochMillis(deferred.pollAt) } }),
          }
        })()
  return {
    ...rest,
    ...(generationDomain === undefined ? {} : { generation: generationDomain }),
    ...(compactions === undefined
      ? {}
      : {
          compactions: compactions.map((item) => {
            const { retry, ...rest } = item
            return {
              ...rest,
              ...(retry === undefined
                ? {}
                : { retry: { ...retry, at: Time.fromEpochMillis(retry.at) } }),
            }
          }),
        }),
  }
}
/**
 * Committed run, generation and tool state for a conversation.
 *
 * @category models
 */
export type LiveState = typeof LiveState.Type
/**
 * Committed generation and tool progress document definition.
 *
 * @category models
 */
export const LiveDoc = Document.defineUnsafe({
  kind: 'harness.live',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Serialization.object(LiveState),
  initial: (): LiveState => ({}),
  checkpointWhen: (value) =>
    value.generation === undefined &&
    !(value.tools ?? []).some((slot) => slot.status === 'running'),
})

/**
 * Admission boundary from which pending inputs and writes are applied.
 *
 * @category models
 */
export interface Boundary {
  readonly conversationId: Record.ConversationId
  readonly inbox: Document.Document.Draft<State>
  readonly modes: Pick<Agent.Settings, 'steeringMode' | 'followUpMode'>
  head: Record.EntryId | undefined
}

/**
 * Reads table state before the commit's first table write.
 *
 * **Details**
 *
 * Queue modes are supplied at this deciding commit.
 *
 * @category combinators
 */
export const prepare = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
  modes: Boundary['modes'],
): Effect.fn.Return<Boundary, StorageError> {
  // Boundary.head is an optional persisted/native draft field; absence is unwrapped only at this DTO boundary.
  const head = (yield* tx.latestHeadMarker(conversationId)).pipe(
    Option.flatMap((entry) => Option.fromUndefinedOr(entry.head)),
    Option.getOrUndefined,
  )
  const inbox = yield* tx.doc(InboxDoc, { owner: conversationId })
  return { conversationId, inbox, modes, head } satisfies Boundary
})

/**
 * Validates and replays view operations while preserving unchanged branches.
 *
 * @category combinators
 */
export const apply = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  boundary: Boundary,
  at: 'postTools' | 'final',
  now: DateTime.Utc,
): Effect.fn.Return<
  { users: Array<Record.SubmissionId>; settled: Array<Record.SubmissionId>; reset: boolean },
  StorageError
> {
  const items = boundary.inbox.items
  const reset = items.some((item) => item._tag === 'write' && item.entry.head === 'self')
  const final = at === 'final' || reset
  const pick = (mode: 'steer' | 'followUp', queueMode: 'one-at-a-time' | 'all') => {
    const indexes = Arr.filterMap(items, (item, index) =>
      item._tag === 'input' && item.mode === mode ? Result.succeed(index) : Result.failVoid,
    )
    return queueMode === 'all' ? indexes : indexes.slice(0, 1)
  }
  const writes = Arr.filterMap(items, (item, index) =>
    item._tag === 'write' ? Result.succeed(index) : Result.failVoid,
  )
  const users = Arr.sort(
    [
      ...pick('steer', boundary.modes.steeringMode),
      ...(final ? pick('followUp', boundary.modes.followUpMode) : []),
    ],
    Order.Number,
  )
  const settled: Array<Record.SubmissionId> = []
  for (const index of writes) {
    const item = items[index]
    if (item?._tag !== 'write') continue
    const entry = item.entry
    if (
      typeof entry.head === 'number' &&
      boundary.head !== undefined &&
      entry.head < boundary.head
    ) {
      yield* tx.settleSubmission(item.id, { status: 'unanswered', reason: 'stale' })
    } else {
      const placed = yield* tx.appendEntry(boundary.conversationId, entry)
      if (entry.head !== undefined) boundary.head = entry.head === 'self' ? placed.id : entry.head
      yield* tx.placeSubmission(item.id, placed.id)
    }
    settled.push(item.id)
  }
  const placed: Array<Record.SubmissionId> = []
  for (const index of users) {
    const item = items[index]
    if (item === undefined || item._tag === 'write') continue
    const entry = yield* tx.appendEntry(boundary.conversationId, {
      kind: 'harness.user',
      model: [item.message],
      data: { timestamp: DateTime.toEpochMillis(now) },
    })
    yield* tx.placeSubmission(item.id, entry.id)
    placed.push(item.id)
  }
  for (const index of Arr.sort([...writes, ...users], Order.flip(Order.Number)))
    items.splice(index, 1)
  return { users: placed, settled, reset }
})

/**
 * Withdraws queued submissions from an inbox boundary.
 *
 * @category combinators
 */
export const withdraw = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
): Effect.fn.Return<Array<Record.SubmissionId>, StorageError> {
  const inbox = yield* tx.doc(InboxDoc, { owner: conversationId })
  const settled: Array<Record.SubmissionId> = []
  for (let index = inbox.items.length - 1; index >= 0; index--) {
    const item = inbox.items[index]
    if (item === undefined || item._tag === 'write') continue
    yield* tx.settleSubmission(item.id, { status: 'unanswered', reason: 'aborted' })
    settled.push(item.id)
    inbox.items.splice(index, 1)
  }
  return Arr.sort(settled, Order.Number)
})

/**
 * Settles exactly the inputs owned by this run; an earlier generation cannot end its successor's run.
 *
 * @category combinators
 */
export const endRun = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  live: Document.Document.Draft<LiveState>,
  taskId: Record.TaskId,
  settlement: Parameters<Session.Transaction['settleSubmission']>[1],
): Effect.fn.Return<Array<Record.SubmissionId>, StorageError> {
  const settled: Array<Record.SubmissionId> = []
  if (live.run?.taskId === taskId) {
    for (const id of live.run.inputs) {
      yield* tx.settleSubmission(id, settlement)
      settled.push(id)
    }
    delete live.run
    delete live.generation
    delete live.tools
  }
  return settled
})

/** Decoded value of the LiveDomain schema.
 * @category models
 */
export type LiveDomain = typeof LiveDomain.Type

/** Checks the decoded Item contract without decoding or coercing input.
 * @category guards
 */
export const isItem: (u: unknown) => u is Item = Schema.is(Schema.toType(Item))

/** Checks the decoded State contract without decoding or coercing input.
 * @category guards
 */
export const isState: (u: unknown) => u is State = Schema.is(Schema.toType(State))

/** Checks the decoded ToolSlot contract without decoding or coercing input.
 * @category guards
 */
export const isToolSlot: (u: unknown) => u is ToolSlot = Schema.is(Schema.toType(ToolSlot))

/** Checks the decoded LiveState contract without decoding or coercing input.
 * @category guards
 */
export const isLiveState: (u: unknown) => u is LiveState = Schema.is(Schema.toType(LiveState))

/** Checks the decoded LiveDomain contract without decoding or coercing input.
 * @category guards
 */
export const isLiveDomain: (u: unknown) => u is LiveDomain = Schema.is(Schema.toType(LiveDomain))
