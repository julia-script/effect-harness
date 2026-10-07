import * as Time from '@effect-harness/harness/Time'
import * as DateTime from 'effect/DateTime'
// Queue boundary rules adapted from pi-durable (MIT), pinned 636703a0.
import * as Agent from '@effect-harness/harness/Agent'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Usage from '@effect-harness/harness/Usage'
import * as Effect from 'effect/Effect'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Document from './Document.ts'
import * as Record from './Record.ts'
import type * as Session from './Session.ts'
import { EntryDraft } from './workflow/Submission.ts'

export const Item = Schema.Union([
  Schema.Struct({
    id: Record.SubmissionId,
    mode: Schema.Literals(['steer', 'followUp']),
    message: Schema.toEncoded(Schema.toCodecJson(Prompt.UserMessage)),
  }),
  Schema.Struct({ id: Record.SubmissionId, mode: Schema.Literal('write'), entry: EntryDraft }),
])
export type Item = typeof Item.Type
export const State = Schema.Struct({ items: Schema.Array(Item) })
export const InboxDoc = Document.defineUnsafe({
  kind: 'harness.inbox',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: State,
  initial: (): typeof State.Type => ({ items: [] }),
  checkpointWhen: (value) => value.items.length === 0,
})

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
/** Decoded deadlines for read/event adapters; LiveDoc and its Proxy drafts retain numeric JSON. */
export const LiveDomain = LiveState.mapFields((fields) => ({
  ...fields,
  generation: Schema.optionalKey(
    Schema.Struct({
      ...fields.generation.schema.fields,
      retry: Schema.optionalKey(Schema.Struct({ at: Time.EpochMillis, error: Schema.String })),
      deferred: Schema.optionalKey(Schema.Struct({ pollAt: Time.EpochMillis })),
    }),
  ),
  compactions: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        ...fields.compactions.schema.value.fields,
        retry: Schema.optionalKey(Schema.Struct({ at: Time.EpochMillis, error: Schema.String })),
      }),
    ),
  ),
}))
/** Preserve opaque mounted references while adapting only the known numeric time leaves. */
export const domain = (value: LiveState): typeof LiveDomain.Type => {
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
export type LiveState = typeof LiveState.Type
export const LiveDoc = Document.defineUnsafe({
  kind: 'harness.live',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Document.jsonObjectCodec(LiveState),
  initial: (): LiveState => ({}),
  checkpointWhen: (value) =>
    value.generation === undefined &&
    !(value.tools ?? []).some((slot) => slot.status === 'running'),
})

export interface Boundary {
  readonly conversationId: Record.ConversationId
  readonly inbox: Document.Draft<typeof State.Type>
  readonly modes: Pick<Agent.Settings, 'steeringMode' | 'followUpMode'>
  head: Record.EntryId | undefined
}

/** Read table state before the commit's first table write. Queue modes are supplied at this deciding commit. */
export const prepare = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
  modes: Boundary['modes'],
) {
  const head = (yield* tx.latestHeadMarker(conversationId))?.head
  const inbox = yield* tx.doc(InboxDoc, { owner: conversationId })
  return { conversationId, inbox, modes, head } satisfies Boundary
})

/** Place all writes first, then selected inputs; a reset also admits follow-ups at a post-tools boundary. */
export const apply = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  boundary: Boundary,
  at: 'postTools' | 'final',
  now: DateTime.Utc,
) {
  const items = boundary.inbox.items
  const reset = items.some((item) => item.mode === 'write' && item.entry.head === 'self')
  const final = at === 'final' || reset
  const pick = (mode: 'steer' | 'followUp', queueMode: 'one-at-a-time' | 'all') => {
    const indexes = items.flatMap((item, index) => (item.mode === mode ? [index] : []))
    return queueMode === 'all' ? indexes : indexes.slice(0, 1)
  }
  const writes = items.flatMap((item, index) => (item.mode === 'write' ? [index] : []))
  const users = [
    ...pick('steer', boundary.modes.steeringMode),
    ...(final ? pick('followUp', boundary.modes.followUpMode) : []),
  ].sort((a, b) => a - b)
  const settled: Record.SubmissionId[] = []
  for (const index of writes) {
    const item = items[index]
    if (item?.mode !== 'write') continue
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
  const placed: Record.SubmissionId[] = []
  for (const index of users) {
    const item = items[index]
    if (item === undefined || item.mode === 'write') continue
    const entry = yield* tx.appendEntry(boundary.conversationId, {
      kind: 'harness.user',
      model: [item.message],
      data: { timestamp: DateTime.toEpochMillis(now) },
    })
    yield* tx.placeSubmission(item.id, entry.id)
    placed.push(item.id)
  }
  for (const index of [...writes, ...users].sort((a, b) => b - a)) items.splice(index, 1)
  return { users: placed, settled, reset }
})

export const withdraw = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  conversationId: Record.ConversationId,
) {
  const inbox = yield* tx.doc(InboxDoc, { owner: conversationId })
  const settled: Record.SubmissionId[] = []
  for (let index = inbox.items.length - 1; index >= 0; index--) {
    const item = inbox.items[index]
    if (item === undefined || item.mode === 'write') continue
    yield* tx.settleSubmission(item.id, { status: 'unanswered', reason: 'aborted' })
    settled.push(item.id)
    inbox.items.splice(index, 1)
  }
  return settled.sort((a, b) => a - b)
})

/** Settle exactly the inputs owned by this run; an earlier generation cannot end its successor's run. */
export const endRun = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  live: Document.Draft<LiveState>,
  taskId: Record.TaskId,
  settlement: Parameters<Session.Transaction['settleSubmission']>[1],
) {
  const settled: Record.SubmissionId[] = []
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
