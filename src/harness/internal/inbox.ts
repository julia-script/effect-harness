/**
 * Boundaries of `pi.inbox` (spec §6): which queued items a run places, and where.
 */
import { Clock, Effect } from 'effect'
import type { Draft } from '../../delta/Tracker.ts'
import type { EntryDraft, Tx } from '../../session/Session.ts'
import type { EntryRecord } from '../../storage/schema/Entry.ts'
import type { ConversationId, EntryId, SubmissionId } from '../../storage/schema/Ids.ts'
import * as Inbox from '../Inbox.ts'
import type { QueueMode, Settings } from '../Settings.ts'
import type { InputEntry, UserInput } from '../Submission.ts'
import { strictJson } from './json.ts'

export const USER_KIND = 'pi.user'
export const RESET_KIND = 'pi.reset'

/** What a boundary reads before the commit's first table write, and the newest head it saw. */
export interface Boundary {
  readonly conversationId: ConversationId
  readonly inbox: Draft<Inbox.State>
  readonly steeringMode: QueueMode
  readonly followUpMode: QueueMode
  /** Start of the active range: the newest head marker's `head`, advanced by heads placed here. */
  head: EntryId | undefined
}

/** Selected user items in ID order, and whether a reset (`head: 'self'` write) was placed. */
export interface BoundaryResult {
  readonly users: ReadonlyArray<SubmissionId>
  readonly reset: boolean
}

/**
 * Read what a boundary needs. Table reads must precede the commit's first table write, so a
 * caller prepares the boundary at the start of its commit.
 */
export const prepareBoundary = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  modes: Settings,
) {
  const head = (yield* tx.latestHeadMarker(conversationId))?.head
  const inbox = yield* tx.doc(Inbox.document, conversationId)
  const boundary: Boundary = {
    conversationId,
    inbox,
    steeringMode: modes.steeringMode,
    followUpMode: modes.followUpMode,
    head,
  }
  return boundary
})

/** Whether a reset is queued: entries appended before it leave the active range when it lands. */
export const hasQueuedReset = (items: ReadonlyArray<Inbox.Item>): boolean =>
  items.some((item) => item.mode === 'write' && item.entry.head === 'self')

/**
 * Whether a write is display-only: it has no `model`, `head`, or `edits` (present counts, even
 * empty), so it changes nothing the model sees and needs no boundary.
 */
export const isDisplayOnly = (entry: EntryDraft): boolean =>
  entry.model === undefined && entry.head === undefined && entry.edits === undefined

/** Whether a head write targets an entry before the active range, bringing back cut history. */
export const isStale = (boundary: Boundary, entry: EntryDraft): boolean =>
  typeof entry.head === 'number' && boundary.head !== undefined && entry.head < boundary.head

/** The kind and data of an input's entry as strict JSON, without other fields a caller passed. */
export const pickInputEntry = ({ kind, data }: InputEntry): InputEntry =>
  strictJson(data === undefined ? { kind } : { kind, data })

/** Append the entry an input is placed as: `entry`'s kind and data, or `pi.user`. */
export const appendInput = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  content: UserInput,
  entry?: InputEntry,
) {
  const timestamp = yield* Clock.currentTimeMillis
  const model = [{ role: 'user', content: strictJson(content), timestamp }]
  const kind = entry === undefined ? { kind: USER_KIND } : pickInputEntry(entry)
  const appended: EntryRecord = yield* tx.appendEntry(conversationId, { ...kind, model })
  return appended
})

/** Indexes of the items of `mode`: all of them, or the first. */
const pick = (items: ReadonlyArray<Inbox.Item>, mode: Inbox.Item['mode'], queue: QueueMode) => {
  const indexes = items.flatMap((item, index) => (item.mode === mode ? [index] : []))
  return queue === 'all' ? indexes : indexes.slice(0, 1)
}

/**
 * Place the queued items a boundary selects (spec §6): every write, the first or all steers, and
 * at `final` the first or all follow-ups. A selected reset turns a `postTools` boundary into
 * `final`. Writes are placed first and user items after them, each in ID order, so user items
 * queued before a reset run in the new context. A write whose head targets an entry before the
 * active range, also one a reset of this boundary started, settles `stale`. Placed and stale items
 * leave the inbox.
 */
export const applyBoundary = Effect.fnUntraced(function* (
  tx: Tx,
  boundary: Boundary,
  at: 'postTools' | 'final',
) {
  const { conversationId, inbox } = boundary
  const items: ReadonlyArray<Inbox.Item> = inbox.items
  const reset = hasQueuedReset(items)
  const final = at === 'final' || reset
  const writes = items.flatMap((item, index) => (item.mode === 'write' ? [index] : []))
  const users = [
    ...pick(items, 'steer', boundary.steeringMode),
    ...(final ? pick(items, 'followUp', boundary.followUpMode) : []),
  ].toSorted((a, b) => a - b)
  for (const index of writes) {
    const item = items[index]
    if (item?.mode !== 'write') continue
    // A plain copy: the draft's handles read empty once the commit settles.
    const draft = strictJson(item.entry)
    if (isStale(boundary, draft)) {
      yield* tx.settleSubmission(item.id, { status: 'unanswered', reason: 'stale' })
      continue
    }
    const entry = yield* tx.appendEntry(conversationId, draft)
    if (draft.head !== undefined) boundary.head = draft.head === 'self' ? entry.id : draft.head
    yield* tx.placeSubmission(item.id, entry.id)
  }
  const placed: Array<SubmissionId> = []
  for (const index of users) {
    const item = items[index]
    if (item === undefined || item.mode === 'write') continue
    const entry = yield* appendInput(tx, conversationId, item.content, item.entry)
    yield* tx.placeSubmission(item.id, entry.id)
    placed.push(item.id)
  }
  // From the last index down, so each removal leaves the earlier indexes in place.
  for (const index of [...writes, ...users].toSorted((a, b) => b - a)) inbox.items.splice(index, 1)
  const result: BoundaryResult = { users: placed, reset }
  return result
})

/** Remove a withdrawn submission's item; the caller settles the submission. */
export const removeItem = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  id: SubmissionId,
) {
  const items = (yield* tx.doc(Inbox.document, conversationId)).items
  const index = items.findIndex((item) => item.id === id)
  if (index >= 0) items.splice(index, 1)
})

/**
 * Withdraw every queued input of a conversation, as conversation aborts and abort cascades do:
 * each settles `unanswered` with reason `aborted` and leaves the inbox. Queued writes stay.
 */
export const withdrawQueuedInputs = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
) {
  const items = (yield* tx.doc(Inbox.document, conversationId)).items
  for (let index = items.length - 1; index >= 0; index--) {
    const item = items[index]
    if (item === undefined || item.mode === 'write') continue
    yield* tx.settleSubmission(item.id, { status: 'unanswered', reason: 'aborted' })
    items.splice(index, 1)
  }
})
