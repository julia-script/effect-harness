/**
 * Admission of submissions (spec §6), in one commit.
 */
import { Effect } from 'effect'
import { cast } from 'effect/Function'
import type { Draft as DocumentDraft } from '../../delta/Tracker.ts'
import type { Tx } from '../../session/Session.ts'
import type { ConversationId, SubmissionId } from '../../storage/schema/Ids.ts'
import { ConversationBusy, InvalidSubmission } from '../../protocol/HarnessError.ts'
import * as Inbox from '../Inbox.ts'
import * as Live from '../Live.ts'
import type { Settings } from '../Settings.ts'
import type { Draft, InputDraft, WriteDraft } from '../../protocol/Submission.ts'
import {
  appendInput,
  applyBoundary,
  hasQueuedReset,
  isDisplayOnly,
  isStale,
  pickInputEntry,
  prepareBoundary,
} from './inbox.ts'
import type { Boundary } from './inbox.ts'
import { strictJson } from '../../internal/strictJson.ts'
import { startRun } from './run.ts'

/** An input entry's kind must be a non-empty string outside the `$` prefix. */
const checkInputEntry = (draft: Draft) => {
  if (draft.type !== 'input' || draft.entry === undefined) return Effect.void
  const { kind } = draft.entry
  return typeof kind === 'string' && kind.length > 0 && !kind.startsWith('$')
    ? Effect.void
    : Effect.fail(
        new InvalidSubmission({
          message: `An input entry's kind must be a non-empty string not starting with "$", got ${kind}`,
        }),
      )
}

/** The submission a request ID already names, when it has the draft's type. */
const existing = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  draft: Draft,
) {
  const found =
    draft.requestId === undefined
      ? draft.requestId
      : yield* tx.submissionByRequest(conversationId, draft.requestId)
  if (found !== undefined && found.type !== draft.type) {
    return yield* new InvalidSubmission({
      message: `Request ${draft.requestId} already identifies a submission of type ${found.type}`,
    })
  }
  return found?.id
})

const requestIdOf = (draft: Draft) =>
  draft.requestId === undefined ? {} : { requestId: draft.requestId }

/** Append a write and settle it `done`, or settle it `stale` when its head is cut. */
const placeWrite = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  draft: WriteDraft,
  boundary?: Boundary,
) {
  const requestId = requestIdOf(draft)
  if (boundary !== undefined && isStale(boundary, draft.entry)) {
    const stale = { status: 'unanswered', reason: 'stale' } as const
    const record = yield* tx.createSubmission({
      conversationId,
      ...requestId,
      type: 'write',
      ...stale,
    })
    return record.id
  }
  const entry = yield* tx.appendEntry(conversationId, strictJson(draft.entry))
  const done = { status: 'done', entry: entry.id } as const
  const record = yield* tx.createSubmission({
    conversationId,
    ...requestId,
    type: 'write',
    ...done,
  })
  return record.id
})

/** Queue a submission in `$inbox` behind the items already there. */
const enqueue = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  draft: Draft,
  inbox: DocumentDraft<Inbox.State>,
) {
  const queued = { status: 'queued', ...requestIdOf(draft) } as const
  const { id } = yield* tx.createSubmission({ conversationId, ...queued, type: draft.type })
  inbox.items.push(cast(item(id, draft)))
  return id
})

const item = (id: SubmissionId, draft: Draft): Inbox.Item => {
  if (draft.type === 'write') return { id, mode: 'write', entry: strictJson(draft.entry) }
  const mode = draft.whenBusy === 'steer' ? 'steer' : 'followUp'
  const content = strictJson(draft.content)
  return draft.entry === undefined
    ? { id, mode, content }
    : { id, mode, content, entry: pickInputEntry(draft.entry) }
}

/** Place idle input as its entry and start a run for it. */
const placeInput = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  draft: InputDraft,
) {
  const live = yield* tx.doc(Live.document, conversationId)
  const entry = yield* appendInput(tx, conversationId, draft.content, draft.entry)
  const placed = { status: 'placed', entry: entry.id } as const
  const record = yield* tx.createSubmission({
    conversationId,
    ...requestIdOf(draft),
    type: 'input',
    ...placed,
  })
  yield* startRun(tx, conversationId, live, [record.id])
  return record.id
})

/**
 * Admit a submission inside a commit. A known request ID returns its submission without writing.
 * A display-only write is appended and settled `done` at once, busy or not, unless a reset is
 * queued. A busy conversation queues anything else, or rejects `whenBusy: 'reject'` input. An
 * idle conversation with queued items queues it behind them and runs a final boundary. Otherwise
 * idle input is placed and starts a run, and an idle write is appended, or settles `stale`.
 */
export const admit = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  draft: Draft,
  settings: Settings,
) {
  yield* checkInputEntry(draft)
  const known = yield* existing(tx, conversationId, draft)
  if (known !== undefined) return known
  const inbox = yield* tx.doc(Inbox.document, conversationId)
  if (draft.type === 'write' && isDisplayOnly(draft.entry) && !hasQueuedReset(inbox.items)) {
    return yield* placeWrite(tx, conversationId, draft)
  }
  const live = yield* tx.doc(Live.document, conversationId)
  if (live.run !== undefined) {
    if (draft.type === 'input' && draft.whenBusy === 'reject') {
      const message = `Conversation ${conversationId} is busy`
      return yield* new ConversationBusy({ message, conversationId })
    }
    return yield* enqueue(tx, conversationId, draft, inbox)
  }
  // A boundary reads the table, so it is prepared before the first table write.
  const boundary = yield* prepareBoundary(tx, conversationId, settings)
  if (boundary.inbox.items.length > 0) {
    const id = yield* enqueue(tx, conversationId, draft, boundary.inbox)
    const { users } = yield* applyBoundary(tx, boundary, 'final')
    if (users.length > 0) yield* startRun(tx, conversationId, live, users)
    return id
  }
  return draft.type === 'write'
    ? yield* placeWrite(tx, conversationId, draft, boundary)
    : yield* placeInput(tx, conversationId, draft)
})
