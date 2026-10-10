/** Inactive scheduler foundation. All helpers run inside the existing Session commit line. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import { pipeArguments } from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import * as Identity from '../Identity.js'
import type * as Record from '../Record.js'
import * as RunControl from '../RunControl.js'
import { SessionError } from '../SessionError.js'
import * as Transaction from '../Transaction.js'
import * as Runtime from './TransactionRuntime.js'
import * as Value from './DocumentValue.js'

export const ModeSchema = RunControl.ModeSchema
export const ItemSchema = RunControl.InboxItemSchema
export type Item = RunControl.InboxItem
export const StateSchema = RunControl.InboxSchema
export const document = RunControl.inbox
export const target = RunControl.target

export const DraftSchema = Schema.Struct({
  mode: ModeSchema,
  payload: Transaction.EntryDraftSchema,
  requestId: Schema.optionalKey(Identity.RequestId),
})
export type Draft = typeof DraftSchema.Type
export const QueueModesSchema = RunControl.QueueModesSchema
export type QueueModes = RunControl.QueueModes
export type Boundary = 'postTools' | 'final'

/** Context engine supplies the active head and explicitly identifies reset writes.
 * The predicate must be pure. Legacy incidental self-heads are not automatically resets.
 */
export interface ContextBoundary {
  readonly head?: Record.EntryId
  readonly resetsRun: (draft: Transaction.EntryDraft) => boolean
}
export interface BoundaryOptions {
  readonly boundary: Boundary
  readonly taskId?: Record.TaskId
  readonly nextTaskId?: Record.TaskId
  readonly modes?: QueueModes
  readonly context?: ContextBoundary
}
export interface Selection {
  readonly boundary: Boundary
  readonly previous: RunControl.Membership | undefined
  readonly active: RunControl.Membership | undefined
  readonly placed: ReadonlyArray<Record.Submission>
  readonly stale: ReadonlyArray<Record.WriteUnanswered>
}

const conflict = (operation: string, message: string) =>
  new SessionError({ reason: 'conflict', operation, message })

type DocumentDraft = Runtime.State['documents'] extends Map<string, infer D> ? D : never

/** Private operation savepoint, following TransactionRuntime's initializer clone/adopt.
 * Parent permit prevents concurrent transaction operations from observing child drafts.
 * Mutable document aliases must share the same clone. IDs may have gaps after rollback.
 */
const atomic = <A, E, R>(
  tx: Transaction.Transaction,
  run: (child: Transaction.Transaction) => Effect.Effect<A, E, R>,
) =>
  Runtime.use(tx, (state) =>
    Effect.gen(function* () {
      const clones = new Map<DocumentDraft, DocumentDraft>()
      const clone = (draft: DocumentDraft): DocumentDraft => {
        const found = clones.get(draft)
        if (found !== undefined) return found
        const copy = { ...draft }
        clones.set(draft, copy)
        return copy
      }
      const child: Runtime.State = {
        ...state,
        semaphore: yield* Semaphore.make(1),
        conversations: new Map(state.conversations),
        entries: new Map(state.entries),
        tasks: new Map(state.tasks),
        submissions: new Map(state.submissions),
        documents: new Map(Array.from(state.documents, ([key, draft]) => [key, clone(draft)])),
        incarnations: new Map(Array.from(state.incarnations, ([id, draft]) => [id, clone(draft)])),
        copiedSources: new Set(state.copiedSources),
      }
      const capability: Transaction.Transaction = {
        [Transaction.TypeId]: Transaction.TypeId,
        pipe() {
          return pipeArguments(this, arguments)
        },
      }
      Runtime.register(capability, child)
      const result = yield* run(capability).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            child.active = false
          }),
        ),
      )
      yield* Runtime.active(state)
      const adopt = <K, V>(destination: Map<K, V>, source: Map<K, V>) => {
        destination.clear()
        for (const [key, value] of source) destination.set(key, value)
      }
      // No fallible work follows adoption; failures above discard the complete child overlay.
      adopt(state.conversations, child.conversations)
      adopt(state.entries, child.entries)
      adopt(state.tasks, child.tasks)
      adopt(state.submissions, child.submissions)
      adopt(state.documents, child.documents)
      adopt(state.incarnations, child.incarnations)
      state.copiedSources.clear()
      for (const id of child.copiedSources) state.copiedSources.add(id)
      return result
    }),
  )

/** Install through the existing atomic conversation initializer, including fork creation. */
export const initialize = (tx: Transaction.Transaction, conversationId: Record.ConversationId) =>
  atomic(tx, (child) =>
    Effect.gen(function* () {
      yield* Transaction.ensureDocument(child, document, target(conversationId))
      yield* Transaction.ensureDocument(child, RunControl.document, target(conversationId))
    }),
  )

/** Queue data only: no task scheduling, model request, or application callback. */
export const admit = (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  input: Draft,
) =>
  atomic(tx, (child) =>
    Effect.gen(function* () {
      const draft = yield* Value.copy(DraftSchema, input)
      const type = draft.mode === 'write' ? 'write' : 'input'
      if (draft.requestId !== undefined) {
        const duplicate = yield* Transaction.submissionByRequest(
          child,
          conversationId,
          draft.requestId,
        )
        if (Option.isSome(duplicate)) {
          if (duplicate.value.type !== type)
            return yield* conflict('inbox.admit', 'Request has a different submission type')
          return duplicate.value
        }
      }
      const snapshot = yield* Transaction.ensureDocument(child, document, target(conversationId))
      yield* Transaction.ensureDocument(child, RunControl.document, target(conversationId))
      const request = draft.requestId === undefined ? {} : { requestId: draft.requestId }
      const submission = yield* Transaction.createSubmission(
        child,
        type === 'write'
          ? { _tag: 'WriteQueued', type, status: 'queued', conversationId, ...request }
          : { _tag: 'InputQueued', type, status: 'queued', conversationId, ...request },
      )
      yield* Transaction.setDocument(child, document, target(conversationId), {
        items: [
          ...snapshot.value.items,
          { submissionId: submission.id, mode: draft.mode, payload: draft.payload },
        ].sort((a, b) => a.submissionId - b.submissionId),
      })
      return submission
    }),
  )

/** Withdrawal of an already placed/settled item is a no-op; abort can retain writes. */
export const withdraw = (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  submissionId: Record.SubmissionId,
) =>
  atomic(tx, (child) =>
    Effect.gen(function* () {
      const snapshot = yield* Transaction.ensureDocument(child, document, target(conversationId))
      const submission = yield* Transaction.submission(child, submissionId)
      if (
        Option.isNone(submission) ||
        submission.value.conversationId !== conversationId ||
        submission.value.status !== 'queued'
      )
        return false
      const current = submission.value
      yield* Transaction.putSubmission(
        child,
        current.type === 'write'
          ? { ...current, _tag: 'WriteUnanswered', status: 'unanswered', reason: 'withdrawn' }
          : { ...current, _tag: 'InputUnanswered', status: 'unanswered', reason: 'withdrawn' },
      )
      yield* Transaction.setDocument(child, document, target(conversationId), {
        items: snapshot.value.items.filter((item) => item.submissionId !== submissionId),
      })
      return true
    }),
  )

const isLive = (task: Record.Task, conversationId: Record.ConversationId) =>
  task.conversationId === conversationId &&
  !task.abortRequested &&
  task.state.status !== 'terminal' &&
  task.state.status !== 'completing'

const reconcileState = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
) {
  const queue = yield* Transaction.ensureDocument(tx, document, target(conversationId))
  const control = yield* Transaction.ensureDocument(tx, RunControl.document, target(conversationId))
  const items: Array<Item> = []
  for (const item of queue.value.items) {
    const receipt = yield* Transaction.submission(tx, item.submissionId)
    if (Option.isNone(receipt) || receipt.value.status !== 'queued') continue
    if (
      receipt.value.conversationId !== conversationId ||
      receipt.value.type !== (item.mode === 'write' ? 'write' : 'input')
    )
      return yield* conflict('inbox.reconcile', 'Queued item identity does not match its receipt')
    items.push(item)
  }
  let active: RunControl.Membership | undefined
  if (control.value.active !== undefined) {
    const task = yield* Transaction.task(tx, control.value.active.taskId)
    if (Option.isSome(task) && isLive(task.value, conversationId)) {
      const ids: Array<Record.SubmissionId> = []
      for (const id of control.value.active.submissionIds) {
        const receipt = yield* Transaction.submission(tx, id)
        if (
          Option.isSome(receipt) &&
          receipt.value.type === 'input' &&
          receipt.value.status === 'placed' &&
          receipt.value.conversationId === conversationId
        )
          ids.push(id)
      }
      if (ids.length > 0) active = { taskId: task.value.id, submissionIds: ids }
    }
  }
  return { items, active }
})

/** Remove receipts that have already left the queue and dead/stale run memberships. */
export const reconcile = (tx: Transaction.Transaction, conversationId: Record.ConversationId) =>
  atomic(tx, (child) =>
    Effect.gen(function* () {
      const state = yield* reconcileState(child, conversationId)
      yield* Transaction.setDocument(child, document, target(conversationId), {
        items: state.items,
      })
      yield* Transaction.setDocument(
        child,
        RunControl.document,
        target(conversationId),
        state.active === undefined ? {} : { active: state.active },
      )
      return state
    }),
  )

/** Place all writes before independently limited user queues. Caller invokes postTools only
 * after every tool settles. final releases prior membership; caller must settle its receipts
 * in this same enclosing commit. A selected final batch requires a distinct, live next task.
 * The returned effective boundary exposes reset handling to the context/scheduler owner.
 */
export const select = (
  tx: Transaction.Transaction,
  conversationId: Record.ConversationId,
  options: BoundaryOptions,
) =>
  atomic(tx, (child) =>
    Effect.gen(function* (): Effect.fn.Return<Selection, import('../SessionError.js').Failure> {
      const modes = yield* Value.copy(QueueModesSchema, options.modes ?? {})
      const state = yield* reconcileState(child, conversationId)
      if (
        options.taskId !== state.active?.taskId ||
        (options.boundary === 'postTools' && state.active === undefined)
      )
        return yield* conflict('inbox.select', 'Boundary owner is no longer the active run')
      let boundary = options.boundary
      let head = options.context?.head
      const placed: Array<Record.Submission> = []
      const stale: Array<Record.WriteUnanswered> = []
      const consumed = new Set<Record.SubmissionId>()
      for (const item of state.items.filter((item) => item.mode === 'write')) {
        const receipt = Option.getOrThrow(yield* Transaction.submission(child, item.submissionId))
        if (receipt._tag !== 'WriteQueued')
          return yield* conflict('inbox.select', 'Write is no longer queued')
        consumed.add(item.submissionId)
        if (
          typeof item.payload.head === 'number' &&
          head !== undefined &&
          item.payload.head < head
        ) {
          const value: Record.WriteUnanswered = {
            ...receipt,
            _tag: 'WriteUnanswered',
            status: 'unanswered',
            reason: 'stale',
          }
          yield* Transaction.putSubmission(child, value)
          stale.push(value)
          continue
        }
        const resets = yield* Effect.try({
          try: () => options.context?.resetsRun(item.payload) ?? false,
          catch: (cause) =>
            new SessionError({
              reason: 'invalid',
              operation: 'inbox.context',
              message: 'Context boundary classification failed',
              cause,
            }),
        })
        const entry = yield* Transaction.appendEntry(child, conversationId, item.payload)
        const value: Record.WriteDone = {
          ...receipt,
          _tag: 'WriteDone',
          status: 'done',
          entry: entry.id,
        }
        yield* Transaction.putSubmission(child, value)
        placed.push(value)
        if (entry.head !== undefined) head = entry.head
        if (resets) boundary = 'final'
      }
      const users: Array<Item> = []
      for (const mode of ['followUp', 'steer'] as const) {
        if (boundary === 'postTools' && mode === 'followUp') continue
        const candidates = state.items.filter((item) => item.mode === mode)
        const limit = mode === 'followUp' ? modes.followUp : modes.steering
        users.push(...(limit === 'all' ? candidates : candidates.slice(0, 1)))
      }
      users.sort((a, b) => a.submissionId - b.submissionId)
      let active = boundary === 'postTools' ? state.active : undefined
      if (users.length > 0) {
        const taskId = boundary === 'postTools' ? state.active?.taskId : options.nextTaskId
        if (taskId === undefined || (boundary === 'final' && taskId === state.active?.taskId))
          return yield* conflict(
            'inbox.select',
            'Final user selection requires a distinct next run',
          )
        const task = yield* Transaction.task(child, taskId)
        if (Option.isNone(task) || !isLive(task.value, conversationId))
          return yield* conflict('inbox.select', 'Selected run is not live in this conversation')
        const ids = [...(active?.submissionIds ?? [])]
        for (const item of users) {
          const receipt = Option.getOrThrow(yield* Transaction.submission(child, item.submissionId))
          if (receipt._tag !== 'InputQueued')
            return yield* conflict('inbox.select', 'Input is no longer queued')
          const entry = yield* Transaction.appendEntry(child, conversationId, item.payload)
          const value: Record.InputPlaced = {
            ...receipt,
            _tag: 'InputPlaced',
            status: 'placed',
            entry: entry.id,
          }
          yield* Transaction.putSubmission(child, value)
          placed.push(value)
          consumed.add(item.submissionId)
          ids.push(item.submissionId)
        }
        active = { taskId, submissionIds: ids.sort((a, b) => a - b) }
      }
      yield* Transaction.setDocument(child, document, target(conversationId), {
        items: state.items.filter((item) => !consumed.has(item.submissionId)),
      })
      yield* Transaction.setDocument(
        child,
        RunControl.document,
        target(conversationId),
        active === undefined ? {} : { active },
      )
      return { boundary, previous: state.active, active, placed, stale }
    }),
  )
