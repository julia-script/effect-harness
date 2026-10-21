import { Effect, Exit, Fiber, Scope, Stream } from 'effect'
import { Session } from '../../session/Session.ts'
import type { CommitPublication, Tx } from '../../session/Session.ts'
import { Storage } from '../../storage/Storage.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import { Registry } from '../Registry.ts'
import type { Options, Service } from '../Scheduler.ts'
import { abort } from './abort.ts'
import { scheduleReconcile } from './chains.ts'
import { abortConversation, settleIdle, waitForIdle } from './idle.ts'
import { inspect } from './inspect.ts'
import { driveReconcile, driveSweep } from './reconcile.ts'
import { drive } from './reserve.ts'
import { kick, makeState, readsFirst, resolveReceipt, seal, track, withState } from './state.ts'
import type { State } from './state.ts'
import {
  cancellationIntent,
  conversationNode,
  failedOutcome,
  known,
  parentOf,
  setEdge,
  sweep,
} from './tree.ts'
import { belowCancelled } from './walk.ts'
import { waitForTask } from './wait.ts'

const LIVE_STATUSES = ['pending', 'running', 'waiting', 'completing'] as const
const SCAN_PAGE_SIZE = 256

/** Interrupt the run invocation of a newly abort-marked task; its next step ends it. */
const interruptRun = (state: State, record: TaskRecord) => {
  const invocation = state.invocations.get(record.id)
  // Not forked yet: its first step finds the mark.
  return invocation?.mode === 'run' && invocation.fiber !== undefined
    ? Effect.asVoid(Effect.forkDetach(Fiber.interrupt(invocation.fiber)))
    : Effect.void
}

/** Request the reconcile passes a live task's change implies. */
const reconcileFor = (state: State, previous: TaskRecord | undefined, record: TaskRecord) => {
  const marked = record.abortRequested && previous?.abortRequested !== true
  // A request replacing a `restart` mark upgrades the marks of the work below.
  const upgraded = previous?.abortReason !== undefined && record.abortReason === undefined
  const status = record.state.status
  const holding = status === 'completing' && previous?.state.status !== 'completing'
  const failFast =
    record.state.status === 'waiting' &&
    record.state.policy === 'failFast' &&
    previous?.state.status !== 'waiting'
  if (failFast) state.failFastChecks.add(record.id)
  if (marked || upgraded || (holding && cancellationIntent(record))) state.withdrawPending = true
  if (marked || upgraded || holding || failFast) scheduleReconcile(state)
}

/** Check the `failFast` waiters of tasks that just failed. */
const checkFailFast = (state: State, failed: ReadonlyArray<TaskId>) => {
  for (const id of failed) {
    for (const waiter of state.waiters.get(id) ?? []) {
      const waiting = state.live.get(waiter)?.state
      if (waiting?.status === 'waiting' && waiting.policy === 'failFast') {
        state.failFastChecks.add(waiter)
        scheduleReconcile(state)
      }
    }
  }
}

/** Work created below a cancelled owner, even after its cascade, is aborted too. */
const checkCreated = (state: State, created: ReadonlyArray<TaskRecord>) => {
  for (const record of created) {
    if (state.tree.unloaded.has(record.id)) scheduleReconcile(state)
    else if (
      !record.background &&
      !record.abortRequested &&
      belowCancelled(state, parentOf(record))
    ) {
      state.withdrawPending = true
      scheduleReconcile(state)
    }
  }
}

/** A queued input below an owner with cancellation intent is withdrawn, even after its cascade. */
const checkQueued = (state: State, publication: CommitPublication) => {
  // Without intent nothing withdraws it; a later intent's mark withdraws it then.
  if (state.intent.size === 0) return
  for (const change of publication.changes) {
    if (change.type !== 'submission') continue
    const { status, type, conversationId } = change.value
    if (status !== 'queued' || type !== 'input') continue
    const node = conversationNode(conversationId)
    if (!known(state, node) || belowCancelled(state, { conversation: conversationId })) {
      state.withdrawPending = true
      scheduleReconcile(state)
    }
  }
}

/** Mirror one task change; `failed` and `created` collect what the publication checks after. */
const observeTask = Effect.fnUntraced(function* (
  state: State,
  record: TaskRecord,
  failed: Array<TaskId>,
  created: Array<TaskRecord>,
) {
  const previous = state.live.get(record.id)
  if (failedOutcome(record) && (previous === undefined || !failedOutcome(previous))) {
    failed.push(record.id)
  }
  track(state, previous, record)
  yield* resolveReceipt(state, record)
  if (record.state.status === 'terminal') {
    // Its owner may finalize now.
    scheduleReconcile(state)
    return
  }
  if (record.abortRequested && previous?.abortRequested !== true) yield* interruptRun(state, record)
  reconcileFor(state, previous, record)
  // Its owners and place never change: what is above it is checked once, when it becomes live.
  if (previous === undefined) created.push(record)
})

/**
 * The commit listener, on the line: mirror every task change and owner edge, complete receipts,
 * interrupt the run invocation of a newly abort-marked task, and request the passes it implies.
 */
const observe = (state: State) =>
  Effect.fnUntraced(function* (publication: CommitPublication) {
    // Edges first, so the tasks a commit creates in a new conversation hang from it.
    for (const change of publication.changes) {
      if (change.type === 'conversation' && !state.tree.edges.has(change.value.id)) {
        setEdge(state, change.value.id, change.value.owner?.taskId ?? null)
      }
    }
    const failed: Array<TaskId> = []
    const created: Array<TaskRecord> = []
    for (const change of publication.changes) {
      if (change.type === 'task') yield* observeTask(state, change.value, failed, created)
    }
    checkFailFast(state, failed)
    checkCreated(state, created)
    checkQueued(state, publication)
    sweep(state)
    if (!publication.changes.some(isTaskChange)) return
    yield* settleIdle(state)
    kick(state)
  })

const isTaskChange = (change: CommitPublication['changes'][number]) => change.type === 'task'

/** Every task record in one status, read in pages. */
const scanStatus = Effect.fnUntraced(function* (tx: Tx, status: (typeof LIVE_STATUSES)[number]) {
  const records: Array<TaskRecord> = []
  let page = yield* tx.scanTasks({ status }, SCAN_PAGE_SIZE)
  records.push(...page.items)
  while (page.next !== undefined) {
    page = yield* tx.scanTasks({ status }, SCAN_PAGE_SIZE, page.next)
    records.push(...page.items)
  }
  return records
})

/** Load live tasks, and change the `running` tasks a crash or close left back to `pending`. */
const recover = (state: State) =>
  readsFirst(
    state.session.commit((tx) =>
      Effect.gen(function* () {
        // Every table read before the first write.
        const records: Array<TaskRecord> = []
        for (const status of LIVE_STATUSES) records.push(...(yield* scanStatus(tx, status)))
        for (const record of records) {
          track(state, undefined, record)
          if (record.abandonOnRestart === true) state.abandoned.add(record.id)
          if (record.state.status === 'waiting' && record.state.policy === 'failFast') {
            state.failFastChecks.add(record.id)
          }
          if (record.state.status === 'running') {
            const checkpoint = record.state.checkpoint
            yield* tx.setTask(withState(record, { status: 'pending', checkpoint }))
          }
        }
      }),
    ),
  )

/** Seal, then interrupt every invocation and wait for it: close writes no outcome. */
const close = (state: State) => Effect.andThen(seal(state), Scope.close(state.scope, Exit.void))

export const make = Effect.fnUntraced(function* (hooks: Options) {
  const session = yield* Session
  const storage = yield* Storage
  const registry = yield* Registry
  // Invocations and the driver run in a child scope, so close can seal before interrupting them.
  const scope = yield* Scope.fork(yield* Effect.scope)
  const state = makeState(session, storage, registry, hooks, scope)
  yield* Effect.addFinalizer(() => close(state))
  yield* session.observeCommits(observe(state))
  yield* recover(state)
  // Derive abort marks and withdrawals a crash left unapplied below cancelled owners, and finalize
  // held outcomes.
  state.withdrawPending = true
  scheduleReconcile(state)
  yield* Effect.forkIn(drive(state), scope)
  yield* Effect.forkIn(driveReconcile(state), scope)
  yield* Effect.forkIn(driveSweep(state), scope)
  yield* Effect.forkIn(
    Stream.runForEach(registry.changes, () =>
      Effect.sync(() => {
        kick(state)
      }),
    ),
    scope,
  )
  kick(state)
  const service: Service = {
    abort: (id) => abort(state, id),
    waitForTask: (id) => waitForTask(state, id),
    waitForIdle: (conversationId) => waitForIdle(state, conversationId),
    abortConversation: (conversationId, options) =>
      abortConversation(state, conversationId, options?.background === true),
    inspect: inspect(state),
  }
  return service
})
