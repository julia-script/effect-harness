/**
 * The reconcile pass: one commit that applies what committed records imply. It writes abort marks
 * below live owners with cancellation intent (spec §5.4), `failFast` marks (spec §5.5), withdrawn
 * queued inputs below those owners, and the terminal record of every `completing` task whose ordinary owned work is gone. The durable records
 * are the intent, so the pass also repairs whatever a crash left unapplied.
 */
import { Cause, Effect } from 'effect'
import type { Latch } from 'effect'
import type { Tx } from '../../session/Session.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import { loadQueued, loadScopes, settleOutcome, withdrawInputs } from './chains.ts'
import { settleIdle } from './idle.ts'
import { report, seal, withAbortMark, withState } from './state.ts'
import type { State } from './state.ts'
import { failedOutcome, sweep } from './tree.ts'
import { belowCancelled, hasOwnedLive, nearestLiveAbove, overlayOf, walkDown } from './walk.ts'

type Reason = 'restart' | undefined

/**
 * Marks of one pass, collected first, so a request wins over a `restart` mark of the same pass. A
 * task already marked keeps its mark, unless a request replaces its `restart` mark.
 */
const collectMark =
  (marks: Map<TaskId, { record: TaskRecord; reason: Reason }>) =>
  (record: TaskRecord, reason?: 'restart'): void => {
    const staged = marks.get(record.id)
    if (staged !== undefined && staged.reason === undefined) return
    if (record.abortRequested && (record.abortReason === undefined || reason !== undefined)) return
    marks.set(record.id, { record, reason })
  }

/** Whether any of `ids` holds or ended with an outcome other than `completed`. */
const anyFailed = Effect.fnUntraced(function* (state: State, ids: ReadonlyArray<TaskId>) {
  for (const id of ids) {
    const record = state.live.get(id) ?? (yield* state.storage.task(id))
    if (record !== undefined && failedOutcome(record)) return true
  }
  return false
})

/**
 * Every live foreground task below a live owner with cancellation intent, which is its nearest one:
 * a walk down from each owner with intent does not enter another, whose own walk covers below it.
 * Only an owner abandoned after a restart passes its `restart` reason on.
 */
const cascadeMarks = (state: State, mark: (record: TaskRecord, reason?: 'restart') => void) => {
  for (const id of state.intent) {
    const owner = state.live.get(id)
    if (owner === undefined) continue
    const restart = owner.abortReason === 'restart' && !failedOutcome(owner)
    for (const { node, live } of walkDown(state, id, {
      enter: (below) => !state.intent.has(below),
    })) {
      if (live !== undefined && !node.background) mark(live, restart ? 'restart' : undefined)
    }
  }
}

/** For each `failFast` waiter with a failed task in its `on`, mark every other live task in it. */
const failFastMarks = Effect.fnUntraced(function* (
  state: State,
  checks: ReadonlyArray<TaskId>,
  mark: (record: TaskRecord, reason?: 'restart') => void,
) {
  for (const id of checks) {
    const waiter = state.live.get(id)
    if (waiter?.state.status !== 'waiting' || !(yield* anyFailed(state, waiter.state.on))) continue
    // The failed one keeps its own outcome.
    for (const member of waiter.state.on) {
      const record = state.live.get(member)
      if (record !== undefined && !failedOutcome(record)) mark(record)
    }
  }
})

/**
 * Write the terminal record of every `completing` task without live ordinary owned work. Finalizing
 * one can free the nearest live task above it, which the next round checks: a holder of many tasks
 * that end together is walked once, not once per task.
 */
const finalize = Effect.fnUntraced(function* (state: State, tx: Tx) {
  let round = [...state.finalizeChecks]
  state.finalizeChecks.clear()
  const done = new Set<TaskId>()
  const overlay = yield* overlayOf(tx)
  while (round.length > 0) {
    const next = new Set<TaskId>()
    for (const id of round) {
      const record = overlay.tasks.get(id) ?? state.live.get(id)
      if (record?.state.status !== 'completing' || done.has(id)) continue
      if (hasOwnedLive(state, id, overlay, done)) continue
      const outcome = record.state.outcome
      const terminal = yield* tx.setTask(withState(record, { status: 'terminal', outcome }))
      yield* settleOutcome(state, tx, record, outcome)
      overlay.tasks.set(id, terminal)
      done.add(id)
      const holder = nearestLiveAbove(state, record, overlay)
      if (holder?.state.status === 'completing') next.add(holder.id)
    }
    round = [...next]
  }
})

/** One reconcile commit; it settles idle waits that the loaded chains decide. */
const reconcile = (state: State) =>
  Effect.suspend(() => {
    const checks = [...state.failFastChecks]
    state.failFastChecks.clear()
    const withdraw = state.withdrawPending
    state.withdrawPending = false
    return state.session
      .commit((tx) =>
        Effect.gen(function* () {
          if (state.closing) return
          yield* loadScopes(state)
          const queued = withdraw ? yield* loadQueued(state) : []
          const marks = new Map<TaskId, { record: TaskRecord; reason: Reason }>()
          const mark = collectMark(marks)
          cascadeMarks(state, mark)
          yield* failFastMarks(state, checks, mark)
          for (const { record, reason } of marks.values()) {
            yield* tx.setTask(withAbortMark(record, reason))
          }
          yield* withdrawInputs(state, tx, queued, (id) =>
            belowCancelled(state, { conversation: id }),
          )
          yield* finalize(state, tx)
        }),
      )
      .pipe(Effect.andThen(settleIdle(state)))
  })

/**
 * Run `pass` whenever `wake` opens. A failed pass seals the Scheduler: no task code runs in it, so
 * it is a storage failure or a bug, and passing again would not fix it.
 */
export const serve = <E>(
  state: State,
  wake: Latch.Latch,
  pass: Effect.Effect<void, E>,
  failure: string,
) =>
  Effect.forever(
    Effect.andThen(
      Effect.andThen(
        wake.await,
        Effect.sync(() => wake.closeUnsafe()),
      ),
      pass,
    ),
  ).pipe(
    Effect.tapCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.void
        : Effect.andThen(report(failure, cause), seal(state)),
    ),
    Effect.ignore,
  )

export const driveReconcile = (state: State) =>
  serve(state, state.reconcileWake, reconcile(state), 'Scheduler stopped: a reconcile pass failed')

/** Sweep in a job of its own on the line, after the operation underway. */
export const driveSweep = (state: State) =>
  serve(
    state,
    state.sweepWake,
    // Fails only once the Session is closed, when nothing is kept anyway.
    Effect.ignore(
      state.session.commit(() =>
        Effect.sync(() => {
          sweep(state)
        }),
      ),
    ),
    'Scheduler stopped: a sweep failed',
  )
