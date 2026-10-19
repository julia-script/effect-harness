import { Deferred, Effect, Option } from 'effect'
import type { Tx } from '../../session/Session.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { Snapshot } from '../Registry.ts'
import { loadScopes, scheduleReconcile } from './chains.ts'
import { start, terminate } from './invocation.ts'
import type { Reservation } from './invocation.ts'
import { serve } from './reconcile.ts'
import { resolve } from './resolve.ts'
import { withAbortMark, withState } from './state.ts'
import type { Invocation, State } from './state.ts'
import { parentOf } from './tree.ts'
import { belowCancelled, ownedLive } from './walk.ts'

/**
 * Live tasks a task waits for before its next invocation: its live ordinary owned work when
 * abort-marked, since abort runs bottom-up, otherwise the live part of the `on` of a wait.
 */
export const waitingOn = (
  state: State,
  record: TaskRecord,
  first = false,
): ReadonlyArray<TaskId> => {
  if (record.abortRequested) return ownedLive(state, record.id, { first })
  if (record.state.status !== 'waiting') return []
  const live = record.state.on.filter((id) => state.live.has(id))
  return first ? live.slice(0, 1) : live
}

const createInvocation = (state: State, record: TaskRecord, mode: Invocation['mode']) => {
  const invocation: Invocation = {
    taskId: record.id,
    conversationId: record.conversationId,
    mode,
    ended: false,
    fiber: undefined,
    done: Deferred.makeUnsafe<void>(),
  }
  state.invocations.set(record.id, invocation)
  return invocation
}

/** Reserve one candidate, or settle it as `orphaned` when it is aborted and blocked. */
const reserveOne = Effect.fnUntraced(function* (
  state: State,
  tx: Tx,
  record: TaskRecord,
  snapshot: Snapshot,
) {
  const mode = record.abortRequested ? 'abort' : 'run'
  const resolution = yield* resolve(state, record, snapshot)
  if (resolution.kind === 'blocked') {
    // A blocked task without a mark stays live; an abandoned one waits for its definition, so its
    // abort handler can clean up; any other aborted one can run no abort handler.
    if (mode === 'abort' && record.abortReason === undefined) {
      yield* terminate(state, tx, record, { status: 'orphaned', reason: resolution.reason })
    }
    return Option.none<Reservation>()
  }
  const resolved = resolution.record
  if (resolved !== record || record.state.status !== 'running') {
    const checkpoint = 'checkpoint' in resolved.state ? resolved.state.checkpoint : null
    yield* tx.setTask(withState(resolved, { status: 'running', checkpoint }))
  }
  // Registered on the line, so marks and later passes see it; `end` makes it a candidate again.
  state.runnable.delete(record.id)
  const reservation: Reservation = {
    invocation: createInvocation(state, record, mode),
    task: resolution.task,
    snapshot,
  }
  return Option.some(reservation)
})

/** Whether a live task can be reserved now; one that cannot leaves the runnable set. */
const isCandidate = (state: State, id: TaskId): TaskRecord | undefined => {
  const record = state.live.get(id)
  const candidate =
    record !== undefined &&
    record.state.status !== 'completing' &&
    !state.invocations.has(id) &&
    // It returns once what it waits for ends, its record changes, or the work below it ends.
    waitingOn(state, record, true).length === 0
  if (!candidate) state.runnable.delete(id)
  return candidate ? record : undefined
}

/** Abort-mark the tasks abandoned by an earlier Scheduler, once, before anything else runs. */
const markAbandoned = Effect.fnUntraced(function* (state: State, tx: Tx) {
  for (const id of state.abandoned) {
    const record = state.live.get(id)
    if (record !== undefined && !record.abortRequested) {
      yield* tx.setTask(withAbortMark(record, 'restart'))
    }
  }
})

/** Reserve every eligible task of one pass; work below a cancelling owner waits for its mark. */
const reserveAll = Effect.fnUntraced(function* (
  state: State,
  tx: Tx,
  reservations: Array<Reservation>,
) {
  yield* loadScopes(state)
  let snapshot: Snapshot | undefined
  // Deleting the visited member of a Set does not disturb the iteration.
  for (const id of state.runnable) {
    const record = isCandidate(state, id)
    if (record === undefined) continue
    if (!record.abortRequested && !record.background && belowCancelled(state, parentOf(record))) {
      scheduleReconcile(state)
      continue
    }
    snapshot ??= yield* state.registry.snapshot
    const reservation = yield* reserveOne(state, tx, record, snapshot)
    if (Option.isSome(reservation)) reservations.push(reservation.value)
  }
})

/**
 * Reserve every eligible task in one commit. The first pass after open only abort-marks the
 * abandoned tasks, so later passes see the marks.
 */
const reserve = (state: State) => {
  const reservations: Array<Reservation> = []
  let abandoned = false
  return state.session
    .commit((tx) =>
      Effect.gen(function* () {
        if (state.closing) return
        if (state.abandoned.size > 0) {
          abandoned = true
          yield* markAbandoned(state, tx)
          return
        }
        yield* reserveAll(state, tx, reservations)
      }),
    )
    .pipe(
      Effect.onError(() =>
        Effect.sync(() => {
          for (const { invocation } of reservations) {
            state.invocations.delete(invocation.taskId)
            state.runnable.add(invocation.taskId)
          }
        }),
      ),
      Effect.map(() => {
        if (abandoned) {
          state.abandoned.clear()
          // Reserve again, now seeing the marks.
          state.wake.openUnsafe()
        }
        return reservations
      }),
    )
}

/** Reserve and start every eligible task. */
const pass = (state: State) =>
  Effect.suspend(() =>
    Effect.flatMap(reserve(state), (reservations) =>
      Effect.forEach(reservations, (reservation) => start(state, reservation), { discard: true }),
    ),
  )

/** Reserve and start tasks whenever a pass is requested. */
export const drive = (state: State) =>
  serve(state, state.wake, pass(state), 'Scheduler stopped: a reservation failed')
