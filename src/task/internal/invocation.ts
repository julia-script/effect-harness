/* oxlint-disable effecttsgo/any-unknown-in-error-context -- task code may fail with anything, and a failure of any type faults its task */
import { Cause, Deferred, Effect, Equal, Exit, Option } from 'effect'
import { cast } from 'effect/Function'
import type { Tx } from '../../session/Session.ts'
import type { TxError } from '../../session/SessionError.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { SchedulerOutcome } from '../Scheduler.ts'
import type { Snapshot } from '../Registry.ts'
import type * as Task from '../Task.ts'
import { loadScopes, scheduleReconcile, settleOutcome } from './chains.ts'
import { canReserve } from './resolve.ts'
import { makeRuntime } from './runtime.ts'
import { causeMessage, end, lookup, report, runTaskCode, withState } from './state.ts'
import type { Erased, Invocation, State } from './state.ts'
import { parentOf } from './tree.ts'
import { belowCancelled, hasOwnedLive, overlayOf } from './walk.ts'

/** A reserved invocation, the definition it runs, and the snapshot it was resolved from. */
export interface Reservation {
  readonly invocation: Invocation
  readonly task: Erased
  readonly snapshot: Snapshot
}

/** Outcome of the phase that just returned, judged by the next step. */
interface PhaseResult {
  readonly checkpoint: Task.Checkpoint
  readonly failure?: Cause.Cause<unknown>
}

/** Continue with the next phase, end the invocation, or end it by writing `faulted`. */
type Decision = boolean | { readonly fault: string }

/** What a run invocation keeps across its phases. */
interface Run {
  task: Erased
  snapshot: Snapshot
  /** The replacement definition last reported as unable to take the task over. */
  reported: { readonly task: Erased | undefined } | undefined
}

/**
 * Write an outcome the scheduler decided. While the task's ordinary owned work is live it holds as
 * `completing` (spec §5.5); otherwise it is terminal.
 */
export const terminate = Effect.fnUntraced(function* (
  state: State,
  tx: Tx,
  record: TaskRecord,
  outcome: SchedulerOutcome,
) {
  yield* loadScopes(state)
  const holds = hasOwnedLive(state, record.id, yield* overlayOf(tx))
  yield* tx.setTask(withState(record, { status: holds ? 'completing' : 'terminal', outcome }))
  if (!holds) yield* settleOutcome(state, tx, record, outcome)
})

const checkpointOf = (record: TaskRecord): Task.Checkpoint =>
  cast('checkpoint' in record.state ? record.state.checkpoint : { phase: '' })

/**
 * One decision on the Session line. A task that is no longer running (rule 1) or a closing
 * Scheduler (rule 2) ends the invocation without a write; otherwise `decide` may stage a write and
 * says whether the invocation goes on. The invocation ends inside the step that decides its end,
 * so a runtime commit it queued either lands before the decision or is refused. A failed step
 * also ends the invocation; the task stays running until the next Scheduler.
 */
const step = (
  state: State,
  invocation: Invocation,
  decide: (tx: Tx, current: TaskRecord) => Effect.Effect<Decision, TxError>,
) =>
  state.session
    .commit((tx) =>
      Effect.gen(function* () {
        const found = state.live.get(invocation.taskId)
        const current = found?.state.status === 'running' ? found : undefined
        const decision =
          current !== undefined && !state.closing ? yield* decide(tx, current) : false
        if (decision === true && current !== undefined) return Option.some(current)
        end(state, invocation)
        if (typeof decision === 'object' && current !== undefined) {
          const outcome = { status: 'faulted', error: { message: decision.fault } } as const
          yield* terminate(state, tx, current, outcome)
        }
        return Option.none<TaskRecord>()
      }),
    )
    .pipe(
      Effect.tapCause((cause) =>
        Effect.andThen(
          Effect.sync(() => {
            end(state, invocation)
          }),
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : report(`Step of task ${invocation.taskId} failed`, cause),
        ),
      ),
      Effect.orElseSucceed(() => Option.none<TaskRecord>()),
    )

/** Rules 3 to 6 for a run invocation, after rules 1 and 2 passed. */
const decide = Effect.fnUntraced(function* (
  state: State,
  tx: Tx,
  current: TaskRecord,
  previous: PhaseResult | undefined,
  run: Run,
) {
  // 3. Abort mark: end; a fresh abort invocation starts once the task's owned work is gone.
  if (current.abortRequested) return false
  // An owner's cancellation intent ends it too, before its cascade marks it.
  if (!current.background && belowCancelled(state, parentOf(current))) {
    scheduleReconcile(state)
    return false
  }
  if (previous === undefined) return true
  // 4. A failed or defective phase.
  if (previous.failure !== undefined) return { fault: causeMessage(previous.failure) }
  // 6. No durable progress.
  const checkpoint = checkpointOf(current)
  if (Equal.equals(checkpoint, previous.checkpoint)) {
    const phase = previous.checkpoint.phase
    return { fault: `Task ${current.kind} phase ${phase} returned without durable progress` }
  }
  // 5. Progress: refresh the snapshot, and hand over to a replacement that can take the task.
  run.snapshot = yield* state.registry.snapshot
  const next = lookup(run.snapshot, current.kind)
  if (next === run.task) return true
  if (next !== undefined && canReserve(next, current)) {
    yield* tx.setTask(withState(current, { status: 'pending', checkpoint }))
    return false
  }
  if (run.reported === undefined || run.reported.task !== next) {
    run.reported = { task: next }
    const reason = next === undefined ? 'missing_task' : 'incompatible_task'
    const message = `Task ${current.id} keeps running under its old ${current.kind} definition`
    yield* report(message, Cause.fail(reason))
  }
  return true
})

/** Run phase handlers, each preceded by a step that decides whether the invocation goes on. */
const runPhases = Effect.fnUntraced(function* (state: State, reservation: Reservation) {
  const { invocation } = reservation
  const run: Run = { task: reservation.task, snapshot: reservation.snapshot, reported: undefined }
  const runtime = makeRuntime(state, invocation)
  let previous: PhaseResult | undefined
  for (;;) {
    const before = previous
    const next = yield* step(state, invocation, (tx, task) => decide(state, tx, task, before, run))
    if (Option.isNone(next) || state.closing) return
    const current = next.value
    const checkpoint = checkpointOf(current)
    const handler = run.task.phases[checkpoint.phase]
    const exit =
      handler === undefined
        ? Exit.die(new Error(`Task ${current.kind} has no phase ${checkpoint.phase}`))
        : yield* runTaskCode(() => handler(cast(current), runtime))
    previous = Exit.isSuccess(exit) ? { checkpoint } : { checkpoint, failure: exit.cause }
  }
})

/** Run the abort handler once; returning without a terminal outcome faults the task. */
const runAbort = Effect.fnUntraced(function* (state: State, reservation: Reservation) {
  const { invocation, task } = reservation
  const current = state.live.get(invocation.taskId)
  if (current?.state.status !== 'running' || state.closing) return
  const runtime = makeRuntime(state, invocation)
  const exit = yield* runTaskCode(() => task.abort(cast(current), runtime))
  const fault = Exit.isFailure(exit)
    ? causeMessage(exit.cause)
    : `Abort handler of task ${invocation.taskId} returned without a terminal outcome`
  yield* step(state, invocation, () => Effect.succeed({ fault }))
})

/**
 * Fork a reserved invocation into the Scheduler's invocation scope. `done` completes from a fiber
 * observer, after the fibers the invocation forked were interrupted too.
 */
export const start = (state: State, reservation: Reservation) => {
  const { invocation } = reservation
  const body =
    invocation.mode === 'run' ? runPhases(state, reservation) : runAbort(state, reservation)
  const ended = Effect.sync(() => {
    end(state, invocation)
  })
  return Effect.map(Effect.forkIn(Effect.ensuring(body, ended), state.scope), (fiber) => {
    invocation.fiber = fiber
    fiber.addObserver(() => {
      Deferred.doneUnsafe(invocation.done, Exit.void)
    })
  })
}
