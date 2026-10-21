import { Effect } from 'effect'
import type { Schema } from 'effect'
import { cast } from 'effect/Function'
import type { Tx } from '../../session/Session.ts'
import type { TaskRecord, TaskOutcome } from '../../storage/schema/Task.ts'
import type * as Task from '../Task.ts'
import { InvocationEnded, TaskRejected } from '../TaskError.ts'
import type { SchedulerClosed } from '../TaskError.ts'
import { closedError, isTerminal, memoOf, withMemo, withState } from './state.ts'
import type { Invocation, State } from './state.ts'
import { abort } from './abort.ts'
import { loadChain, loadScopes } from './chains.ts'
import { parentOf } from './tree.ts'
import { hasOwnedLive, overlayOf } from './walk.ts'
import { validateWait, waitForTask } from './wait.ts'

type ErasedRuntime = Task.Runtime<Schema.Json, Task.Checkpoint, Schema.Json>
type ErasedNext = Task.NextState<Task.Checkpoint, Schema.Json>

/** A state the change returned; returning nothing leaves the task's state. */
const isNextState = (value: ErasedNext | void): value is ErasedNext => value !== undefined

const endedError = (invocation: Invocation) =>
  new InvocationEnded({
    message: `Task ${invocation.taskId} invocation has ended`,
    taskId: invocation.taskId,
  })

const rejected = (invocation: Invocation, message: string) =>
  new TaskRejected({ message: `Task ${invocation.taskId} ${message}`, taskId: invocation.taskId })

/** Run `effect` unless the invocation has ended. */
const live = <A, E>(invocation: Invocation, effect: Effect.Effect<A, E>) =>
  Effect.suspend((): Effect.Effect<A, E | InvocationEnded> =>
    invocation.ended ? Effect.fail(endedError(invocation)) : effect,
  )

type Refusal = InvocationEnded | SchedulerClosed | TaskRejected

/** Why a runtime commit is refused, read on the line; `undefined` admits it. */
const gate = (
  state: State,
  invocation: Invocation,
  found: TaskRecord | undefined,
): Refusal | undefined => {
  if (invocation.ended) return endedError(invocation)
  if (state.closing) return closedError()
  if (found === undefined) return rejected(invocation, 'is terminal')
  if (found.state.status !== 'running') return rejected(invocation, `is ${found.state.status}`)
  return invocation.mode === 'run' && found.abortRequested
    ? rejected(invocation, 'has a durable abort mark')
    : undefined
}

/**
 * Commit after rereading the task on the line and gating the invocation. The commit is bound to
 * the task, so its entries record it and its new tasks default to its conversation.
 */
export const gated = <A, E>(
  state: State,
  invocation: Invocation,
  change: (tx: Tx, current: TaskRecord) => Effect.Effect<A, E>,
) =>
  live(
    invocation,
    state.session.commit(
      (tx) =>
        Effect.suspend((): Effect.Effect<A, E | Refusal> => {
          const found = state.live.get(invocation.taskId)
          const refused = gate(state, invocation, found)
          return refused !== undefined || found === undefined
            ? Effect.fail(refused ?? endedError(invocation))
            : change(tx, found)
        }),
      { conversationId: invocation.conversationId, taskId: invocation.taskId },
    ),
  )

/**
 * Whether ordinary owned work is live below the task, judged on the commit's candidates, so work
 * the same commit creates below it counts.
 */
const holds = Effect.fnUntraced(function* (state: State, tx: Tx, current: TaskRecord) {
  const overlay = yield* overlayOf(tx)
  yield* loadScopes(state)
  for (const record of overlay.tasks.values()) yield* loadChain(state, parentOf(record), overlay)
  return hasOwnedLive(state, current.id, overlay)
})

/**
 * Replace the running task's state with what it committed. A wait is validated first; an outcome
 * holds as `completing` while ordinary owned work is live.
 */
const commitState = Effect.fnUntraced(function* (
  state: State,
  invocation: Invocation,
  tx: Tx,
  current: TaskRecord,
  next: ErasedNext,
) {
  if (next.status === 'waiting') yield* validateWait(state, tx, invocation, next.on, next.policy)
  if (next.status === 'terminal' && (yield* holds(state, tx, current))) {
    yield* tx.setTask(withState(current, { status: 'completing', outcome: next.outcome }))
    return
  }
  yield* tx.setTask(withState(current, next))
})

const outcomes = (state: State, invocation: Invocation, ids: ReadonlyArray<TaskRecord['id']>) =>
  live(
    invocation,
    state.session.commit((tx) =>
      Effect.forEach(ids, (id) =>
        Effect.flatMap(tx.task(id), (record) =>
          record !== undefined && isTerminal(record)
            ? Effect.succeed<TaskOutcome>(record.state.outcome)
            : Effect.fail(rejected(invocation, `cannot read task ${id}: it is not terminal`)),
        ),
      ),
    ),
  )

/** Abort a task the invocation's task owns, keeping a `restart` mark, and wait for its end. */
const abortOwned = (state: State, invocation: Invocation, id: TaskRecord['id']) =>
  live(
    invocation,
    Effect.flatMap(
      state.session.commit(() => state.storage.task(id)),
      (record): Effect.Effect<void, Task.RuntimeError> => {
        if (record?.owner !== invocation.taskId) {
          return Effect.fail(rejected(invocation, `does not own task ${id}`))
        }
        if (isTerminal(record)) return Effect.void
        return Effect.andThen(abort(state, id, true), waitForTask(state, id))
      },
    ),
  )

/** Live tasks the invocation's task owns directly: each is listed under its owner task. */
const ownedTasks = (state: State, invocation: Invocation) =>
  live(
    invocation,
    state.session.commit(() =>
      Effect.suspend(() => {
        // The step that ends the invocation may have run on the line before this.
        if (invocation.ended) return Effect.fail(endedError(invocation))
        const owned: Array<TaskRecord> = []
        for (const node of state.tree.below.get(invocation.taskId) ?? []) {
          const record = typeof node === 'string' ? undefined : state.live.get(node)
          if (record?.owner === invocation.taskId) owned.push(record)
        }
        return Effect.succeed(owned.toSorted((left, right) => left.id - right.id))
      }),
    ),
  )

/** The runtime of one invocation, with its definition's types erased. */
export const makeRuntime = (state: State, invocation: Invocation): ErasedRuntime => ({
  taskId: invocation.taskId,
  conversationId: invocation.conversationId,
  commit: (change) =>
    gated(state, invocation, (tx, current) =>
      Effect.flatMap(change(tx, cast(current)), (next) =>
        isNextState(next) ? commitState(state, invocation, tx, current, next) : Effect.void,
      ),
    ),
  memo: (name, candidate) =>
    gated(state, invocation, (tx, current) => {
      const winner = memoOf(current, name)
      if (winner !== undefined) return Effect.succeed(cast<Schema.Json, typeof candidate>(winner))
      return Effect.as(tx.setTask(withMemo(current, name, candidate)), candidate)
    }),
  readMemo: (name) =>
    live(
      invocation,
      Effect.sync(() => memoOf(state.live.get(invocation.taskId), name)),
    ),
  getTask: (id) =>
    live(
      invocation,
      state.session.commit((tx) => tx.task(id)),
    ),
  waitForTask: (id) => live(invocation, waitForTask(state, id)),
  abortOwned: (id) => abortOwned(state, invocation, id),
  ownedTasks: ownedTasks(state, invocation),
  outcomes: (ids) => outcomes(state, invocation, ids),
  snapshot: state.session.snapshot,
  snapshotAsOf: state.session.snapshotAsOf,
  watchDoc: state.session.watchDoc,
  documentState: state.session.documentState,
})
