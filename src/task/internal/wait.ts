import { Deferred, Effect, Option } from 'effect'
import type { Tx } from '../../session/Session.ts'
import type { StorageError } from '../../storage/StorageError.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { JoinPolicy, TaskRecord } from '../../storage/schema/Task.ts'
import type { TerminalTask } from '../Task.ts'
import { TaskNotFound, TaskRejected } from '../TaskError.ts'
import type { SchedulerClosed } from '../TaskError.ts'
import { loadScopes } from './chains.ts'
import { closedError, isTerminal, readsFirst } from './state.ts'
import type { Invocation, State } from './state.ts'
import { overlayOf, walkDown } from './walk.ts'
import type { Overlay } from './walk.ts'

export const notFound = (taskId: TaskId) =>
  new TaskNotFound({ message: `Task ${taskId} does not exist`, taskId })

const rejected = (taskId: TaskId, message: string) => new TaskRejected({ message, taskId })

/** The shared receipt of a live task, created at its first waiter. */
const receiptOf = (state: State, id: TaskId) => {
  const existing = state.receipts.get(id)
  if (existing !== undefined) return existing
  const created = Deferred.makeUnsafe<TerminalTask, SchedulerClosed>()
  state.receipts.set(id, created)
  return created
}

/**
 * Wait for a task's terminal receipt. Checking the task and registering the wait happen on the
 * line, so no terminal commit falls between them; the wait itself runs off the line.
 */
export const waitForTask = (state: State, id: TaskId) =>
  Effect.flatMap(
    readsFirst(
      state.session.commit((tx) =>
        Effect.gen(function* () {
          if (state.closing) return yield* closedError()
          const record = state.live.has(id) ? undefined : yield* tx.task(id)
          if (record !== undefined && isTerminal(record)) return { done: record }
          if (record === undefined && !state.live.has(id)) return yield* notFound(id)
          return { wait: receiptOf(state, id) }
        }),
      ),
    ),
    (found) => ('done' in found ? Effect.succeed(found.done) : Deferred.await(found.wait)),
  )

/** A task record as the commit stages it, as committed, or from Storage. */
const memberOf = (
  state: State,
  staged: ReadonlyMap<TaskId, TaskRecord>,
  id: TaskId,
): Effect.Effect<TaskRecord | undefined, StorageError> => {
  const known = staged.get(id) ?? state.live.get(id)
  return known === undefined ? state.storage.task(id) : Effect.succeed(known)
}

/** The path from `end` back to the start of the search. */
const pathTo = (from: ReadonlyMap<TaskId, TaskId | undefined>, end: TaskId) => {
  const steps: Array<TaskId> = []
  for (let at: TaskId | undefined = end; at !== undefined; at = from.get(at)) steps.unshift(at)
  return steps
}

/**
 * The path from a task in `on` back to `task` in the wait-for graph, if the wait would close a
 * cycle (spec §5.1). An edge X -> Y means X cannot settle before Y: Y is in the `on` of waiting X
 * that is not abort-marked, or Y is live ordinary owned work of X, which holds X as `completing`.
 * Each task is visited once and each subtree walked once. Owner chains must be loaded.
 */
const waitCycle = (
  state: State,
  task: TaskId,
  on: ReadonlyArray<TaskId>,
  overlay: Overlay,
): ReadonlyArray<TaskId> | undefined => {
  const from = new Map<TaskId, TaskId | undefined>()
  const walked = new Set<TaskId>()
  const pending: Array<TaskId> = []
  const reach = (id: TaskId, via?: TaskId) => {
    if (from.has(id)) return
    from.set(id, via)
    pending.push(id)
  }
  for (const id of on) reach(id)
  for (let id = pending.pop(); id !== undefined; id = pending.pop()) {
    if (id === task) return pathTo(from, id)
    const record = overlay.tasks.get(id) ?? state.live.get(id)
    if (record === undefined || record.state.status === 'terminal') continue
    // An abort-marked task waits only for its owned work, as abort runs bottom-up.
    if (record.state.status === 'waiting' && !record.abortRequested) {
      for (const member of record.state.on) reach(member, id)
    }
    if (walked.has(id)) continue
    walked.add(id)
    // Below a walked task the walk does not go again: its own walk covered what lies below.
    const found: Array<TaskId> = []
    for (const below of walkDown(state, id, { overlay, enter: (next) => !walked.has(next) })) {
      if (below.live === undefined || below.node.background) continue
      found.push(below.id)
      reach(below.id, id)
    }
    for (const below of found) walked.add(below)
  }
  return undefined
}

const waitProblem = Effect.fnUntraced(function* (
  state: State,
  tx: Tx,
  invocation: Invocation,
  on: ReadonlyArray<TaskId>,
  policy: JoinPolicy,
) {
  const id = invocation.taskId
  if (invocation.mode === 'abort') return Option.some(`Abort handler of task ${id} cannot wait`)
  const overlay = yield* overlayOf(tx)
  const targets: Array<TaskId> = []
  for (const member of on) {
    const record = yield* memberOf(state, overlay.tasks, member)
    if (record === undefined) return Option.some(`Task ${member} does not exist`)
    if (policy === 'failFast' && record.owner !== id) {
      return Option.some(`Task ${id} can wait failFast only on tasks it owns; ${member} is not one`)
    }
    // A live child already holds the waiter: waiting on it adds no edge. A terminal task has none.
    if (record.state.status !== 'terminal' && record.owner !== id) targets.push(member)
  }
  if (targets.length === 0) return Option.none<string>()
  // Owned work is found by walks down, which need the chains of every live task loaded.
  yield* loadScopes(state)
  return Option.map(
    Option.fromNullishOr(waitCycle(state, id, targets, overlay)),
    (cycle) =>
      `Task ${id} cannot wait on ${cycle[0]}: ${[id, ...cycle].join(' -> ')} is a wait cycle`,
  )
})

/**
 * A wait names existing tasks, and `failFast` only tasks the waiter owns. It may not close a
 * cycle in the wait-for graph: waiting on itself or on an owner holding it are the shortest. An
 * abort handler cannot wait.
 */
export const validateWait = (
  state: State,
  tx: Tx,
  invocation: Invocation,
  on: ReadonlyArray<TaskId>,
  policy: JoinPolicy,
) =>
  Effect.flatMap(waitProblem(state, tx, invocation, on, policy), (problem) =>
    Option.isSome(problem) ? Effect.fail(rejected(invocation.taskId, problem.value)) : Effect.void,
  )
