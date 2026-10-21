/* oxlint-disable effecttsgo/any-unknown-in-error-context -- task code may fail with anything, and a failure of any type faults its task */
import { Cause, Deferred, Effect, Latch } from 'effect'
import { cast } from 'effect/Function'
import type { Fiber, Schema, Scope } from 'effect'
import type { Service as SessionService } from '../../session/Session.ts'
import type { Service as StorageService } from '../../storage/Storage.ts'
import type { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord, TaskState } from '../../storage/schema/Task.ts'
import type { Service as RegistryService, Snapshot } from '../Registry.ts'
import type { Options } from '../Scheduler.ts'
import type * as Task from '../Task.ts'
import { SchedulerClosed } from '../TaskError.ts'
import {
  addTo,
  cancellationIntent,
  deleteFrom,
  dropIfUnneeded,
  known,
  makeTree,
  nodeOf,
  parentNode,
  relist,
} from './tree.ts'
import type { Tree } from './tree.ts'
import { release } from './walk.ts'

/** One in-memory execution of a task: its phases in run mode, or its abort handler. */
export interface Invocation {
  readonly taskId: TaskId
  readonly conversationId: ConversationId
  readonly mode: 'run' | 'abort'
  /** Set on the line by the step that ends it, or when its fiber exits. */
  ended: boolean
  /** Set once the fiber is forked, just after the reservation commit. */
  fiber: Fiber.Fiber<void> | undefined
  /** Completes when the fiber has exited. */
  readonly done: Deferred.Deferred<void>
}

/**
 * The Scheduler's in-memory state. `live` mirrors every committed non-terminal task: the commit
 * listener updates it on the Session line, so code on the line reads exactly the committed state.
 * Every task transition is decided by one callback on the line. No pass visits every live task;
 * the sets below hold each pass's candidates.
 */
export interface State {
  readonly session: SessionService
  /** Committed reads on the line inside a commit, after its table writes. */
  readonly storage: StorageService
  readonly registry: RegistryService
  readonly options: Options
  /** Where invocation fibers run; closing it interrupts them. */
  readonly scope: Scope.Closeable
  readonly live: Map<TaskId, TaskRecord>
  readonly invocations: Map<TaskId, Invocation>
  /** The ownership index around live work. */
  readonly tree: Tree
  /** Live tasks with cancellation intent, the sources of abort cascades (spec §5.4). */
  readonly intent: Set<TaskId>
  /** Live tasks the next reservation pass considers; ones found waiting leave it. */
  readonly runnable: Set<TaskId>
  /** `completing` tasks the next finalize checks: ones that started holding, and holders of ended work. */
  readonly finalizeChecks: Set<TaskId>
  /** `failFast` waiters the next reconcile checks for a failed task in their `on`. */
  readonly failFastChecks: Set<TaskId>
  /**
   * Live `abandonOnRestart` tasks found at open, from an earlier Scheduler. The first reservation
   * pass abort-marks them with reason `restart`, so nothing of them or below them runs again.
   */
  readonly abandoned: Set<TaskId>
  /** Waiting tasks by each task in their `on`. */
  readonly waiters: Map<TaskId, Set<TaskId>>
  /** Terminal receipts awaited by `waitForTask`, by task. */
  readonly receipts: Map<TaskId, Deferred.Deferred<Task.TerminalTask, SchedulerClosed>>
  /** Idle waits by conversation; `undefined` waits for every ownerless conversation. */
  readonly idleWaiters: Map<ConversationId | undefined, Deferred.Deferred<void, SchedulerClosed>>
  /** The definition whose migration failed, per task; retried only for another definition. */
  readonly failedMigrations: Map<TaskId, { readonly task: Erased; readonly message: string }>
  /** Opened to request a reservation pass. */
  readonly wake: Latch.Latch
  /** Opened to request a reconcile pass. */
  readonly reconcileWake: Latch.Latch
  /** Opened to request a sweep of the ownership index. */
  readonly sweepWake: Latch.Latch
  /** Whether the next reconcile withdraws queued inputs below owners with cancellation intent. */
  withdrawPending: boolean
  closing: boolean
}

export const makeState = (
  session: SessionService,
  storage: StorageService,
  registry: RegistryService,
  options: Options,
  scope: Scope.Closeable,
): State => ({
  session,
  storage,
  registry,
  options,
  scope,
  live: new Map(),
  invocations: new Map(),
  tree: makeTree(),
  intent: new Set(),
  runnable: new Set(),
  finalizeChecks: new Set(),
  failFastChecks: new Set(),
  abandoned: new Set(),
  waiters: new Map(),
  receipts: new Map(),
  idleWaiters: new Map(),
  failedMigrations: new Map(),
  wake: Latch.makeUnsafe(false),
  reconcileWake: Latch.makeUnsafe(false),
  sweepWake: Latch.makeUnsafe(false),
  withdrawPending: false,
  closing: false,
})

/** Request a reservation pass. */
export const kick = (state: State): void => {
  state.wake.openUnsafe()
}

/** Whether a record is terminal, typed as the receipt. */
export const isTerminal = (record: TaskRecord): record is Task.TerminalTask =>
  record.state.status === 'terminal'

/** Forget a task that ended; it may free the task above it and its waiters. */
const untrack = (state: State, record: TaskRecord): void => {
  const { id } = record
  const node = nodeOf(record)
  state.live.delete(id)
  state.intent.delete(id)
  state.runnable.delete(id)
  state.finalizeChecks.delete(id)
  state.failFastChecks.delete(id)
  state.failedMigrations.delete(id)
  state.tree.settled.set(id, node)
  // Before anything is forgotten: the walk up to the task it held passes the ended tasks between.
  release(state, node)
  for (const waiter of state.waiters.get(id) ?? []) state.runnable.add(waiter)
  relist(state, id)
  dropIfUnneeded(state, id)
}

/**
 * Bring the state in line with a task's committed record. A task that ended may free the nearest
 * live task above it: one holding its outcome finalizes, an abort-marked one runs its abort
 * handler; its waiters may run.
 */
export const track = (state: State, previous: TaskRecord | undefined, record: TaskRecord): void => {
  const { id } = record
  if (previous?.state.status === 'waiting') {
    for (const on of previous.state.on) deleteFrom(state.waiters, on, id)
  }
  if (isTerminal(record)) {
    untrack(state, record)
    return
  }
  state.live.set(id, record)
  if (previous === undefined) {
    if (!known(state, parentNode(record))) state.tree.unloaded.add(id)
    relist(state, id)
  }
  if (cancellationIntent(record)) state.intent.add(id)
  else state.intent.delete(id)
  if (record.state.status === 'completing') {
    state.runnable.delete(id)
    state.finalizeChecks.add(id)
  } else {
    state.runnable.add(id)
  }
  if (record.state.status === 'waiting') {
    for (const on of record.state.on) addTo(state.waiters, on, id)
  }
}

/** End an invocation: its runtime operations fail from now on, and its task is free. */
export const end = (state: State, invocation: Invocation): void => {
  if (invocation.ended) return
  invocation.ended = true
  if (state.invocations.get(invocation.taskId) === invocation) {
    state.invocations.delete(invocation.taskId)
    if (state.live.has(invocation.taskId)) state.runnable.add(invocation.taskId)
  }
  kick(state)
}

/** A record's fields besides its state and memos. */
const fieldsOf = (record: TaskRecord) => {
  if ('memos' in record) {
    const { state: _state, memos: _memos, ...fields } = record
    return fields
  }
  const { state: _state, ...fields } = record
  return fields
}

/** Replace a live record's state; memos disappear once an outcome is decided. */
export const withState = (record: TaskRecord, next: TaskState): TaskRecord => {
  if (next.status === 'terminal' || next.status === 'completing') {
    return { ...fieldsOf(record), state: next }
  }
  const memos = 'memos' in record ? record.memos : undefined
  return { ...fieldsOf(record), ...(memos === undefined ? {} : { memos }), state: next }
}

/**
 * The record with an abort mark: `restart` for an abandonment; without a reason, a request,
 * which replaces `restart`.
 */
export const withAbortMark = (record: TaskRecord, reason?: 'restart'): TaskRecord => {
  const { abortReason: _reason, ...rest } = record
  return { ...rest, abortRequested: true, ...(reason === undefined ? {} : { abortReason: reason }) }
}

/** Own memo entry only: a name such as `toString` must not resolve to an inherited property. */
export const memoOf = (record: TaskRecord | undefined, name: string): Schema.Json | undefined => {
  const memos = record !== undefined && 'memos' in record ? record.memos : undefined
  return memos !== undefined && Object.hasOwn(memos, name) ? memos[name] : undefined
}

/** The message of a failure, for a `faulted` outcome. */
export const causeMessage = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause)
  if (error instanceof Error) return error.message
  return typeof error === 'string' ? error : Cause.pretty(cause)
}

/** Report a non-fatal failure. */
export const report = (message: string, cause: Cause.Cause<unknown>) =>
  Effect.logError(message, cause)

/** Complete the receipt waits of a task that became terminal. */
export const resolveReceipt = (state: State, record: TaskRecord) =>
  Effect.suspend(() => {
    const receipt = state.receipts.get(record.id)
    if (receipt === undefined || !isTerminal(record)) return Effect.void
    state.receipts.delete(record.id)
    return Deferred.succeed(receipt, record)
  })

/**
 * Run task code and capture how it ended. Task code may fail with anything: a failure of any
 * type faults its task, or blocks a migration.
 */
export const runTaskCode = <A>(code: () => Effect.Effect<A, unknown>) =>
  Effect.exit(Effect.suspend(code))

/** The record with `value` as its memo `name`; settled records hold no memos. */
export const withMemo = (record: TaskRecord, name: string, value: Schema.Json): TaskRecord => {
  const memos = 'memos' in record ? record.memos : undefined
  const state = record.state
  if (state.status === 'terminal' || state.status === 'completing') return record
  return { ...fieldsOf(record), memos: { ...memos, [name]: value }, state }
}

export const closedError = () => new SchedulerClosed({ message: 'Scheduler is closed' })

/** Stop scheduling: no phase starts any more, and every receipt and idle wait fails. */
export const seal = (state: State) =>
  Effect.suspend(() => {
    state.closing = true
    const receipts = [...state.receipts.values()]
    const idle = [...state.idleWaiters.values()]
    state.receipts.clear()
    state.idleWaiters.clear()
    return Effect.andThen(
      Effect.forEach(receipts, (wait) => Deferred.fail(wait, closedError()), { discard: true }),
      Effect.forEach(idle, (wait) => Deferred.fail(wait, closedError()), { discard: true }),
    )
  })

/** A definition with its types erased, as the scheduler runs it. */
export type Erased = Task.Task<Schema.Json, Task.Checkpoint, Schema.Json>

/** Every `Task.Any` was made by `Task.make`, so it is a whole definition. */
export const erase = (task: Task.Any): Erased => cast(task)

/** The definition a snapshot holds for a task kind. */
export const lookup = (snapshot: Snapshot, kind: string): Erased | undefined => {
  const task = snapshot.get(kind)
  return task === undefined ? undefined : erase(task)
}
