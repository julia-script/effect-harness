/** Standalone registered custom-task execution over one Session. */
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Option from 'effect/Option'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import type * as Record from './Record.js'
import type * as RegistryScope from './RegistryScope.js'
import type * as Session from './Session.js'
import type * as Task from './Task.js'
import type * as TaskExecution from './TaskExecution.js'
import type * as TaskPolicy from './TaskPolicy.js'
import type * as TaskResolution from './TaskResolution.js'
import type { Registry } from './Registry.js'
import type { RegistryError } from './RegistryError.js'
import type * as StorageFormat from './StorageFormat.js'
import * as Internal from './internal/TaskScheduler.js'

export type Failure = TaskExecution.Failure | RegistryError | StorageFormat.Failure

/**
 * Supplied host capabilities. Each open runs in a fresh invocation Scope; returned
 * conversation handles must revoke on that Scope's closure. Native ownership, joins,
 * abort cascades and restart policies are always enforced by the scheduler. Agent,
 * settings and context have native defaults; external conversation requests require
 * a supplied adapter and fail with a typed TaskError when unavailable. Hooks dispatch
 * through the pinned native registry; task lifecycle notifications describe invocation
 * settlement, including outcomes held until owned work drains.
 * This standalone scheduler does not replace Harness.run or supervise legacy runs.
 */
export type HostAccess = Partial<
  Pick<
    TaskExecution.Access,
    | 'agent'
    | 'settings'
    | 'conversation'
    | 'createChild'
    | 'createConversation'
    | 'abort'
    | 'abortOwned'
    | 'context'
    | 'dispatchHook'
  >
> & {
  /** Validate ownership, cycles and failFast admission before any wait transition. */
  readonly validateWait?: (
    tx: import('./Transaction.js').Transaction,
    record: Record.Task,
    next: { readonly on: ReadonlyArray<Record.TaskId>; readonly policy: 'failFast' | 'allSettled' },
  ) => Effect.Effect<void, TaskExecution.Failure>
}
export interface Host<R = never> {
  /**
   * Optional additional admission policy, called on Session's line after native
   * ownership reconciliation. Stage changes in this Transaction; nested Session
   * coordination is rejected. False leaves the task blocked.
   */
  readonly reconcile?: (
    tx: import('./Transaction.js').Transaction,
    record: Record.Task,
  ) => Effect.Effect<boolean, TaskExecution.Failure, R>
  /** Capture declared dependencies at scheduler construction; only Scope is fresh here. */
  readonly open: (invocation: {
    readonly taskId: Record.TaskId
    readonly conversationId: Record.ConversationId
    readonly registry: RegistryScope.Snapshot
    readonly mode: 'run' | 'abort'
  }) => Effect.Effect<HostAccess, TaskExecution.Failure, R | Scope.Scope>
}
export interface Inspection {
  readonly record: Record.Task
  readonly resolution: TaskResolution.Inspection<Task.Any>
  readonly invoking: boolean
  readonly scheduling: 'ready' | 'running' | 'waiting' | 'completing' | 'terminal' | 'blocked'
  readonly waitingOn: ReadonlyArray<Record.TaskId>
}
export interface GraphNode {
  readonly id: Record.TaskId
  readonly conversationId: Record.ConversationId
  readonly owner?: Record.TaskId
  readonly kind: string
  readonly version: number
  readonly background: boolean
  readonly abortRequested: boolean
  readonly status: Record.TaskState['status']
  readonly phase?: string
  readonly on: ReadonlyArray<Record.TaskId>
  readonly ownedConversations: ReadonlyArray<Record.ConversationId>
}
export interface Graph {
  readonly tasks: ReadonlyArray<GraphNode>
  readonly conversations: ReadonlyArray<Record.Conversation>
}
export interface GraphFrame {
  readonly snapshot: Graph
  /** Absent only on the atomically attached initial snapshot. Later frames name committed batches. */
  readonly seq?: import('./Sequence.js').Sequence
}
export interface Service {
  /** Validate the selected registered contract, create once, then enable scheduling. */
  readonly start: <T extends Task.Any>(
    definition: T,
    input: Task.Input<T>,
    options: {
      readonly conversationId: Record.ConversationId
      readonly restartPolicy?: TaskPolicy.RestartPolicy
      /** Conversation-owned background supervisors are independent of ordinary parent drains. */
      readonly background?: boolean
    },
  ) => Effect.Effect<Task.Ref<T>, Failure>
  /** Recover stranded running records and wake dispatch; existing invocations stay reserved. */
  readonly resume: Effect.Effect<void, Failure>
  /** Read-only, lease-pinned inspection. Does not initialize or attempt migrations. */
  readonly inspect: (id: Record.TaskId) => Effect.Effect<Inspection, Failure>
  readonly outcome: <T extends Task.Any>(
    ref: Task.Ref<T>,
  ) => Effect.Effect<
    Option.Option<Task.Outcome<T>>,
    Failure,
    T['success']['DecodingServices'] | T['failure']['DecodingServices']
  >
  /** Attach before checking; interruption releases this observer and never cancels durable work. */
  readonly wait: <T extends Task.Any>(
    ref: Task.Ref<T>,
  ) => Effect.Effect<
    Task.Outcome<T>,
    Failure,
    T['success']['DecodingServices'] | T['failure']['DecodingServices']
  >
  /** Latest health state, including persistence failures. Resume retries reconciliation. */
  readonly failure: Effect.Effect<Option.Option<Failure>>
  /** Slow observers coalesce state notifications and never delay Session commits. */
  readonly changes: Stream.Stream<void>
  readonly waitForTask: (id: Record.TaskId) => Effect.Effect<Record.Task, Failure>
  readonly outcomes: (
    ids: ReadonlyArray<Record.TaskId>,
  ) => Effect.Effect<ReadonlyArray<Record.TaskOutcome>, Failure>
  /** Capture descendants in the marking commit, then await that exact set's cleanup. */
  readonly abort: (
    id: Record.TaskId,
    options?: { readonly includeBackground?: boolean },
  ) => Effect.Effect<void, Failure>
  readonly abortConversation: (
    id: Record.ConversationId,
    options?: { readonly includeBackground?: boolean },
  ) => Effect.Effect<void, Failure>
  readonly graph: Effect.Effect<Graph, Failure>
  /** Full snapshots in commit order. Slow consumers fail with SessionError overflow and must reattach. */
  readonly watchGraph: Stream.Stream<GraphFrame, Failure>
}

/**
 * One scoped scheduler per Session. Construction captures Registry and optional typed host
 * dependencies, attaches commit/registry wakes, and performs no task or format writes.
 * start/resume/wait require the Session's exact supported StorageFormat capability.
 * Registry Layers own captured resources; invocation leases keep them alive across reloads.
 */
export const make = <R = never>(
  session: Session.Session,
  options: { readonly host?: Host<R> } = {},
): Effect.Effect<Service, Failure, Registry | Scope.Scope | R> => Internal.make(session, options)

/** Scoped standalone scheduler service; each Session accepts one active scheduler. */
export class TaskScheduler extends Context.Service<TaskScheduler, Service>()(
  'effect-harness/TaskScheduler',
) {}

export const layer = <R = never>(
  session: Session.Session,
  options: { readonly host?: Host<R> } = {},
): Layer.Layer<TaskScheduler, Failure, Registry | R> =>
  Layer.effect(TaskScheduler, make(session, options))
