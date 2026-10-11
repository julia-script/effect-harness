/** Invocation service contract supplied by TaskScheduler under its reservation gate. */
import * as Context from 'effect/Context'
import type * as DateTime from 'effect/DateTime'
import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Schema from 'effect/Schema'
import type * as Stream from 'effect/Stream'
import type * as Agent from './Agent.js'
import type * as Conversation from './Conversation.js'
import type * as Document from './Document.js'
import type * as Extension from './Extension.js'
import type * as Hook from './Hook.js'
import type * as Record from './Record.js'
import type * as Settings from './Settings.js'
import type * as Task from './Task.js'
import type * as TaskPolicy from './TaskPolicy.js'
import type { Failure } from './TaskError.js'
import type * as Tool from './Tool.js'
import type * as Transaction from './Transaction.js'

/** A phase pins static declarations; live registry leases belong to the scheduler integration. */
export interface Registry {
  readonly tasks: ReadonlyArray<Task.Any>
  readonly extensions: ReadonlyArray<Extension.Any>
  readonly tools: ReadonlyArray<Tool.Any>
  readonly hooks: ReadonlyArray<Hook.Any>
}
export interface ChildOptions {
  readonly conversationId?: Record.ConversationId
  readonly background?: boolean
  readonly restartPolicy?: TaskPolicy.RestartPolicy
}
export interface ConversationOptions {
  readonly agent?: Agent.Change
  readonly fork?: { readonly conversationId: Record.ConversationId; readonly at: Record.EntryId }
}

/**
 * All writes share Session's existing coordination line. The scheduler checks reservation,
 * task identity, abort intent and invocation lifetime inside that line before the callback.
 * Use the supplied Transaction within commit; nested Session calls are not permitted.
 * Returned handles and streams must revoke/terminate when the invocation Scope ends.
 * Child ownership, raw conversation creation, agent/settings and context are native.
 * External conversation request capabilities require a TaskScheduler.Host adapter;
 * missing operations fail with a typed TaskError.
 */
export interface Access {
  /** Native supervised conversations; direct/older standalone services may omit it. */
  readonly subagent?: import('./Subagent.js').Access
  /** Reservation-bound programmatic calls; unsupported on schedulers without native integration. */
  readonly invocation: import('./ToolInvocation.js').Access
  readonly taskId: Record.TaskId
  readonly conversationId: Record.ConversationId
  readonly registry: Registry
  readonly agent: Effect.Effect<Agent.State, Failure>
  /** Sample the live validated host policy; prepared requests keep their own pinned values. */
  readonly settings: Effect.Effect<Settings.Snapshot, Failure>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Conversation.Conversation, Failure>
  readonly commit: <A, E, R>(
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | Failure, R>
  /** Stage typed progress with domain writes in the same gated commit, never a second commit. */
  readonly progress: <T extends Task.Contract>(
    tx: Transaction.Transaction,
    definition: T,
    next: Task.NextState<T>,
  ) => Effect.Effect<void, Failure, Task.ProgressServices<T>>
  /** Own-property lookup; missing memos and prototype-like keys must stay distinguishable. */
  readonly memo: <S extends Schema.Constraint>(
    key: string,
    schema: S,
  ) => Effect.Effect<Option.Option<S['Type']>, Failure, S['DecodingServices']>
  /** First committed writer wins. Return the winning decoded value, with detached JSON storage. */
  readonly memoize: <S extends Schema.Constraint>(
    tx: Transaction.Transaction,
    key: string,
    schema: S,
    value: S['Type'],
  ) => Effect.Effect<S['Type'], Failure, S['EncodingServices'] | S['DecodingServices']>
  readonly createChild: <T extends Task.Any>(
    tx: Transaction.Transaction,
    definition: T,
    input: Task.Input<T>,
    options?: ChildOptions,
  ) => Effect.Effect<Task.Ref<T>, Failure, Task.CreationServices<T>>
  readonly createConversation: (
    tx: Transaction.Transaction,
    options?: ConversationOptions,
  ) => Effect.Effect<Record.Conversation, Failure>
  /** A conversation-owned background task may own its own conversation; direct children cannot. */
  readonly createBackgroundSupervisor: <T extends Task.Any>(
    tx: Transaction.Transaction,
    definition: T,
    input: Task.Input<T>,
    options?: { readonly restartPolicy?: TaskPolicy.RestartPolicy },
  ) => Effect.Effect<Task.Ref<T>, Failure, Task.CreationServices<T>>
  /** Admit registered work in a conversation owned by this invocation's task. */
  readonly createConversationTask: <T extends Task.Any>(
    tx: Transaction.Transaction,
    conversationId: Record.ConversationId,
    definition: T,
    input: Task.Input<T>,
    options?: { readonly background?: boolean; readonly restartPolicy?: TaskPolicy.RestartPolicy },
  ) => Effect.Effect<Task.Ref<T>, Failure, Task.CreationServices<T>>
  /** Terminal wire receipts in caller order, including duplicates. Nonterminal members fail. */
  readonly outcomes: (
    ids: ReadonlyArray<Record.TaskId>,
  ) => Effect.Effect<ReadonlyArray<Record.TaskOutcome>, Failure>
  readonly outcome: <T extends Task.Any>(
    ref: Task.Ref<T>,
  ) => Effect.Effect<
    Option.Option<Task.Outcome<T>>,
    Failure,
    T['success']['DecodingServices'] | T['failure']['DecodingServices']
  >
  /** Interrupting this observer does not cancel the durable child. */
  readonly wait: <T extends Task.Any>(
    ref: Task.Ref<T>,
  ) => Effect.Effect<
    Task.Outcome<T>,
    Failure,
    T['success']['DecodingServices'] | T['failure']['DecodingServices']
  >
  readonly abort: (
    id: Record.TaskId,
    options?: { readonly includeBackground?: boolean },
  ) => Effect.Effect<void, Failure>
  /** Mark owned descendants and wait for actual terminal cleanup; background inclusion is a snapshot. */
  readonly abortOwned: (options?: {
    readonly includeBackground?: boolean
  }) => Effect.Effect<void, Failure>
  readonly entry: (id: Record.EntryId) => Effect.Effect<Option.Option<Record.Entry>, Failure>
  readonly entries: Stream.Stream<Record.Entry, Failure>
  /** Canonical active entries under the conversation's pinned context interpretation. */
  readonly context: Effect.Effect<ReadonlyArray<Record.Entry>, Failure>
  readonly snapshot: <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ) => Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
  readonly snapshotAsOf: <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ) => Effect.Effect<Option.Option<Document.Snapshot<S>>, Failure, S['DecodingServices']>
  readonly watch: <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ) => Stream.Stream<Document.Snapshot<S>, Failure, S['DecodingServices']>
  /** Hook order/phase pinning and cancellation follow the consuming registry's composition policy. */
  readonly dispatchHook: <N extends Hook.Name>(
    event: Hook.Input<N>,
  ) => Effect.Effect<Hook.Output<N>, Failure>
  /** Implement with Effect Clock/DateTime and interruptible sleep, never a host timer. */
  readonly sleepUntil: (instant: DateTime.Utc) => Effect.Effect<void, Failure>
}

/** Native access supplied to one phase or fresh abort invocation by TaskScheduler. */
export class TaskExecution extends Context.Service<TaskExecution, Access>()(
  'effect-harness/TaskExecution',
) {}
export type { Failure } from './TaskError.js'
