/**
 * Structured task ownership, joins and completion holds.
 */
import * as Arr from 'effect/Array'
import * as Outcome from './Outcome.ts'
import type * as Identity from '../Identity.ts'
import * as Cause from 'effect/Cause'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Ownership from '../Ownership.ts'
import type * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import * as Cancellation from './Cancellation.ts'
import type { StorageError } from '../StorageError.ts'
import { ExecutionError, InvalidState } from './ExecutionError.ts'

const invalid = (message: string, cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidState({ message, ...(cause === undefined ? {} : { cause }) }),
  })

/**
 * Encodes a native Workflow payload and computes its durable execution binding.
 *
 * **Details**
 *
 * Uses the declaration’s payload schema and idempotency key. Returns metadata for a task
 * record; this operation neither creates the task nor starts execution.
 *
 * @category combinators
 */
export const domainBinding = Effect.fnUntraced(function* <
  N extends string,
  P extends Workflow.AnyStructSchema,
  A extends Schema.Top,
  E extends Schema.Top,
>(
  workflow: Workflow.Workflow<N, P, A, E>,
  payload: P['Type'],
): Effect.fn.Return<Ownership.Binding, Schema.SchemaError, P['EncodingServices']> {
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(workflow.payloadSchema))(payload)
  const executionId = yield* workflow.executionId(payload)
  return Ownership.Binding.make({ workflow: workflow._tag, executionId, payload: encoded })
})

/**
 * Binds a task to its replayable native Workflow execution.
 *
 * **Details**
 *
 * The caller prefetches the task before any transaction table writes.
 *
 * @category combinators
 */
export const bind = Effect.fnUntraced(function* <
  N extends string,
  P extends Workflow.AnyStructSchema,
  A extends Schema.Top,
  E extends Schema.Top,
>(
  tx: Session.Transaction,
  task: Record.Task,
  workflow: Workflow.Workflow<N, P, A, E>,
  payload: P['Type'],
): Effect.fn.Return<Ownership.Binding, StorageError | Schema.SchemaError, P['EncodingServices']> {
  const binding = yield* domainBinding(workflow, payload)
  yield* tx.write({ _tag: 'task', type: 'task', value: { ...task, input: binding } })
  return binding
})

/**
 * Outcome classification is shared by holds and fail-fast joins.
 *
 * @category guards
 */
export const isFailed = Outcome.failed

const heldFailure = (task: Record.Task | undefined) =>
  task !== undefined &&
  (task.state.status === 'completing' || task.state.status === 'terminal') &&
  isFailed(task.state.outcome)

/**
 * A captured domain callback dispatches pending submissions through ordinary native workflows.
 *
 * @category services
 */
export class DrainConversations extends Context.Service<
  DrainConversations,
  {
    readonly drain: (
      session: Session.Service,
      owner: Record.Task,
      conversation: Record.Conversation,
      submissions: ReadonlyArray<Record.Submission>,
      sessionId: Identity.SessionId,
    ) => Effect.Effect<void, ExecutionError | import('../StorageError.ts').StorageError>
  }
>()('@effect-harness/durable/workflow/Structured/DrainConversations') {}
/**
 * Provides the callback used to drain pending work in owned conversations.
 *
 * **Details**
 *
 * The callback dispatches and joins ordinary native Workflow executions; it does not replace
 * the WorkflowEngine.
 *
 * @category layers
 */
export const layerDrainConversations = (
  drain: DrainConversations['Service']['drain'],
): Layer.Layer<DrainConversations> =>
  Layer.succeed(DrainConversations, DrainConversations.of({ drain }))

const pendingConversations = (graph: Ownership.Graph, reached: Option.Option<Ownership.Reached>) =>
  Arr.flatMap(
    reached.pipe(
      Option.map((value) => value.conversations),
      Option.getOrElse(() => []),
    ),
    (conversation) => {
      const submissions = Arr.filter(
        graph.submissions ?? [],
        (submission) =>
          submission.conversationId === conversation.id &&
          (submission.status === 'queued' || submission.status === 'placed'),
      )
      return Arr.isReadonlyArrayEmpty(submissions) ? [] : [{ conversation, submissions }]
    },
  )

/**
 * Persists a held outcome.
 *
 * **Details**
 *
 * Pass a graph collected before any table writes when composing this with entry/document mutations in an executor's atomic commit.
 *
 * @category combinators
 */
export const hold = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  task: Record.Task,
  outcome: Record.Json,
  graph: Ownership.Graph,
): Effect.fn.Return<Record.Task, StorageError> {
  if (task.state.status === 'terminal' || task.state.status === 'completing') return task
  const reached = Ownership.reach(graph, { _tag: 'task', kind: 'task', id: task.id })
  const children = reached.pipe(
    Option.map((value) => Arr.filter(value.tasks, (child) => child.id !== task.id)),
    Option.getOrElse(() => []),
  )
  const pending = pendingConversations(graph, reached)
  const value: Record.Task = {
    ...task,
    state: {
      status:
        Arr.isReadonlyArrayEmpty(children) && Arr.isReadonlyArrayEmpty(pending)
          ? 'terminal'
          : 'completing',
      outcome,
    },
  }
  yield* tx.write({ _tag: 'task', type: 'task', value })
  return value
})

const execute = Effect.fnUntraced(function* (
  session: Session.Service,
  task: Record.Task,
  sessionId?: Identity.SessionId,
): Effect.fn.Return<
  void,
  ExecutionError | StorageError,
  Ownership.Declarations | Cancellation.Cancellation | WorkflowEngine.WorkflowEngine
> {
  if (task.state.status === 'terminal') return
  const binding = yield* Schema.decodeUnknownEffect(Ownership.Binding)(task.input).pipe(
    Effect.mapError((cause) => invalid(`Task ${task.id} has no native Workflow binding`, cause)),
  )
  const declarations = yield* Ownership.Declarations
  if (Option.isNone(declarations.get(binding.workflow))) {
    if (!task.abortRequested) {
      // Missing code is a recoverable registration boundary. Preserve the work
      // and let the ordinary native parent resume once its declaration returns.
      const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
      if (Option.isSome(instance)) return yield* Workflow.suspend(instance.value)
      return yield* invalid(
        `Workflow ${binding.workflow} is not declared; task ${task.id} remains blocked`,
      )
    }
    yield* complete(
      session,
      task.id,
      Outcome.Orphaned.make({
        status: 'orphaned',
        reason: `Workflow ${binding.workflow} is not declared`,
      }),
      sessionId,
    )
    return
  }
  yield* Ownership.execute(binding).pipe(Effect.ignore)
})

/**
 * Joins owned native executions in input order.
 *
 * **Details**
 *
 * allSettled is the default. failFast marks the listed owned siblings when a failure is
 * observed, including a held failure before descendants finish draining.
 *
 * **Gotchas**
 *
 * A native result without a terminal domain projection is rejected. Do not return from a
 * custom owned child without settling its domain task.
 *
 * @category combinators
 */
export const join = Effect.fnUntraced(function* (
  session: Session.Service,
  ownerId: Record.TaskId,
  ids: ReadonlyArray<Record.TaskId>,
  policy: 'failFast' | 'allSettled' = 'allSettled',
): Effect.fn.Return<
  Array<Record.Json>,
  StorageError | ExecutionError,
  Cancellation.Cancellation | WorkflowEngine.WorkflowEngine | Ownership.Declarations
> {
  const state = yield* session.committed
  const owner = Arr.findFirst(state.tasks, (task) => task.id === ownerId)
  if (Option.isNone(owner)) return yield* invalid('Join owner is absent')
  const ancestors = new Set<Record.TaskId>([ownerId])
  let ancestor =
    owner.value.owner ??
    Option.getOrUndefined(
      Option.flatMap(
        Arr.findFirst(
          state.conversations,
          (conversation) => conversation.id === owner.value.conversationId,
        ),
        (conversation) => Option.fromUndefinedOr(conversation.owner?.taskId),
      ),
    )
  while (ancestor !== undefined && !ancestors.has(ancestor)) {
    ancestors.add(ancestor)
    const parent = Arr.findFirst(state.tasks, (task) => task.id === ancestor)
    if (Option.isNone(parent)) break
    ancestor =
      parent.value.owner ??
      Option.getOrUndefined(
        Option.flatMap(
          Arr.findFirst(
            state.conversations,
            (conversation) => conversation.id === parent.value.conversationId,
          ),
          (conversation) => Option.fromUndefinedOr(conversation.owner?.taskId),
        ),
      )
  }
  const tasks: Array<Record.Task> = []
  for (const id of ids) {
    const task = Arr.findFirst(state.tasks, (task) => task.id === id)
    if (Option.isNone(task)) return yield* invalid(`Awaited task ${id} is absent`)
    if (ancestors.has(id)) return yield* invalid('A task cannot await itself or its owner')
    if (policy === 'failFast' && task.value.owner !== ownerId)
      return yield* invalid('Fail-fast requires directly owned tasks')
    tasks.push(task.value)
  }
  yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const currentOption = yield* tx.task(ownerId)
      if (
        Option.isNone(currentOption) ||
        currentOption.value.abortRequested ||
        currentOption.value.state.status === 'terminal' ||
        currentOption.value.state.status === 'completing'
      )
        return yield* invalid('Join requires a live owner')
      const current = currentOption.value
      yield* tx.write({
        _tag: 'task',
        type: 'task',
        value: {
          ...current,
          state: { ...current.state, status: 'waiting', on: ids, policy },
        },
      })
    }),
  )
  const marked = yield* Ref.make(false)
  const failFast = Effect.gen(function* () {
    if ((yield* Ref.get(marked)) || policy !== 'failFast') return
    const latest = yield* session.committed
    if (
      !ids.some((id) =>
        Option.exists(
          Arr.findFirst(latest.tasks, (task) => task.id === id),
          heldFailure,
        ),
      )
    )
      return
    if (yield* Ref.getAndSet(marked, true)) return
    const current = yield* Effect.serviceOption(Ownership.Current)
    if (Option.isNone(current)) return yield* invalid('Fail-fast requires a scoped owner identity')
    for (const id of ids) {
      const task = Arr.findFirst(latest.tasks, (task) => task.id === id)
      if (Option.isNone(task) || task.value.state.status === 'terminal' || heldFailure(task.value))
        continue
      const reached = yield* Cancellation.mark(session, { _tag: 'task', kind: 'task', id })
      yield* Cancellation.cancel(current.value.sessionId, reached)
    }
  })
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const monitor =
        policy === 'failFast'
          ? yield* Effect.forever(failFast.pipe(Effect.andThen(Effect.sleep('20 millis')))).pipe(
              Effect.forkScoped,
            )
          : undefined
      yield* failFast
      const awaiting = Effect.forEach(tasks, (task) => execute(session, task), {
        // P5-explicit-concurrency-option: native frontier members may await a later
        // member; all joins must start together to avoid stranding that dependency.
        concurrency: 'unbounded',
        discard: true,
      })
      if (monitor === undefined) yield* awaiting
      else yield* Effect.raceFirst(awaiting, Fiber.join(monitor))
      yield* failFast
      const latest = yield* session.committed
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const currentOption = yield* tx.task(ownerId)
          if (Option.isNone(currentOption) || currentOption.value.state.status !== 'waiting') return
          const current = currentOption.value
          yield* tx.write({
            _tag: 'task',
            type: 'task',
            value: {
              ...current,
              state: {
                status: 'running',
                ...(current.state.checkpoint === undefined
                  ? {}
                  : { checkpoint: current.state.checkpoint }),
              },
            },
          })
        }),
      )
      return yield* Effect.forEach(ids, (id) => {
        const task = Arr.findFirst(latest.tasks, (task) => task.id === id)
        return Option.isSome(task) && task.value.state.status === 'terminal'
          ? Effect.succeed(task.value.state.outcome ?? null)
          : Effect.fail(
              invalid(`Native execution ${id} ended before its domain projection settled`),
            )
      })
    }),
  )
})

/**
 * Joins owned native executions and atomically releases a completing outcome.
 *
 * **Details**
 *
 * Re-reading each committed graph admits ordinary new work in owned conversations during the hold; transaction validation seals the final drain race.
 *
 * @category combinators
 */
export const drain = Effect.fnUntraced(function* (
  session: Session.Service,
  taskId: Record.TaskId,
  sessionId?: Identity.SessionId,
): Effect.fn.Return<
  Record.Json,
  ExecutionError | StorageError,
  Ownership.Declarations | Cancellation.Cancellation | WorkflowEngine.WorkflowEngine
> {
  while (true) {
    const state = yield* session.committed
    const task = Arr.findFirst(state.tasks, (task) => task.id === taskId)
    if (Option.isNone(task)) return yield* invalid('Completing task is absent')
    if (task.value.state.status === 'terminal') return task.value.state.outcome ?? null
    if (task.value.state.status !== 'completing') return yield* invalid('Task has no held outcome')
    const reached = Ownership.reach(state, { _tag: 'task', kind: 'task', id: taskId })
    const children = reached.pipe(
      Option.map((value) => Arr.filter(value.tasks, (child) => child.id !== taskId)),
      Option.getOrElse(() => []),
    )
    const pending = pendingConversations(state, reached)
    if (Arr.isReadonlyArrayNonEmpty(pending)) {
      const callback = yield* Effect.serviceOption(DrainConversations)
      if (Option.isNone(callback))
        return yield* invalid('Owned conversation submissions require a native drain callback')
      const currentIdentity = yield* Effect.serviceOption(Ownership.Current)
      const identity =
        sessionId ??
        Option.getOrUndefined(Option.map(currentIdentity, (current) => current.sessionId))
      if (identity === undefined)
        return yield* invalid('Owned conversation drain requires a session identity')
      yield* Effect.forEach(
        pending,
        ({ conversation, submissions }) =>
          callback.value.drain(session, task.value, conversation, submissions, identity),
        {
          // P5-explicit-concurrency-option: native frontier members may await a later
          // member; all joins must start together to avoid stranding that dependency.
          concurrency: 'unbounded',
          discard: true,
        },
      )
      const refreshed = yield* session.committed
      const dispatched = new Set(
        Arr.flatMap(pending, ({ submissions }) => submissions.map((submission) => submission.id)),
      )
      if (
        refreshed.submissions.some(
          (submission) =>
            dispatched.has(submission.id) &&
            (submission.status === 'queued' || submission.status === 'placed'),
        )
      )
        return yield* invalid('Owned conversation drain returned before its submissions settled')
    }
    if (Arr.isReadonlyArrayNonEmpty(children)) {
      if (isFailed(task.value.state.outcome)) {
        const currentIdentity = yield* Effect.serviceOption(Ownership.Current)
        const identity =
          sessionId ??
          Option.getOrUndefined(Option.map(currentIdentity, (current) => current.sessionId))
        if (identity === undefined)
          return yield* invalid('Cancellation requires a session identity')
        for (const child of children) {
          const marked = yield* Cancellation.mark(session, {
            _tag: 'task',
            kind: 'task',
            id: child.id,
          })
          yield* Cancellation.cancel(identity, marked)
        }
      }
      yield* Effect.forEach(
        children,
        Effect.fnUntraced(function* (child) {
          yield* execute(session, child, sessionId)
          const settled = Arr.findFirst(
            (yield* session.committed).tasks,
            (task) => task.id === child.id,
          )
          if (Option.isNone(settled) || settled.value.state.status !== 'terminal')
            return yield* invalid(
              `Native execution ${child.id} ended before its domain projection settled`,
            )
        }),
        {
          // P5-explicit-concurrency-option: native frontier members may await a later
          // member; all joins must start together to avoid stranding that dependency.
          concurrency: 'unbounded',
          discard: true,
        },
      )
      continue
    }
    const result = yield* session.transaction(
      Effect.fnUntraced(function* (tx) {
        const graph = yield* Ownership.readGraph(tx)
        const current = Arr.findFirst(graph.tasks, (item) => item.id === taskId)
        if (Option.isNone(current)) return yield* invalid('Completing task is absent')
        if (current.value.state.status === 'terminal')
          return { done: true, outcome: current.value.state.outcome ?? null }
        const remaining = Ownership.reach(graph, { _tag: 'task', kind: 'task', id: taskId })
        const pending =
          Option.exists(remaining, (value) => value.tasks.some((item) => item.id !== taskId)) ||
          Arr.isReadonlyArrayNonEmpty(pendingConversations(graph, remaining))
        if (pending) return { done: false, outcome: null }
        yield* tx.write({
          _tag: 'task',
          type: 'task',
          value: {
            ...current.value,
            state: { status: 'terminal', outcome: current.value.state.outcome ?? null },
          },
        })
        return { done: true, outcome: current.value.state.outcome ?? null }
      }),
    )
    if (result.done) return result.outcome
  }
})

/**
 * Commits a result and releases it only after all ordinary owned native work drains.
 *
 * @category combinators
 */
export const complete = Effect.fnUntraced(function* (
  session: Session.Service,
  taskId: Record.TaskId,
  outcome: Record.Json,
  sessionId?: Identity.SessionId,
): Effect.fn.Return<
  Record.Json,
  ExecutionError | StorageError,
  Ownership.Declarations | Cancellation.Cancellation | WorkflowEngine.WorkflowEngine
> {
  yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const graph = yield* Ownership.readGraph(tx)
      const task = Arr.findFirst(graph.tasks, (task) => task.id === taskId)
      if (Option.isNone(task)) return yield* invalid('Completing task is absent')
      return yield* hold(tx, task.value, outcome, graph)
    }),
  )
  return yield* drain(session, taskId, sessionId)
})

/**
 * Reserves an owned child task with a replayable native Workflow binding.
 *
 * **Details**
 *
 * The caller supplies a stable key belonging to its native Activity. The receipt saves the
 * child identity and binding; execute the native Workflow separately.
 *
 * **Gotchas**
 *
 * Requires Ownership.Current. A completing, terminal or abort-marked owner cannot admit new
 * direct children. The child must commit its domain terminal projection before joins can
 * succeed.
 *
 * @category combinators
 */
export const child = Effect.fnUntraced(function* <
  N extends string,
  P extends Workflow.AnyStructSchema,
  A extends Schema.Top,
  E extends Schema.Top,
>(
  workflow: Workflow.Workflow<N, P, A, E>,
  payload: (taskId: Record.TaskId) => P['Type'],
  key: string,
): Effect.fn.Return<
  { id: Record.TaskId; binding: Ownership.Binding },
  StorageError | ExecutionError | Schema.SchemaError,
  Ownership.Current | P['EncodingServices']
> {
  const current = yield* Ownership.Current
  yield* current.check
  return yield* current.session.transaction(
    Effect.fnUntraced(function* (tx) {
      yield* current.check
      const ownerOption = yield* tx.task(current.taskId)
      if (
        Option.isNone(ownerOption) ||
        ownerOption.value.abortRequested ||
        ownerOption.value.state.status === 'completing' ||
        ownerOption.value.state.status === 'terminal'
      )
        return yield* invalid('Child creation requires a live owner')

      const projection = {
        conversationId: current.conversationId,
        kind: workflow._tag,
        version: 1,
        input: null,
        owner: current.taskId,
        background: false,
        abortRequested: false,
        state: { status: 'pending' as const },
      }
      const id = yield* tx.createTask(projection)
      const binding = yield* bind(tx, { ...projection, id }, workflow, payload(id))
      return { id, binding }
    }),
    { key: `ownership/child/${current.taskId}/${key}` },
  )
})

/**
 * Evaluates a domain body inside an ordinary Workflow.toLayer handler.
 *
 * **Details**
 *
 * Native suspension/abandonment remains interruption so the engine can replay; completed bodies, typed failures and defects acquire an owned completing hold.
 *
 * @category combinators
 */
export const evaluate = Effect.fnUntraced(function* <E, R>(
  identity: Ownership.Identity,
  session: Session.Service,
  body: Effect.Effect<Record.Json, E, R>,
): Effect.fn.Return<
  Record.Json,
  E | ExecutionError | StorageError,
  | Exclude<R, Ownership.Current>
  | Ownership.Declarations
  | Cancellation.Cancellation
  | WorkflowEngine.WorkflowEngine
> {
  const exit = yield* Cancellation.run(identity, session, body).pipe(Effect.exit)
  const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
  if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
    if (Option.isSome(instance) && (instance.value.suspended || instance.value.abandoned))
      return yield* Effect.failCause(exit.cause)
    const task = Arr.findFirst(
      (yield* session.committed).tasks,
      (task) => task.id === identity.taskId,
    )
    if (!Option.exists(task, (found) => found.abortRequested))
      return yield* Effect.failCause(exit.cause)
  }
  let outcome: Record.Json
  if (Exit.isSuccess(exit))
    outcome = yield* Schema.decodeEffect(Outcome.Completed)({
      status: 'completed',
      result: exit.value,
    }).pipe(Effect.mapError((cause) => invalid('Invalid structured outcome', cause)))
  else {
    const error = Cause.squash(exit.cause)
    let status: (typeof Outcome.Failed.Type)['status'] = Cause.hasDies(exit.cause)
      ? 'faulted'
      : 'failed'
    if (error instanceof ExecutionError && error.reason._tag === 'Aborted') status = 'aborted'
    outcome = Outcome.Failed.make({
      status,
      error: { message: error instanceof Error ? error.message : String(error) },
    })
  }
  return yield* complete(session, identity.taskId, outcome, identity.sessionId)
})
