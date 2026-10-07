import * as Cause from 'effect/Cause'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as Ownership from '../Ownership.ts'
import * as Record from '../Record.ts'
import type * as Session from '../Session.ts'
import * as Cancellation from './Cancellation.ts'
import type { StorageError } from '../StorageError.ts'
import { ExecutionError, InvalidState } from './ExecutionError.ts'

const invalid = (message: string, cause?: unknown) =>
  new ExecutionError({
    reason: new InvalidState({ message, ...(cause === undefined ? {} : { cause }) }),
  })

/** Attach native Workflow identity to an already created domain task in the same transaction. */
export const domainBinding = Effect.fnUntraced(function* <
  N extends string,
  P extends Workflow.AnyStructSchema,
  A extends Schema.Top,
  E extends Schema.Top,
>(workflow: Workflow.Workflow<N, P, A, E>, payload: P['Type']) {
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(workflow.payloadSchema))(payload)
  const executionId = yield* workflow.executionId(payload)
  return Ownership.Binding.make({ workflow: workflow._tag, executionId, payload: encoded })
})

/** The caller prefetches the task before any transaction table writes. */
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
) {
  const binding = yield* domainBinding(workflow, payload)
  yield* tx.write({ type: 'task', value: { ...task, input: binding } })
  return binding
})

/** Outcome classification is shared by holds and fail-fast joins. */
export const failed = (outcome: Record.Json | undefined): boolean => {
  if (outcome === null || typeof outcome !== 'object' || Array.isArray(outcome)) return false
  const receipt = Reflect.get(outcome, 'receipt')
  const status =
    Reflect.get(outcome, 'status') ??
    (receipt !== null && typeof receipt === 'object' ? Reflect.get(receipt, 'status') : undefined)
  return (
    status === 'failed' || status === 'faulted' || status === 'orphaned' || status === 'aborted'
  )
}

const heldFailure = (task: Record.Task | undefined) =>
  task !== undefined &&
  (task.state.status === 'completing' || task.state.status === 'terminal') &&
  failed(task.state.outcome)

/** A captured domain callback dispatches pending submissions through ordinary native workflows. */
export class DrainConversations extends Context.Service<
  DrainConversations,
  {
    readonly drain: (
      session: Session.Service,
      owner: Record.Task,
      conversation: Record.Conversation,
      submissions: ReadonlyArray<Record.Submission>,
      sessionId: string,
    ) => Effect.Effect<void, ExecutionError | import('../StorageError.ts').StorageError>
  }
>()('@effect-harness/durable/Structured/DrainConversations') {}
export const layerDrainConversations = (drain: DrainConversations['Service']['drain']) =>
  Layer.succeed(DrainConversations, DrainConversations.of({ drain }))

const pendingConversations = (graph: Ownership.Graph, reached: Ownership.Reached | undefined) =>
  (reached?.conversations ?? []).flatMap((conversation) => {
    const submissions =
      graph.submissions?.filter(
        (submission) =>
          submission.conversationId === conversation.id &&
          (submission.status === 'queued' || submission.status === 'placed'),
      ) ?? []
    return submissions.length === 0 ? [] : [{ conversation, submissions }]
  })

/**
 * Persist a held outcome. Pass a graph collected before any table writes when
 * composing this with entry/document mutations in an executor's atomic commit.
 */
export const hold = Effect.fnUntraced(function* (
  tx: Session.Transaction,
  task: Record.Task,
  outcome: Record.Json,
  graph: Ownership.Graph,
) {
  if (task.state.status === 'terminal' || task.state.status === 'completing') return task
  const reached = Ownership.reach(graph, { kind: 'task', id: task.id })
  const children = reached?.tasks.filter((child) => child.id !== task.id) ?? []
  const pending = pendingConversations(graph, reached)
  const value: Record.Task = {
    ...task,
    state: {
      status: children.length === 0 && pending.length === 0 ? 'terminal' : 'completing',
      outcome,
    },
  }
  yield* tx.write({ type: 'task', value })
  return value
})

const execute = Effect.fnUntraced(function* (
  session: Session.Service,
  task: Record.Task,
  sessionId?: string,
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
  if (declarations.get(binding.workflow) === undefined) {
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
      { status: 'orphaned', reason: `Workflow ${binding.workflow} is not declared` },
      sessionId,
    )
    return
  }
  yield* Ownership.execute(binding).pipe(Effect.catch(() => Effect.void))
})

/**
 * Native joins retain input order. Fail-fast marks only listed owned siblings;
 * held failures trigger the same mark before their descendants finish draining.
 */
export const join = Effect.fnUntraced(function* (
  session: Session.Service,
  ownerId: Record.TaskId,
  ids: ReadonlyArray<Record.TaskId>,
  policy: 'failFast' | 'allSettled' = 'allSettled',
) {
  const state = yield* session.committed
  const owner = state.tasks.find((task) => task.id === ownerId)
  if (owner === undefined) return yield* invalid('Join owner is absent')
  const ancestors = new Set<Record.TaskId>([ownerId])
  let ancestor =
    owner.owner ??
    state.conversations.find((conversation) => conversation.id === owner.conversationId)?.owner
      ?.taskId
  while (ancestor !== undefined && !ancestors.has(ancestor)) {
    ancestors.add(ancestor)
    const parent = state.tasks.find((task) => task.id === ancestor)
    if (parent === undefined) break
    ancestor =
      parent.owner ??
      state.conversations.find((conversation) => conversation.id === parent.conversationId)?.owner
        ?.taskId
  }
  const tasks: Record.Task[] = []
  for (const id of ids) {
    const task = state.tasks.find((task) => task.id === id)
    if (task === undefined) return yield* invalid(`Awaited task ${id} is absent`)
    if (ancestors.has(id)) return yield* invalid('A task cannot await itself or its owner')
    if (policy === 'failFast' && task.owner !== ownerId)
      return yield* invalid('Fail-fast requires directly owned tasks')
    tasks.push(task)
  }
  yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const current = yield* tx.task(ownerId)
      if (
        current === undefined ||
        current.abortRequested ||
        current.state.status === 'terminal' ||
        current.state.status === 'completing'
      )
        return yield* invalid('Join requires a live owner')
      yield* tx.write({
        type: 'task',
        value: {
          ...current,
          state: { ...current.state, status: 'waiting', on: ids, policy },
        },
      })
    }),
  )
  let marked = false
  const failFast = Effect.gen(function* () {
    if (marked || policy !== 'failFast') return
    const latest = yield* session.committed
    if (!ids.some((id) => heldFailure(latest.tasks.find((task) => task.id === id)))) return
    marked = true
    const current = yield* Effect.serviceOption(Ownership.Current)
    if (Option.isNone(current)) return yield* invalid('Fail-fast requires a scoped owner identity')
    for (const id of ids) {
      const task = latest.tasks.find((task) => task.id === id)
      if (task === undefined || task.state.status === 'terminal' || heldFailure(task)) continue
      const reached = yield* Cancellation.mark(session, { kind: 'task', id })
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
        concurrency: 'unbounded',
        discard: true,
      })
      if (monitor === undefined) yield* awaiting
      else yield* Effect.raceFirst(awaiting, Fiber.join(monitor))
      yield* failFast
      const latest = yield* session.committed
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const current = yield* tx.task(ownerId)
          if (current === undefined || current.state.status !== 'waiting') return
          yield* tx.write({
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
        const task = latest.tasks.find((task) => task.id === id)
        return task?.state.status === 'terminal'
          ? Effect.succeed(task.state.outcome ?? null)
          : Effect.fail(
              invalid(`Native execution ${id} ended before its domain projection settled`),
            )
      })
    }),
  )
})

/**
 * Join owned native executions and atomically release a completing outcome.
 * Re-reading each committed graph admits ordinary new work in owned conversations
 * during the hold; transaction validation seals the final drain race.
 */
export const drain = Effect.fnUntraced(function* (
  session: Session.Service,
  taskId: Record.TaskId,
  sessionId?: string,
): Effect.fn.Return<
  Record.Json,
  ExecutionError | StorageError,
  Ownership.Declarations | Cancellation.Cancellation | WorkflowEngine.WorkflowEngine
> {
  while (true) {
    const state = yield* session.committed
    const task = state.tasks.find((task) => task.id === taskId)
    if (task === undefined) return yield* invalid('Completing task is absent')
    if (task.state.status === 'terminal') return task.state.outcome ?? null
    if (task.state.status !== 'completing') return yield* invalid('Task has no held outcome')
    const reached = Ownership.reach(state, { kind: 'task', id: taskId })
    const children = reached?.tasks.filter((child) => child.id !== taskId) ?? []
    const pending = pendingConversations(state, reached)
    if (pending.length > 0) {
      const callback = yield* Effect.serviceOption(DrainConversations)
      if (Option.isNone(callback))
        return yield* invalid('Owned conversation submissions require a native drain callback')
      const currentIdentity = yield* Effect.serviceOption(Ownership.Current)
      const identity =
        sessionId ?? (Option.isSome(currentIdentity) ? currentIdentity.value.sessionId : undefined)
      if (identity === undefined)
        return yield* invalid('Owned conversation drain requires a session identity')
      yield* Effect.forEach(
        pending,
        ({ conversation, submissions }) =>
          callback.value.drain(session, task, conversation, submissions, identity),
        { concurrency: 'unbounded', discard: true },
      )
      const refreshed = yield* session.committed
      const dispatched = new Set(
        pending.flatMap(({ submissions }) => submissions.map((submission) => submission.id)),
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
    if (children.length > 0) {
      if (failed(task.state.outcome)) {
        const currentIdentity = yield* Effect.serviceOption(Ownership.Current)
        const identity =
          sessionId ??
          (Option.isSome(currentIdentity) ? currentIdentity.value.sessionId : undefined)
        if (identity === undefined)
          return yield* invalid('Cancellation requires a session identity')
        for (const child of children) {
          const marked = yield* Cancellation.mark(session, { kind: 'task', id: child.id })
          yield* Cancellation.cancel(identity, marked)
        }
      }
      yield* Effect.forEach(
        children,
        (child) =>
          Effect.gen(function* () {
            yield* execute(session, child, sessionId)
            const settled = (yield* session.committed).tasks.find((task) => task.id === child.id)
            if (settled?.state.status !== 'terminal')
              return yield* invalid(
                `Native execution ${child.id} ended before its domain projection settled`,
              )
          }),
        {
          concurrency: 'unbounded',
          discard: true,
        },
      )
      continue
    }
    const result = yield* session.transaction(
      Effect.fnUntraced(function* (tx) {
        const graph = yield* Ownership.readGraph(tx)
        const current = graph.tasks.find((item) => item.id === taskId)
        if (current === undefined) return yield* invalid('Completing task is absent')
        if (current.state.status === 'terminal')
          return { done: true, outcome: current.state.outcome ?? null }
        const remaining = Ownership.reach(graph, { kind: 'task', id: taskId })
        const pending =
          remaining?.tasks.some((item) => item.id !== taskId) ||
          pendingConversations(graph, remaining).length > 0
        if (pending) return { done: false, outcome: null }
        yield* tx.write({
          type: 'task',
          value: {
            ...current,
            state: { status: 'terminal', outcome: current.state.outcome ?? null },
          },
        })
        return { done: true, outcome: current.state.outcome ?? null }
      }),
    )
    if (result.done) return result.outcome
  }
})

/** Commit a result and release it only after all ordinary owned native work drains. */
export const complete = Effect.fnUntraced(function* (
  session: Session.Service,
  taskId: Record.TaskId,
  outcome: Record.Json,
  sessionId?: string,
): Effect.fn.Return<
  Record.Json,
  ExecutionError | StorageError,
  Ownership.Declarations | Cancellation.Cancellation | WorkflowEngine.WorkflowEngine
> {
  yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const graph = yield* Ownership.readGraph(tx)
      const task = graph.tasks.find((task) => task.id === taskId)
      if (task === undefined) return yield* invalid('Completing task is absent')
      return yield* hold(tx, task, outcome, graph)
    }),
  )
  return yield* drain(session, taskId, sessionId)
})

/**
 * Reserve a directly owned child with a replayable native binding. The stable
 * key belongs to the caller's native Activity; execution remains workflow.execute.
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
) {
  const current = yield* Ownership.Current
  yield* current.check
  return yield* current.session.transaction(
    Effect.fnUntraced(function* (tx) {
      yield* current.check
      const owner = yield* tx.task(current.taskId)
      if (
        owner === undefined ||
        owner.abortRequested ||
        owner.state.status === 'completing' ||
        owner.state.status === 'terminal'
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
 * A domain outcome helper used inside an ordinary Workflow.toLayer handler.
 * Native suspension/abandonment remains interruption so the engine can replay;
 * completed bodies, typed failures and defects acquire an owned completing hold.
 */
export const evaluate = <E, R>(
  identity: Ownership.Identity,
  session: Session.Service,
  body: Effect.Effect<Record.Json, E, R>,
) =>
  Effect.gen(function* () {
    const exit = yield* Cancellation.run(identity, session, body).pipe(Effect.exit)
    const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
    if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
      if (Option.isSome(instance) && (instance.value.suspended || instance.value.abandoned))
        return yield* Effect.failCause(exit.cause)
      const task = (yield* session.committed).tasks.find((task) => task.id === identity.taskId)
      if (!task?.abortRequested) return yield* Effect.failCause(exit.cause)
    }
    let outcome: Record.Json
    if (Exit.isSuccess(exit)) outcome = { status: 'completed', result: exit.value }
    else {
      const error = Cause.squash(exit.cause)
      let status = Cause.hasDies(exit.cause) ? 'faulted' : 'failed'
      if (error instanceof ExecutionError && error.reason._tag === 'Aborted') status = 'aborted'
      outcome = {
        status,
        error: { message: error instanceof Error ? error.message : String(error) },
      }
    }
    return yield* complete(session, identity.taskId, outcome, identity.sessionId)
  })
