import * as Id from './Identity.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Option from 'effect/Option'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Record from './Record.ts'
import * as Session from './Session.ts'
import { ExecutionError, InvalidState, Closed, Aborted } from './workflow/ExecutionError.ts'

/** Durable references identify native Workflow executions; they contain no custom scheduler state. */
export const Binding = Schema.Struct({
  workflow: Schema.String,
  executionId: Schema.String,
  payload: Schema.Json,
})
export type Binding = typeof Binding.Type

/** Declarations for cancellation and inspection; execution registration is each declaration's ordinary toLayer. */
export class Declarations extends Context.Service<
  Declarations,
  {
    readonly get: (name: string) => Workflow.Any | undefined
    /** Schema context captured when the heterogeneous declaration registry is constructed. */
    readonly schemaContext: Context.Context<never>
  }
>()('@effect-harness/durable/Ownership/Declarations') {}
/** Schema services retained structurally across heterogeneous native declarations. */
export type DeclarationServices<W extends Workflow.Any> =
  | W['payloadSchema']['EncodingServices']
  | W['payloadSchema']['DecodingServices']
  | W['successSchema']['DecodingServices']
  | W['errorSchema']['DecodingServices']

export const layerDeclarations = <const W extends ReadonlyArray<Workflow.Any>>(
  workflows: W,
): Layer.Layer<Declarations, never, DeclarationServices<W[number]>> =>
  Layer.effect(
    Declarations,
    Effect.gen(function* () {
      const context = yield* Effect.context<DeclarationServices<W[number]>>()
      const declarations = new Map(workflows.map((workflow) => [workflow._tag, workflow]))
      return Declarations.of({
        get: (name) => declarations.get(name),
        // Never retain a construction-time native execution identity or engine.
        schemaContext: context.pipe(
          Context.omit(WorkflowEngine.WorkflowInstance, WorkflowEngine.WorkflowEngine),
        ),
      })
    }),
  )

/** Execute native declarations with their captured schema context and the caller's optional WorkflowInstance. */
export const execute = Effect.fnUntraced(function* (
  binding: Binding,
): Effect.fn.Return<unknown, ExecutionError, Declarations | WorkflowEngine.WorkflowEngine> {
  const declarations = yield* Declarations
  const declaration = declarations.get(binding.workflow)
  if (declaration === undefined)
    return yield* new ExecutionError({
      reason: new InvalidState({ message: `Workflow ${binding.workflow} is not declared` }),
    })
  const workflow = Workflow.make(declaration._tag, {
    payload: declaration.payloadSchema,
    success: declaration.successSchema,
    error: declaration.errorSchema,
    annotations: declaration.annotations,
    idempotencyKey: () => binding.executionId,
  })
  const engine = yield* WorkflowEngine.WorkflowEngine
  const parent = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
  // Native Workflow.Any erases heterogeneous schema service requirements. The
  // registry's Layer required every declaration service and captured their exact
  // Context; only this execution boundary restores that erased schema environment.
  // oxlint-disable effecttsgo/any-unknown-in-error-context
  return yield* Effect.gen(function* () {
    const decode = Schema.decodeEffect(Schema.toCodecJson(workflow.payloadSchema))(binding.payload)
    const payload = yield* Option.isSome(parent)
      ? Workflow.wrapActivityResult(decode, () => false).pipe(
          Effect.provideService(WorkflowEngine.WorkflowInstance, parent.value),
        )
      : decode
    return yield* engine.execute(workflow, {
      executionId: binding.executionId,
      payload,
      suspendedRetrySchedule: declaration.suspendedRetrySchedule,
    })
  }).pipe(
    Effect.provideContext(declarations.schemaContext as Context.Context<unknown>),
    Effect.mapError((error) =>
      error instanceof ExecutionError
        ? error
        : new ExecutionError({
            reason: new InvalidState({
              message: `Native workflow ${binding.workflow} failed`,
              cause: error,
            }),
          }),
    ),
  )
  // oxlint-enable effecttsgo/any-unknown-in-error-context
})

export interface Identity {
  readonly sessionId: Id.SessionId
  readonly conversationId: Record.ConversationId
  readonly taskId: Record.TaskId
}
/** A scoped domain identity for tools and user-defined native Workflow activities. */
export class Current extends Context.Service<
  Current,
  Identity & {
    readonly session: Session.Service
    readonly check: Effect.Effect<void, ExecutionError>
  }
>()('@effect-harness/durable/Ownership/Current') {}

export const layerCurrent = (identity: Identity): Layer.Layer<Current, never, Session.Session> =>
  Layer.effect(Current)(
    Effect.gen(function* () {
      const session = yield* Session.Session
      const active = yield* Ref.make(true)
      yield* Effect.addFinalizer(() => Ref.set(active, false))
      return Current.of({
        ...identity,
        session,
        check: Ref.get(active).pipe(
          Effect.flatMap((active) =>
            active
              ? Effect.void
              : Effect.fail(
                  new ExecutionError({
                    reason: new Closed({ message: 'Task invocation has ended' }),
                  }),
                ),
          ),
        ),
      })
    }),
  )

function writable(task: Record.Task | undefined): Effect.Effect<Record.Task, ExecutionError> {
  if (task === undefined)
    return Effect.fail(
      new ExecutionError({ reason: new InvalidState({ message: 'Task projection is absent' }) }),
    )
  if (task.abortRequested || task.state.status === 'terminal' || task.state.status === 'completing')
    return Effect.fail(
      new ExecutionError({
        reason: new Aborted({ message: 'Task no longer accepts invocation writes' }),
      }),
    )
  return Effect.succeed(task)
}

/** First committed value wins. The producer can repeat after a crash; memoization does not promise remote exactly-once effects. */
export const memo = Effect.fnUntraced(function* <S extends Schema.Constraint, E, R>(
  name: string,
  schema: S,
  produce: Effect.Effect<S['Type'], E, R>,
) {
  const current = yield* Current
  yield* current.check
  const task = yield* current.session.task(current.taskId)
  if (task === undefined)
    return yield* new ExecutionError({
      reason: new InvalidState({ message: 'Task projection is absent' }),
    })
  const codec = Schema.toCodecJson(schema)
  if (task.memos !== undefined && Object.hasOwn(task.memos, name))
    return yield* Schema.decodeEffect(codec)(task.memos[name] ?? null)
  yield* writable(task)
  const value = yield* produce
  const encoded = yield* Schema.encodeEffect(codec)(value)
  const committed = yield* current.session.transaction(
    Effect.fnUntraced(function* (tx) {
      yield* current.check
      const latest = yield* writable(yield* tx.task(current.taskId))
      if (latest.memos !== undefined && Object.hasOwn(latest.memos, name))
        return latest.memos[name] ?? null
      const memos = { ...latest.memos }
      Object.defineProperty(memos, name, {
        value: encoded,
        enumerable: true,
        writable: true,
        configurable: true,
      })
      yield* tx.write({ type: 'task', value: { ...latest, memos } })
      return encoded
    }),
  )
  return yield* Schema.decodeEffect(codec)(committed)
})

export interface Graph {
  readonly conversations: ReadonlyArray<Record.Conversation>
  readonly tasks: ReadonlyArray<Record.Task>
  readonly submissions?: ReadonlyArray<Record.Submission>
}
export type Target =
  | { readonly kind: 'conversation'; readonly id: Record.ConversationId }
  | { readonly kind: 'task'; readonly id: Record.TaskId }
export interface Reached {
  /** Children precede parents so native interrupts and compensation can drain bottom-up. */
  readonly tasks: ReadonlyArray<Record.Task>
  readonly conversations: ReadonlyArray<Record.Conversation>
}

/** Read-only ownership traversal. Background roots fence their entire subtree unless explicitly selected or included. */
export function reach(graph: Graph, target: Target, background = false): Reached | undefined {
  const tasks = new Map(graph.tasks.map((task) => [task.id, task]))
  const conversations = new Map(
    graph.conversations.map((conversation) => [conversation.id, conversation]),
  )
  if (target.kind === 'task' ? !tasks.has(target.id) : !conversations.has(target.id))
    return undefined
  if (target.kind === 'task' && tasks.get(target.id)?.state.status === 'terminal')
    return { tasks: [], conversations: [] }
  const seenTasks = new Set<Record.TaskId>()
  const seenConversations = new Set<Record.ConversationId>()
  const ordered: Record.Task[] = []
  const selected: Record.Conversation[] = []
  type Work =
    | { readonly type: 'conversation'; readonly id: Record.ConversationId }
    | { readonly type: 'task'; readonly id: Record.TaskId; readonly direct: boolean }
    | { readonly type: 'end'; readonly task: Record.Task }
  const work: Work[] =
    target.kind === 'task'
      ? [{ type: 'task', id: target.id, direct: true }]
      : [{ type: 'conversation', id: target.id }]
  while (work.length > 0) {
    const item = work.pop()
    if (item === undefined) break
    if (item.type === 'end') {
      ordered.push(item.task)
      continue
    }
    if (item.type === 'conversation') {
      if (seenConversations.has(item.id)) continue
      const conversation = conversations.get(item.id)
      if (conversation === undefined) continue
      seenConversations.add(item.id)
      selected.push(conversation)
      for (const task of graph.tasks.toReversed())
        if (task.conversationId === item.id && (task.owner === undefined || !tasks.has(task.owner)))
          work.push({ type: 'task', id: task.id, direct: false })
      continue
    }
    if (seenTasks.has(item.id)) continue
    const task = tasks.get(item.id)
    if (
      task === undefined ||
      (target.kind === 'task' && task.state.status === 'terminal') ||
      (task.background && !background && !item.direct)
    )
      continue
    seenTasks.add(item.id)
    if (task.state.status !== 'terminal') work.push({ type: 'end', task })
    for (const conversation of graph.conversations.toReversed())
      if (conversation.owner?.taskId === task.id)
        work.push({ type: 'conversation', id: conversation.id })
    for (const child of graph.tasks.toReversed())
      if (child.owner === task.id) work.push({ type: 'task', id: child.id, direct: false })
  }
  return { tasks: ordered, conversations: selected }
}

/** Table reads are collected before any abort marks or inbox withdrawal are written. */
export const readGraph = Effect.fnUntraced(function* (
  tx: Session.Transaction,
): Effect.fn.Return<Graph, import('./StorageError.ts').StorageError> {
  const conversations: Record.Conversation[] = []
  const tasks: Record.Task[] = []
  let cursor: Record.Cursor | undefined
  do {
    const page = yield* tx.scanConversations({}, 100, cursor)
    conversations.push(...page.items)
    cursor = page.next
  } while (cursor !== undefined)
  cursor = undefined
  do {
    const page: Record.Page<Record.Task> = yield* tx.scanTasks({}, 100, cursor)
    tasks.push(...page.items)
    cursor = page.next
  } while (cursor !== undefined)
  const submissions: Record.Submission[] = []
  cursor = undefined
  do {
    const page: Record.Page<Record.Submission> = yield* tx.scanSubmissions({}, 100, cursor)
    submissions.push(...page.items)
    cursor = page.next
  } while (cursor !== undefined)
  return { conversations, tasks, submissions }
})
