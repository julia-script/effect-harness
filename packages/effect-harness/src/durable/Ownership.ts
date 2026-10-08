/**
 * Native Workflow declaration metadata and pure ownership traversal.
 */
import * as MutableHashMap from 'effect/MutableHashMap'
import * as Arr from 'effect/Array'
import { absurd, constFalse, constant } from 'effect/Function'
import * as Predicate from 'effect/Predicate'
import { dual } from 'effect/Function'
import * as Data from 'effect/Data'
import type { StorageError } from './StorageError.ts'
import type * as identity from './Identity.ts'
// effect-nit-allow P9-namespace-alias-equals-module: the exported Identity value type collides with the imported identifier namespace.
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Option from 'effect/Option'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import type * as Record from './Record.ts'
import * as Session from './Session.ts'
import {
  ExecutionError,
  InvalidStateError,
  ClosedError,
  AbortedError,
} from './workflow/ExecutionError.ts'

/**
 * Durable references identify native Workflow executions; they contain no custom scheduler state.
 *
 * @category schemas
 */
export const Binding = Schema.Struct({
  workflow: Schema.String,
  executionId: Schema.String,
  payload: Schema.Json,
})
/**
 * Native Workflow name, execution identity and encoded payload bound to a task.
 *
 * @category models
 */
export type Binding = typeof Binding.Type

/**
 * Declarations for cancellation and inspection; execution registration is each declaration's ordinary toLayer.
 *
 * @category services
 */
export class Declarations extends Context.Service<
  Declarations,
  {
    readonly get: (name: string) => Option.Option<Workflow.Any>
    /**
     * Schema context captured when the heterogeneous declaration registry is constructed.
     */
    readonly schemaContext: Context.Context<never>
  }
>()('effect-harness/durable/Ownership/Declarations') {}

/**
 * Provides the native Workflow declarations permitted for owned task bindings.
 *
 * **Details**
 *
 * Captures payload schema services supplied during construction. The service exposes
 * declaration metadata without acquiring a WorkflowEngine.
 *
 * **Gotchas**
 *
 * Include all built-in declarations when extending the set for custom owned work. A missing
 * declaration prevents its persisted binding from executing.
 *
 * @category layers
 */
export const layerDeclarations = <const W extends ReadonlyArray<Workflow.Any>>(
  workflows: W,
): Layer.Layer<Declarations, never, Declarations.Services<W[number]>> =>
  Layer.effect(
    Declarations,
    Effect.gen(function* () {
      const context = yield* Effect.context<Declarations.Services<W[number]>>()
      const declarations = MutableHashMap.fromIterable(
        workflows.map((workflow) => [workflow._tag, workflow]),
      )
      return Declarations.of({
        get: (name) => MutableHashMap.get(declarations, name),
        // Never retain a construction-time native execution identity or engine.
        schemaContext: context.pipe(
          Context.omit(WorkflowEngine.WorkflowInstance, WorkflowEngine.WorkflowEngine),
        ),
      })
    }),
  )

/**
 * Executes native declarations with their captured schema context and the caller's optional WorkflowInstance.
 *
 * @category combinators
 */
export const execute = Effect.fnUntraced(function* (
  binding: Binding,
): Effect.fn.Return<unknown, ExecutionError, Declarations | WorkflowEngine.WorkflowEngine> {
  const declarations = yield* Declarations
  const declarationOption = declarations.get(binding.workflow)
  if (Option.isNone(declarationOption))
    return yield* new ExecutionError({
      reason: new InvalidStateError({ message: `Workflow ${binding.workflow} is not declared` }),
    })
  const declaration = declarationOption.value
  const workflow = Workflow.make(declaration._tag, {
    payload: declaration.payloadSchema,
    success: declaration.successSchema,
    error: declaration.errorSchema,
    annotations: declaration.annotations,
    idempotencyKey: constant(binding.executionId),
  })
  const engine = yield* WorkflowEngine.WorkflowEngine
  const parent = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
  // Native Workflow.Any erases heterogeneous schema service requirements. The
  // registry's Layer required every declaration service and captured their exact
  // Context; only this execution boundary restores that erased schema environment.
  // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Workflow.Any erases services already required and captured by layerDeclarations.
  return yield* Effect.gen(function* () {
    // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Workflow.Any erases heterogeneous schema channels; layerDeclarations requires and captures every schema service, and this boundary supplies that Context and maps native failures.
    const decode = Schema.decodeEffect(Schema.toCodecJson(workflow.payloadSchema))(binding.payload)
    // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Workflow.Any erases heterogeneous schema channels; layerDeclarations requires and captures every schema service, and this boundary supplies that Context and maps native failures.
    const payload = yield* Option.match(parent, {
      // Heterogeneous declaration schema requirements were captured at construction.
      onSome: (instance) =>
        // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Native Workflow.Any erases schema services already required and captured by layerDeclarations; this invocation supplies that exact Context.
        Workflow.wrapActivityResult(decode, constFalse).pipe(
          // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- The native wrapper carries only the captured heterogeneous declaration schema services, not a new unknown requirement.
          Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
        ),
      // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Workflow.Any erases only declaration schema services already required and captured by layerDeclarations.
      onNone: () => decode,
    })
    // oxlint-disable-next-line effecttsgo/any-unknown-in-error-context -- Native Workflow.Any erases schema errors; the boundary below maps every native typed failure to structured ExecutionError.
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
            reason: new InvalidStateError({
              message: `Native workflow ${binding.workflow} failed`,
              cause: error,
            }),
          }),
    ),
  )
})
/**
 * A scoped domain identity for tools and user-defined native Workflow activities.
 *
 * @category services
 */
export class Current extends Context.Service<
  Current,
  Current.Identity & {
    readonly session: Session.Session.Service
    readonly check: Effect.Effect<void, ExecutionError>
  }
>()('effect-harness/durable/Ownership/Current') {}

/**
 * Provides invocation identity and checks for an owned task in its exact Session.
 *
 * **Details**
 *
 * The supplied Session must be the invocation’s domain Session. check fences abort-marked or
 * terminal work before it can continue.
 *
 * @category layers
 */
export const layerCurrent = (
  identity: Current.Identity,
): Layer.Layer<Current, never, Session.Session> =>
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
                    reason: new ClosedError({ message: 'Task invocation has ended' }),
                  }),
                ),
          ),
        ),
      })
    }),
  )

function writable(option: Option.Option<Record.Task>): Effect.Effect<Record.Task, ExecutionError> {
  if (Option.isNone(option))
    return Effect.fail(
      new ExecutionError({
        reason: new InvalidStateError({ message: 'Task projection is absent' }),
      }),
    )
  const task = option.value
  if (task.abortRequested || task.state.status === 'terminal' || task.state.status === 'completing')
    return Effect.fail(
      new ExecutionError({
        reason: new AbortedError({ message: 'Task no longer accepts invocation writes' }),
      }),
    )
  return Effect.succeed(task)
}

/**
 * Returns the first committed memo value for the selected task and key.
 *
 * **Details**
 *
 * The producer can repeat after a crash; memoization does not promise remote exactly-once effects.
 *
 * @category combinators
 */
export const memo = Effect.fnUntraced(function* <S extends Schema.Constraint, E, R>(
  name: string,
  schema: S,
  produce: Effect.Effect<S['Type'], E, R>,
): Effect.fn.Return<
  S['Type'],
  E | ExecutionError | Schema.SchemaError | StorageError,
  R | Current | S['DecodingServices'] | S['EncodingServices']
> {
  const current = yield* Current
  yield* current.check
  const taskOption = yield* current.session.task(current.taskId)
  if (Option.isNone(taskOption))
    return yield* new ExecutionError({
      reason: new InvalidStateError({ message: 'Task projection is absent' }),
    })
  const task = taskOption.value
  const codec = Schema.toCodecJson(schema)
  if (task.memos !== undefined && Object.hasOwn(task.memos, name))
    return yield* Schema.decodeEffect(codec)(task.memos[name] ?? null)
  yield* writable(taskOption)
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
      yield* tx.write({ _tag: 'task', value: { ...latest, memos } })
      return encoded
    }),
  )
  return yield* Schema.decodeEffect(codec)(committed)
})

/**
 * Committed tasks and conversations used for ownership traversal.
 *
 * @category models
 */
export interface Graph {
  readonly conversations: ReadonlyArray<Record.Conversation>
  readonly tasks: ReadonlyArray<Record.Task>
  readonly submissions?: ReadonlyArray<Record.Submission> | undefined
}
/**
 * Ownership traversal target constructors.
 *
 * @category combinators
 */
export const Target = Data.taggedEnum<reach.Target>()

/**
 * Read-only ownership traversal. Background roots fence their entire subtree unless explicitly selected or included.
 */
function reachImpl(
  self: Graph,
  target: reach.Target,
  background = false,
): Option.Option<reach.Reached> {
  const tasks = MutableHashMap.fromIterable(self.tasks.map((task) => [task.id, task]))
  const conversations = MutableHashMap.fromIterable(
    self.conversations.map((conversation) => [conversation.id, conversation]),
  )
  if (
    target._tag === 'task'
      ? !MutableHashMap.has(tasks, target.id)
      : !MutableHashMap.has(conversations, target.id)
  )
    return Option.none()
  if (
    target._tag === 'task' &&
    Option.exists(MutableHashMap.get(tasks, target.id), (task) => task.state.status === 'terminal')
  )
    return Option.some({ tasks: [], conversations: [] })
  const seenTasks = new Set<Record.TaskId>()
  const seenConversations = new Set<Record.ConversationId>()
  const ordered: Array<Record.Task> = []
  const selected: Array<Record.Conversation> = []
  type Work = Data.TaggedEnum<{
    conversation: { readonly id: Record.ConversationId }
    task: { readonly id: Record.TaskId; readonly direct: boolean }
    end: { readonly task: Record.Task }
  }>
  const Work = Data.taggedEnum<Work>()
  const work: Array<Work> =
    target._tag === 'task'
      ? [Work.task({ id: target.id, direct: true })]
      : [Work.conversation({ id: target.id })]
  while (Arr.isArrayNonEmpty(work)) {
    const item = work.pop()
    if (item === undefined) break
    switch (item._tag) {
      case 'end': {
        ordered.push(item.task)
        continue
      }
      case 'conversation': {
        if (seenConversations.has(item.id)) continue
        const foundConversation = MutableHashMap.get(conversations, item.id)
        if (Option.isNone(foundConversation)) continue
        const conversation = foundConversation.value
        seenConversations.add(item.id)
        selected.push(conversation)
        for (const task of self.tasks.toReversed())
          if (
            task.conversationId === item.id &&
            (task.owner === undefined || !MutableHashMap.has(tasks, task.owner))
          )
            work.push(Work.task({ id: task.id, direct: false }))
        continue
      }
      case 'task': {
        if (seenTasks.has(item.id)) continue
        const foundTask = MutableHashMap.get(tasks, item.id)
        if (Option.isNone(foundTask)) continue
        const task = foundTask.value
        if (
          (target._tag === 'task' && task.state.status === 'terminal') ||
          (task.background && !background && !item.direct)
        )
          continue
        seenTasks.add(item.id)
        if (task.state.status !== 'terminal') work.push(Work.end({ task }))
        for (const conversation of self.conversations.toReversed())
          if (conversation.owner?.taskId === task.id)
            work.push(Work.conversation({ id: conversation.id }))
        for (const child of self.tasks.toReversed())
          if (child.owner === task.id) work.push(Work.task({ id: child.id, direct: false }))
        continue
      }
    }
    return absurd(item)
  }
  return Option.some({ tasks: ordered, conversations: selected })
}

/**
 * Reads the committed conversation, task and submission tables into an ownership graph.
 *
 * **Details**
 *
 * Collect these reads before writing abort marks or withdrawing inbox entries.
 *
 * @category combinators
 */
export const readGraph = Effect.fnUntraced(function* (
  tx: Session.Transaction,
): Effect.fn.Return<Graph, import('./StorageError.ts').StorageError> {
  const conversations: Array<Record.Conversation> = []
  const tasks: Array<Record.Task> = []
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
  const submissions: Array<Record.Submission> = []
  cursor = undefined
  do {
    const page: Record.Page<Record.Submission> = yield* tx.scanSubmissions({}, 100, cursor)
    submissions.push(...page.items)
    cursor = page.next
  } while (cursor !== undefined)
  return { conversations, tasks, submissions }
})

/**
 * Returns the bottom-up ownership closure reachable from a target.
 *
 * @category combinators
 */
export const reach: {
  (target: reach.Target, background?: boolean): (self: Graph) => Option.Option<reach.Reached>
  (self: Graph, target: reach.Target, background?: boolean): Option.Option<reach.Reached>
} = dual((args) => Predicate.hasProperty(args[0], 'tasks'), reachImpl)

/**
 * Type-level contracts for `Declarations`.
 *
 */
export declare namespace Declarations {
  /**
   * Payload schema services required by the registered Workflow declaration set.
   *
   * @category models
   */
  export type Services<W extends Workflow.Any> =
    | W['payloadSchema']['EncodingServices']
    | W['payloadSchema']['DecodingServices']
    | W['successSchema']['DecodingServices']
    | W['errorSchema']['DecodingServices']
}

/**
 * Type-level contracts for `Current`.
 *
 */
export declare namespace Current {
  /**
   * Session, conversation and task identities for an owned invocation.
   *
   * @category models
   */
  export interface Identity {
    readonly sessionId: identity.SessionId
    readonly conversationId: Record.ConversationId
    readonly taskId: Record.TaskId
  }
}

/**
 * Type-level contracts for `reach`.
 *
 */
export declare namespace reach {
  /**
   * Ownership traversal target constructors.
   *
   * @category models
   */
  export type Target = Data.TaggedEnum<{
    conversation: { readonly id: Record.ConversationId }
    task: { readonly id: Record.TaskId }
  }>
  /**
   * Owned tasks and conversations reachable from a selected root.
   *
   * @category models
   */
  export interface Reached {
    /**
     * Reachable tasks ordered with children before parents for bottom-up native interruption and compensation.
     */
    readonly tasks: ReadonlyArray<Record.Task>
    readonly conversations: ReadonlyArray<Record.Conversation>
  }
}

/** Checks the decoded Binding contract without decoding or coercing input.
 * @category guards
 */
export const isBinding: (u: unknown) => u is Binding = Schema.is(Schema.toType(Binding))

/** Checks the established Target tag protocol without claiming field validation.
 * @category guards
 */
export const isTarget: (u: unknown) => u is reach.Target = (u) =>
  Target.$is('conversation')(u) || Target.$is('task')(u)
