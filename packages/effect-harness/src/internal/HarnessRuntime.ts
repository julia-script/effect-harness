/** Persisted task checkpoints are authoritative; scoped fibers are disposable. */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Identity from '../Identity.js'
import * as Record from '../Record.js'
import * as Agent from '../Agent.js'
import * as ConversationInitializer from '../ConversationInitializer.js'
import * as Document from '../Document.js'
import { ExecutionError } from '../ExecutionError.js'
import type * as Extension from '../Extension.js'
import type { HarnessBackendService } from '../HarnessBackend.js'
import {
  CreateOptionsSchema,
  ConfigureSchema,
  SubmitSchema,
  ForkSchema,
  WatchSchema,
} from '../HarnessBackend.js'
import { HarnessError } from '../HarnessError.js'
import type { Options, HarnessRuntimeService, Requirements } from '../HarnessRuntime.js'
import type * as Hook from '../Hook.js'
import { HookExecution } from '../HookExecution.js'
import type * as Model from '../Model.js'
import * as Session from '../Session.js'
import { Storage } from '../Storage.js'
import type * as StorageRecord from '../Record.js'
import type * as Submission from '../Submission.js'
import type * as Tool from '../Tool.js'
import * as Toolkit from '../Toolkit.js'
import * as Transaction from '../Transaction.js'
import * as Capability from './Capability.js'
import * as RunLoop from './RunLoop.js'
import { document as agentDocument, target as agentTarget } from './AgentState.js'
import { protect, absent } from './HarnessFailure.js'
import * as Run from './RunState.js'

const rootOptions = { ownership: { _tag: 'ownerless' } } as const
const runKind = 'harness.run'
interface Invocation {
  ended: boolean
  fiber?: Fiber.Fiber<void, never>
}
interface BoundHook {
  readonly hook: Hook.Any
  readonly context: Context.Context<never>
  readonly extension?: string
}

export const make = Effect.fnUntraced(function* <
  T extends Record<string, Tool.Any>,
  H extends ReadonlyArray<Hook.Any>,
  X extends ReadonlyArray<Extension.Any>,
  M extends ReadonlyArray<Model.Any>,
  I extends ReadonlyArray<ConversationInitializer.Any>,
>(
  options: Options<T, H, X, M, I>,
): Effect.fn.Return<
  HarnessRuntimeService,
  HarnessError,
  Scope.Scope | Requirements<T, H, X, M, I>
> {
  const context = yield* Effect.context<Requirements<T, H, X, M, I>>()
  const owner = yield* Scope.Scope
  const ownedScope = yield* Scope.fork(owner)
  return yield* protect(
    'runtime.make',
    Effect.gen(function* () {
      const storage = yield* Storage
      const session = yield* Session.make({
        initializers: [
          ConversationInitializer.make({
            execute: (tx, record) =>
              Transaction.ensureDocument(tx, agentDocument, agentTarget(record.id)).pipe(
                Effect.asVoid,
              ),
          }),
          ...(options.initializers ?? []),
        ],
      })
      const schedulerScope = yield* Scope.make()
      const wakeups = yield* Queue.make<void>({ capacity: 1, strategy: 'dropping' })
      const topTools = Object.values(options.tools?.tools ?? {})
      const extensions = options.extensions ?? []
      const toolkit = Toolkit.make(
        ...topTools,
        ...extensions.flatMap((extension) => extension.tools),
      )
      // The native toolkit resolves duplicate names last-wins. Policy and admission must
      // use those same declarations as the captured handler bindings.
      const allTools = Object.values(toolkit.tools)
      const hooks: Array<BoundHook> = []
      const sections: Array<RunLoop.BoundSection> = []
      let handlerContext: Context.Context<never> = context
      for (const hook of options.hooks ?? [])
        hooks.push({ hook, context: yield* Capability.context(hook) })
      for (const extension of extensions) {
        const extensionContext = yield* Capability.context(extension)
        handlerContext = Context.merge(handlerContext, extensionContext)
        yield* Effect.gen(function* () {
          for (const hook of extension.hooks)
            hooks.push({
              hook,
              context: yield* Capability.context(hook),
              extension: extension.name,
            })
          for (const section of extension.sections)
            sections.push({
              section,
              context: yield* Capability.context(section),
              extension: extension.name,
            })
        }).pipe(Effect.provideContext(extensionContext))
      }
      // The public generic requirement enumerates each handler and codec service before this runtime erases tool names.
      const handlers = yield* toolkit.pipe(
        Effect.provideContext(handlerContext as Context.Context<unknown>),
      )
      const defaultAgent = yield* Schema.decodeEffect(Agent.StateSchema)(options.agent ?? {})
      const maxTurns = yield* Schema.decodeEffect(Schema.Int.check(Schema.isGreaterThan(0)))(
        options.maxTurns ?? 100,
      )
      const models = options.models ?? []
      const nativeDefault = Context.getOption(context, LanguageModel.LanguageModel)
      if (models.length === 0 && Option.isNone(nativeDefault))
        return yield* absent('runtime.make', 'A LanguageModel or model descriptor is required')
      const active = new Map<Record.ConversationId, Invocation>()
      const waiters = new Map<
        Record.SubmissionId,
        Set<Deferred.Deferred<Submission.Settled, HarnessError>>
      >()
      const idleWaiters = new Set<Deferred.Deferred<void, HarnessError>>()
      let closing = false
      let enabled = false
      let schedulerFailure: HarnessError | undefined
      const checkOpen = Effect.suspend(() => {
        if (closing)
          return Effect.fail(
            new HarnessError({
              reason: 'closed',
              operation: 'runtime',
              message: 'The owning runtime Scope is closed',
            }),
          )
        if (schedulerFailure !== undefined) return Effect.fail(schedulerFailure)
        return Effect.void
      })
      const wake = Queue.offer(wakeups, undefined).pipe(Effect.asVoid)
      const activate = Effect.gen(function* () {
        yield* checkOpen
        enabled = true
        yield* wake
      })
      const requireConversation = Effect.fnUntraced(function* (
        tx: Transaction.Transaction,
        id: Record.ConversationId,
      ) {
        const found = yield* Transaction.conversation(tx, id)
        if (Option.isNone(found))
          return yield* absent('conversation', `Conversation ${id} does not exist`)
        return found.value
      })
      const readSubmission = Effect.fnUntraced(function* (id: Record.SubmissionId) {
        yield* checkOpen
        const found = yield* Session.submission(session, id)
        if (Option.isNone(found) || found.value.type !== 'input')
          return yield* absent('submission.read', `Input submission ${id} does not exist`)
        return found.value
      })
      const notify = Effect.fnUntraced(function* () {
        for (const [id, waiting] of waiters) {
          const record = yield* readSubmission(id)
          if (record.status === 'done' || record.status === 'unanswered') {
            waiters.delete(id)
            for (const waiter of waiting) yield* Deferred.succeed(waiter, record)
          }
        }
      })
      const snapshotAgent = Effect.fnUntraced(function* (id: Record.ConversationId) {
        const snapshot = yield* Session.snapshot(session, agentDocument, agentTarget(id))
        return { ...defaultAgent, ...(Option.isSome(snapshot) ? snapshot.value.value : {}) }
      })
      const access = (id: Record.ConversationId) => ({
        conversationId: id,
        snapshot: <S extends Document.Codec>(
          document: Document.Document<S>,
          target: Document.Target,
        ) => Session.snapshot(session, document, target),
        watch: <S extends Document.Codec>(
          document: Document.Document<S>,
          target: Document.Target,
        ) => Session.watch(session, document, target),
        commit: <A, E, R>(change: (tx: Transaction.Transaction) => Effect.Effect<A, E, R>) =>
          Session.commit(session, change, { conversationId: id }),
      })
      const runHooks = Effect.fnUntraced(function* (
        id: Record.ConversationId,
        agent: Agent.State,
        event: Hook.Event,
        taskId?: Record.TaskId,
      ) {
        let output: Hook.Output<Hook.Name> = undefined
        for (const bound of hooks) {
          if (
            bound.hook.event !== event._tag ||
            (bound.extension !== undefined &&
              agent.extensions !== undefined &&
              !agent.extensions.includes(bound.extension))
          )
            continue
          // Event matching establishes the concrete callback's input; errors are normalized at this boundary.
          const execute = bound.hook.execute as (
            event: Hook.Event,
          ) => Effect.Effect<Hook.Output<Hook.Name>, HarnessError, HookExecution>
          const value = yield* protect(
            'hook.execute',
            execute(event).pipe(
              Effect.updateContext((input: Context.Context<HookExecution>) =>
                Context.merge(bound.context, input),
              ),
              Effect.provideService(HookExecution, {
                ...access(id),
                ...(taskId === undefined ? {} : { taskId }),
              }),
            ),
          )
          if (value !== undefined) output = value
        }
        return output
      })
      const saveState = Effect.fnUntraced(function* (
        tx: Transaction.Transaction,
        task: Record.Task,
        state: Run.RunState,
      ) {
        yield* Transaction.putTask(tx, {
          ...task,
          state: { status: 'running', checkpoint: yield* Run.encode(state) },
        })
      })
      const currentTask = Effect.fnUntraced(function* (
        tx: Transaction.Transaction,
        task: Record.Task,
        invocation: Invocation,
      ) {
        yield* checkOpen.pipe(
          Effect.mapError(
            (error) =>
              new ExecutionError({
                reason: 'closed',
                operation: 'task.commit',
                message: error.message,
                cause: error,
              }),
          ),
        )
        const found = yield* Transaction.task(tx, task.id)
        if (
          invocation.ended ||
          Option.isNone(found) ||
          found.value.state.status === 'terminal' ||
          found.value.abortRequested
        )
          return yield* new ExecutionError({
            reason: 'revoked',
            operation: 'task.commit',
            message: 'The task invocation is no longer active',
          })
        return found.value
      })
      const taskCommit = <A, E, R>(
        task: Record.Task,
        invocation: Invocation,
        change: (tx: Transaction.Transaction, current: Record.Task) => Effect.Effect<A, E, R>,
      ) =>
        Session.commit(
          session,
          (tx) =>
            currentTask(tx, task, invocation).pipe(
              Effect.flatMap((current) => change(tx, current)),
            ),
          { conversationId: task.conversationId, taskId: task.id },
        )
      const unanswered = Effect.fnUntraced(function* (
        task: Record.Task,
        reason: string,
        detail?: Schema.Json,
      ) {
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const current = yield* Transaction.task(tx, task.id)
            if (Option.isNone(current) || current.value.state.status === 'terminal') return
            const state = yield* Run.decode(current.value.state.checkpoint)
            const record = yield* Transaction.submission(tx, state.submissionId)
            if (
              Option.isSome(record) &&
              record.value.type === 'input' &&
              record.value.status !== 'done' &&
              record.value.status !== 'unanswered'
            ) {
              yield* Transaction.putSubmission(tx, {
                ...record.value,
                _tag: 'InputUnanswered',
                status: 'unanswered',
                reason,
                ...(detail === undefined ? {} : { detail }),
              })
            }
            yield* Transaction.putTask(tx, {
              ...current.value,
              state: {
                status: 'terminal',
                outcome: { status: 'failed', error: { message: reason } },
              },
            })
          }),
        )
        yield* notify()
      })
      const failScheduler = Effect.fnUntraced(function* (error: HarnessError) {
        schedulerFailure = error
        for (const waiting of waiters.values())
          for (const waiter of waiting) yield* Deferred.fail(waiter, error)
        waiters.clear()
        for (const waiter of idleWaiters) yield* Deferred.fail(waiter, error)
        idleWaiters.clear()
      })
      const pump = Effect.fnUntraced(function* () {
        if (closing || schedulerFailure !== undefined) return
        const tasks = yield* Stream.runCollect(
          Session.scanTasks(session, { kind: runKind, order: 'ascending' }),
        ).pipe(Effect.map((values) => values.filter((task) => task.state.status !== 'terminal')))
        if (enabled)
          for (const task of tasks) {
            if (active.has(task.conversationId)) continue
            const invocation: Invocation = { ended: false }
            active.set(task.conversationId, invocation)
            const execute = RunLoop.run(
              {
                session,
                context,
                allTools,
                extensions,
                sections,
                models,
                nativeDefault,
                maxTurns,
                defaultAgent,
                checkOpen,
                access,
                runHooks,
                saveState,
                notify: notify(),
                handlers,
                write: (change) => taskCommit(task, invocation, change),
                invocationActive: () => !invocation.ended,
              },
              task,
            ).pipe(
              Effect.catchCause((cause) => {
                if (Cause.hasInterrupts(cause) || closing) return Effect.void
                return protect(
                  'run.fail',
                  unanswered(task, 'failed', { message: Cause.pretty(cause) }),
                ).pipe(Effect.catch((error) => failScheduler(error)))
              }),
              Effect.ensuring(
                Effect.gen(function* () {
                  invocation.ended = true
                  active.delete(task.conversationId)
                  yield* wake
                }),
              ),
            )
            invocation.fiber = yield* Effect.forkIn(execute, schedulerScope)
          }
        if (active.size === 0 && tasks.length === 0) {
          for (const waiter of idleWaiters) yield* Deferred.succeed(waiter, undefined)
          idleWaiters.clear()
        }
      })
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closing = true
          for (const invocation of active.values()) invocation.ended = true
          yield* Scope.close(schedulerScope, Exit.void)
          const error = new HarnessError({
            reason: 'closed',
            operation: 'runtime.close',
            message: 'The owning runtime Scope is closed',
          })
          yield* failScheduler(error)
        }),
      )
      yield* Effect.forkIn(
        Effect.forever(
          Queue.take(wakeups).pipe(
            Effect.andThen(
              protect('runtime.pump', pump()).pipe(Effect.catch((error) => failScheduler(error))),
            ),
          ),
        ),
        schedulerScope,
      )
      const rawSnapshot = Effect.fnUntraced(function* (address: StorageRecord.DocumentAddress) {
        yield* checkOpen
        return yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const records = yield* Stream.runCollect(
              Transaction.scanDocuments(tx, { scope: address.scope, kind: address.kind }),
            )
            const record = records.find((record) => record.key === address.key)
            if (record === undefined) return Option.none<StorageRecord.StoredDocument>()
            return yield* storage.document(record.id)
          }),
        )
      })
      const initialize = Effect.fnUntraced(function* (
        root: boolean,
        input: import('../HarnessBackend.js').CreateOptions = {},
      ) {
        yield* checkOpen
        const options = yield* Schema.decodeEffect(CreateOptionsSchema)(input)
        let created = false
        const record = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const previous = root
              ? yield* Transaction.conversation(tx, Record.ROOT_CONVERSATION_ID)
              : Option.none<Record.Conversation>()
            const value = root
              ? yield* Transaction.ensureRoot(tx)
              : yield* Transaction.createConversation(tx, rootOptions)
            created = Option.isNone(previous)
            yield* Transaction.ensureDocument(tx, agentDocument, agentTarget(value.id))
            if (options.agent !== undefined)
              yield* Transaction.setDocument(
                tx,
                agentDocument,
                agentTarget(value.id),
                options.agent,
              )
            return value
          }),
        )
        if (created)
          yield* runHooks(record.id, yield* snapshotAgent(record.id), {
            _tag: 'conversationCreated',
            conversationId: record.id,
          })
        yield* activate
        return record.id
      })
      const backend: HarnessBackendService = {
        usage: (id) =>
          protect('harness.usage', checkOpen.pipe(Effect.andThen(Session.usage(session, id)))),
        root: protect('harness.root', initialize(true)),
        create: (options) => protect('harness.create', initialize(false, options)),
        conversation: (id) =>
          protect(
            'harness.conversation',
            checkOpen.pipe(
              Effect.andThen(Session.conversation(session, id)),
              Effect.tap((value) => (Option.isSome(value) ? activate : Effect.void)),
              Effect.map(Option.map((value) => value.id)),
            ),
          ),
        submit: (input) =>
          protect(
            'conversation.submit',
            Effect.gen(function* () {
              yield* checkOpen
              const { conversationId, draft } = yield* Schema.decodeEffect(
                Schema.toType(SubmitSchema),
              )(input)
              const requestId =
                draft.requestId === undefined
                  ? undefined
                  : yield* Schema.decodeEffect(Identity.RequestId)(draft.requestId)
              const result = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* requireConversation(tx, conversationId)
                  if (requestId !== undefined) {
                    const existing = yield* Transaction.submissionByRequest(
                      tx,
                      conversationId,
                      requestId,
                    )
                    if (Option.isSome(existing)) return existing.value
                  }
                  const submission = yield* Transaction.createSubmission(tx, {
                    _tag: 'InputQueued',
                    type: 'input',
                    status: 'queued',
                    conversationId,
                    ...(requestId === undefined ? {} : { requestId }),
                  })
                  const state: Run.RunState = {
                    _tag: 'queued',
                    phase: 'queued',
                    submissionId: submission.id,
                    draft,
                  }
                  yield* Transaction.createTask(tx, {
                    conversationId,
                    kind: runKind,
                    version: 1,
                    background: false,
                    abortRequested: false,
                    input: submission.id,
                    state: { status: 'pending', checkpoint: yield* Run.encode(state) },
                  })
                  return submission
                }),
              )
              yield* activate
              return { id: result.id, conversationId: result.conversationId }
            }),
          ),
        configure: (input) =>
          protect(
            'conversation.configure',
            Effect.gen(function* () {
              yield* checkOpen
              const { conversationId, change } = yield* Schema.decodeEffect(ConfigureSchema)(input)
              yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* requireConversation(tx, conversationId)
                  const snapshot = yield* Transaction.ensureDocument(
                    tx,
                    agentDocument,
                    agentTarget(conversationId),
                  )
                  const value = { ...snapshot.value }
                  for (const key of ['model', 'extensions', 'tools', 'instructions'] as const) {
                    if (change[key] === null) delete value[key]
                    else if (change[key] !== undefined) Object.assign(value, { [key]: change[key] })
                  }
                  yield* Transaction.appendEntry(tx, conversationId, {
                    kind: 'agent.configure',
                    head: 'self',
                  })
                  yield* Transaction.setDocument(
                    tx,
                    agentDocument,
                    agentTarget(conversationId),
                    value,
                  )
                }),
              )
            }),
          ),
        agent: (id) =>
          protect('conversation.agent', checkOpen.pipe(Effect.andThen(snapshotAgent(id)))),
        fork: (input) =>
          protect(
            'conversation.fork',
            Effect.gen(function* () {
              yield* checkOpen
              const { conversationId, options } = yield* Schema.decodeEffect(ForkSchema)(input)
              return yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* requireConversation(tx, conversationId)
                  const entries = yield* Stream.runCollect(
                    Transaction.scanEntries(tx, { conversationId, order: 'descending' }),
                  )
                  const at = options?.at ?? entries[0]?.id
                  if (at === undefined)
                    return yield* new HarnessError({
                      reason: 'invalid',
                      operation: 'conversation.fork',
                      message: 'An empty conversation has no visible entry to fork',
                    })
                  return (yield* Transaction.forkConversation(tx, conversationId, at, rootOptions))
                    .id
                }),
              )
            }),
          ),
        abort: (id) =>
          protect(
            'conversation.abort',
            Effect.gen(function* () {
              yield* checkOpen
              const tasks = yield* Stream.runCollect(
                Session.scanTasks(session, { conversationId: id, kind: runKind }),
              )
              for (const task of tasks)
                if (task.state.status !== 'terminal') yield* unanswered(task, 'aborted')
              const invocation = active.get(id)
              if (invocation !== undefined) {
                invocation.ended = true
                if (invocation.fiber !== undefined) yield* Fiber.interrupt(invocation.fiber)
              }
              yield* wake
            }),
          ),
        read: (id) => protect('submission.read', readSubmission(id)),
        wait: (id) =>
          protect(
            'submission.wait',
            Effect.gen(function* () {
              const waiter = yield* Deferred.make<Submission.Settled, HarnessError>()
              const settled = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* checkOpen
                  const found = yield* Transaction.submission(tx, id)
                  if (Option.isNone(found) || found.value.type !== 'input')
                    return yield* absent('submission.wait', 'Input submission does not exist')
                  if (found.value.status === 'done' || found.value.status === 'unanswered')
                    return found.value
                  const waiting = waiters.get(id) ?? new Set()
                  waiting.add(waiter)
                  waiters.set(id, waiting)
                  return undefined
                }),
              )
              if (settled !== undefined) return settled
              return yield* Deferred.await(waiter).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    const waiting = waiters.get(id)
                    waiting?.delete(waiter)
                    if (waiting?.size === 0) waiters.delete(id)
                  }),
                ),
              )
            }),
          ),
        withdraw: (id) =>
          protect(
            'submission.withdraw',
            Effect.gen(function* () {
              const result = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* checkOpen
                  const found = yield* Transaction.submission(tx, id)
                  if (Option.isNone(found) || found.value.type !== 'input')
                    return yield* absent('submission.withdraw', 'Input submission does not exist')
                  if (found.value.status === 'placed') return 'already_placed' as const
                  if (found.value.status === 'done' || found.value.status === 'unanswered')
                    return 'settled' as const
                  yield* Transaction.putSubmission(tx, {
                    ...found.value,
                    _tag: 'InputUnanswered',
                    status: 'unanswered',
                    reason: 'withdrawn',
                  })
                  const tasks = yield* Stream.runCollect(
                    Transaction.scanTasks(tx, {
                      conversationId: found.value.conversationId,
                      kind: runKind,
                    }),
                  )
                  for (const task of tasks) {
                    if (task.state.status === 'terminal') continue
                    const state = yield* Run.decode(task.state.checkpoint)
                    if (state.submissionId === id)
                      yield* Transaction.putTask(tx, {
                        ...task,
                        state: {
                          status: 'terminal',
                          outcome: { status: 'aborted', reason: 'withdrawn' },
                        },
                      })
                  }
                  return 'aborted' as const
                }),
              )
              yield* notify()
              yield* wake
              return result
            }),
          ),
        entries: (input) =>
          Stream.unwrap(
            protect(
              'conversation.entries',
              Effect.gen(function* () {
                yield* checkOpen
                const query = yield* Schema.decodeEffect(WatchSchema)(input)
                return Stream.unwrap(
                  Effect.gen(function* () {
                    const pull = yield* Stream.toPull(Session.commits(session))
                    let cursor = query.after ?? 0
                    const read = Session.scanEntries(session, {
                      conversationId: query.conversationId,
                      order: 'ascending',
                    }).pipe(
                      Stream.filter((entry) => entry.id > cursor),
                      Stream.tap((entry) =>
                        Effect.sync(() => {
                          cursor = entry.id
                        }),
                      ),
                    )
                    return Stream.concat(
                      read,
                      Stream.fromEffectRepeat(pull).pipe(Stream.flatMap(() => read)),
                    )
                  }),
                )
              }),
            ),
          ).pipe(
            Stream.mapError((error) =>
              Schema.is(HarnessError)(error)
                ? error
                : new HarnessError({
                    reason: 'failed',
                    operation: 'conversation.entries',
                    message: String(error),
                    cause: error,
                  }),
            ),
          ),
        snapshot: (address) => protect('conversation.snapshot', rawSnapshot(address)),
        watch: (address) =>
          Stream.unwrap(
            protect(
              'conversation.watch',
              Effect.gen(function* () {
                const initial = yield* rawSnapshot(address)
                if (Option.isNone(initial))
                  return yield* absent('conversation.watch', 'Document does not exist')
                const record = initial.value.record
                const metadata = yield* Schema.decodeUnknownEffect(Document.MetadataSchema)({
                  kind: record.kind,
                  version: initial.value.version,
                  scope: record.scope._tag,
                  ...(record.scope._tag === 'conversation'
                    ? { history: record.history ?? 'latest', fork: record.fork ?? 'current' }
                    : {}),
                })
                const definition = { ...metadata, schema: Schema.JsonObject, initial: () => ({}) }
                const document =
                  record.key === undefined
                    ? Document.define(definition)
                    : Document.family(definition)
                return Session.watch(session, document, address)
              }),
            ),
          ).pipe(
            Stream.mapError((error) =>
              Schema.is(HarnessError)(error)
                ? error
                : new HarnessError({
                    reason: 'failed',
                    operation: 'conversation.watch',
                    message: error.message,
                    cause: error,
                  }),
            ),
          ),
        waitForIdle: protect(
          'harness.waitForIdle',
          Effect.gen(function* () {
            yield* checkOpen
            const waiter = yield* Deferred.make<void, HarnessError>()
            idleWaiters.add(waiter)
            yield* activate.pipe(
              Effect.andThen(Deferred.await(waiter)),
              Effect.ensuring(
                Effect.sync(() => {
                  idleWaiters.delete(waiter)
                }),
              ),
            )
          }),
        ),
      }
      return { session, backend }
    }).pipe(
      Effect.provideService(Scope.Scope, ownedScope),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(ownedScope, exit) : Effect.void)),
    ),
  )
})
