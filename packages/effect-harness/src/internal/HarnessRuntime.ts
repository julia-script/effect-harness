/** Persisted task checkpoints are authoritative; scoped fibers are disposable. */
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
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
import * as Registry from '../Registry.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as TaskScheduler from '../TaskScheduler.js'
import * as RunTask from './RunTask.js'
import * as Transaction from '../Transaction.js'
import { document as agentDocument, target as agentTarget } from './AgentState.js'
import { protect, absent } from './HarnessFailure.js'
import * as Run from './RunState.js'

const rootOptions = { ownership: { _tag: 'ownerless' } } as const
const runKind = 'harness.run'
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
      const registry = options.registry ?? (yield* Registry.make)
      if (options.tools !== undefined)
        yield* registry.install(
          Registry.toolkit({ id: 'native-runtime-tools', version: 1 }, options.tools),
        )
      for (const [index, extension] of (options.extensions ?? []).entries())
        yield* registry.install(
          Registry.extension(
            { id: `native-runtime-extension-${index}`, version: 1 },
            // Options retains the concrete extension channels before heterogeneous installation.
            extension as Extension.Extension<Requirements<T, H, X, M, I>, HarnessError>,
          ),
        )
      for (const [index, hook] of (options.hooks ?? []).entries())
        yield* registry.install(
          Registry.hook(
            { id: `native-runtime-hook-${index}`, version: 1 },
            // Options enumerates each hook's R/E before the heterogeneous installation bridge.
            hook as Hook.Hook<Hook.Name, HarnessError, Requirements<T, H, X, M, I>>,
          ),
        )
      const defaultAgent = yield* Schema.decodeEffect(Agent.StateSchema)(options.agent ?? {})
      const maxTurns = yield* Schema.decodeEffect(Schema.Int.check(Schema.isGreaterThan(0)))(
        options.maxTurns ?? 100,
      )
      const models = options.models ?? []
      const nativeDefault = Context.getOption(context, LanguageModel.LanguageModel)
      if (models.length === 0 && Option.isNone(nativeDefault))
        return yield* absent('runtime.make', 'A LanguageModel or model descriptor is required')
      const waiters = new Map<
        Record.SubmissionId,
        Set<Deferred.Deferred<Submission.Settled, HarnessError>>
      >()
      const idleWaiters = new Set<Deferred.Deferred<void, HarnessError>>()
      let closing = false
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
      const activate = Effect.suspend(() => protect('runtime.resume', scheduler.resume))
      const wake = activate
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
      const dispatchHooks = Effect.fnUntraced(function* (
        lease: RegistryScope.Lease,
        id: Record.ConversationId,
        agent: Agent.State,
        event: Hook.Event,
        taskId?: Record.TaskId,
      ) {
        let active = true
        const guard = Effect.suspend(() =>
          active && !closing
            ? Effect.void
            : Effect.fail(
                new ExecutionError({
                  reason: 'revoked',
                  operation: 'hook.access',
                  message: 'Hook invocation has ended',
                }),
              ),
        )
        const native = access(id)
        return yield* protect(
          'hook.execute',
          Effect.scoped(
            lease
              .dispatchHook(event, {
                // Creation already committed: preserve its caller-visible notification failure.
                ...(event._tag === 'conversationCreated' ? { report: Effect.fail } : {}),
                registrations: lease.snapshot.hooks
                  .filter((item) => {
                    const extension = lease.snapshot.extensions.find(
                      (bound) => bound.owner.id === item.owner.id,
                    )
                    return (
                      extension === undefined ||
                      agent.extensions === undefined ||
                      agent.extensions.includes(extension.declaration.name)
                    )
                  })
                  .map((item) => item.owner.id),
              })
              .pipe(
                Effect.provideService(HookExecution, {
                  conversationId: id,
                  ...(taskId === undefined ? {} : { taskId }),
                  snapshot: (document, target) =>
                    guard.pipe(Effect.andThen(native.snapshot(document, target))),
                  watch: (document, target) =>
                    Stream.unwrap(guard.pipe(Effect.as(native.watch(document, target)))).pipe(
                      Stream.mapEffect((value) => guard.pipe(Effect.as(value))),
                    ),
                  commit: (change) =>
                    guard.pipe(
                      Effect.andThen(
                        native.commit((tx) =>
                          Effect.gen(function* () {
                            if (taskId !== undefined) {
                              const current = yield* Transaction.task(tx, taskId)
                              if (
                                Option.isNone(current) ||
                                current.value.abortRequested ||
                                current.value.state.status === 'terminal'
                              )
                                return yield* new ExecutionError({
                                  reason: 'revoked',
                                  operation: 'hook.commit',
                                  message: 'Task invocation has ended',
                                })
                            }
                            const value = yield* change(tx)
                            yield* guard
                            return value
                          }),
                        ),
                      ),
                    ),
                }),
              ),
          ),
        ).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              active = false
            }),
          ),
        )
      })
      const runHooks = (
        id: Record.ConversationId,
        agent: Agent.State,
        event: Hook.Event,
        taskId?: Record.TaskId,
      ) =>
        Effect.scoped(
          Effect.gen(function* () {
            const lease = yield* registry.acquire
            return yield* dispatchHooks(lease, id, agent, event, taskId)
          }),
        )
      const native = RunTask.make(
        (lease) => ({
          session,
          context,
          lease,
          allTools: lease.snapshot.tools
            .filter((item) => item.owner.enabled)
            .map((item) => item.declaration),
          toolExtensions: new Map(
            lease.snapshot.tools.flatMap((item) => {
              const extension = lease.snapshot.extensions.find(
                (bound) => bound.owner.id === item.owner.id,
              )
              return extension === undefined
                ? []
                : [[item.declaration.name, extension.declaration.name] as const]
            }),
          ),
          sections: lease.snapshot.sections
            .filter((item) => item.owner.enabled)
            .map((item) => ({
              section: item.declaration,
              extension: lease.snapshot.extensions.find((bound) => bound.owner.id === item.owner.id)
                ?.declaration.name,
              registration: item.owner.id,
            })),
          models,
          nativeDefault,
          maxTurns,
          defaultAgent,
          checkOpen,
          access,
          runHooks: (id, agent, event, taskId) => dispatchHooks(lease, id, agent, event, taskId),
          notify: notify(),
        }),
        registry,
      )
      yield* registry.install(native.registration)
      yield* Effect.addFinalizer(() => registry.uninstall('native-runtime-run').pipe(Effect.ignore))
      const scheduler = yield* TaskScheduler.make(session, {
        nativeRun: native.adapter,
        host: {
          open: () => Effect.succeed({}),
          reconcile: (tx, record) =>
            Effect.gen(function* () {
              if (record.kind !== runKind || record.abortRequested) return true
              const records = yield* Transaction.scanTasks(tx, {
                conversationId: record.conversationId,
                kind: runKind,
                order: 'ascending',
              }).pipe(Stream.runCollect)
              return records.find((task) => task.state.status !== 'terminal')?.id === record.id
            }),
        },
      }).pipe(Effect.provideService(Registry.Registry, registry))
      const failScheduler = Effect.fnUntraced(function* (error: HarnessError) {
        schedulerFailure = error
        for (const waiting of waiters.values())
          for (const waiter of waiting) yield* Deferred.fail(waiter, error)
        waiters.clear()
        for (const waiter of idleWaiters) yield* Deferred.fail(waiter, error)
        idleWaiters.clear()
      })
      const observe = Effect.fnUntraced(function* () {
        if (closing) return
        const failure = yield* scheduler.failure
        if (Option.isSome(failure))
          return yield* protect('runtime.scheduler', Effect.fail(failure.value))
        yield* notify()
        const tasks = yield* Session.scanTasks(session).pipe(Stream.runCollect)
        if (tasks.every((task) => task.state.status === 'terminal')) {
          for (const waiter of idleWaiters) yield* Deferred.succeed(waiter, undefined)
          idleWaiters.clear()
        }
      })
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closing = true
          yield* Scope.close(schedulerScope, Exit.void)
          yield* failScheduler(
            new HarnessError({
              reason: 'closed',
              operation: 'runtime.close',
              message: 'The owning runtime Scope is closed',
            }),
          )
        }),
      )
      yield* scheduler.changes.pipe(
        Stream.runForEach(() =>
          protect('runtime.observe', observe()).pipe(Effect.catch(failScheduler)),
        ),
        Effect.forkIn(schedulerScope),
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
              yield* scheduler.abortConversation(id)
              yield* notify()
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
      return { session, backend, registry, scheduler }
    }).pipe(
      Effect.provideService(Scope.Scope, ownedScope),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(ownedScope, exit) : Effect.void)),
    ),
  )
})
