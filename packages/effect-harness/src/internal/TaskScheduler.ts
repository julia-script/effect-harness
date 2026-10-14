import * as Tracing from './Tracing.js'
/** Session-line reservation and invocation writes; handlers and observers run off that line. */
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as DateTime from 'effect/DateTime'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as PubSub from 'effect/PubSub'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Record from '../Record.js'
import * as Settings from '../Settings.js'
import * as ContextProjection from '../ContextProjection.js'
import { ExecutionError, type Failure as ExecutionFailure } from '../ExecutionError.js'
import { HookExecution } from '../HookExecution.js'
import type * as TaskHook from '../TaskHook.js'
import * as AgentState from './AgentState.js'
import * as Supervision from './TaskSupervision.js'
import * as Tree from './TaskTree.js'
import { RegistryError } from '../RegistryError.js'
import { StorageFormatError } from '../StorageFormat.js'
import { Registry } from '../Registry.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Session from '../Session.js'
import { TaskError } from '../TaskError.js'
import { StorageError } from '../StorageError.js'
import { SessionError } from '../SessionError.js'
import * as Task from '../Task.js'
import * as ToolInvocation from '../ToolInvocation.js'
import * as NestedInvocation from './NestedInvocation.js'
import * as ConversationInvocation from './ConversationInvocation.js'
import * as Inbox from './Inbox.js'
import * as Subagent from '../Subagent.js'
import { HarnessError } from '../HarnessError.js'
import { TaskExecution, type Access } from '../TaskExecution.js'
import * as TaskPolicy from '../TaskPolicy.js'
import * as TaskResolution from '../TaskResolution.js'
import type * as Scheduler from '../TaskScheduler.js'
import * as Transaction from '../Transaction.js'
import * as SessionRuntime from './SessionRuntime.js'
import type * as TaskRegistry from './TaskRegistry.js'
import * as Transitions from './TaskTransitions.js'

interface Invocation extends Transitions.Invocation {
  ended: boolean
  readonly scope: Scope.Closeable
  readonly done: Deferred.Deferred<void>
  readonly transactions: WeakSet<Transaction.Transaction>
  toolInvocation?: ToolInvocation.Access
  subagent?: Subagent.Access
  fiber?: Fiber.Fiber<void, never>
}
const error = (reason: TaskError['reason'], operation: string, message: string, cause?: unknown) =>
  new TaskError({ reason, operation, message, ...(cause === undefined ? {} : { cause }) })
const unavailable = (operation: string) =>
  error(
    'invalidDefinition',
    operation,
    `Task scheduler requires a supplied host capability for ${operation}`,
  )
const task = Effect.fnUntraced(function* (tx: Transaction.Transaction, id: Record.TaskId) {
  const found = yield* Transaction.task(tx, id)
  if (Option.isNone(found)) return yield* error('notFound', 'task.read', 'Task is absent')
  return found.value
})
const nativeReason = (
  reason: Cause.Reason<unknown>,
): reason is Cause.Fail<StorageError | SessionError> =>
  Cause.isFailReason(reason) &&
  (Schema.is(StorageError)(reason.error) || Schema.is(SessionError)(reason.error))
const invocationFailure = (failure: Scheduler.Failure): import('../TaskError.js').Failure =>
  Schema.is(RegistryError)(failure) || Schema.is(StorageFormatError)(failure)
    ? error('faulted', 'task.hostBoundary', 'Task scheduler boundary failed', failure)
    : failure
const nativeBoundary = <A, E extends Scheduler.Failure, R>(effect: Effect.Effect<A, E, R>) => {
  const identity = (
    reason: Cause.Reason<Scheduler.Failure>,
  ): reason is Cause.Fail<import('../TaskError.js').Failure> =>
    Cause.isFailReason(reason) &&
    !Schema.is(RegistryError)(reason.error) &&
    !Schema.is(StorageFormatError)(reason.error)
  return effect.pipe(
    Effect.catchCause((cause) =>
      Effect.failCause(
        Cause.fromReasons<import('../TaskError.js').Failure>(
          cause.reasons.map((reason) => {
            if (!Cause.isFailReason(reason)) return reason
            if (identity(reason)) return reason
            return Cause.makeFailReason(invocationFailure(reason.error))
          }),
        ),
      ),
    ),
  )
}
const toolInvocationFailure = (
  failure: ToolInvocation.Failure,
): import('../TaskError.js').Failure =>
  Schema.is(ToolInvocation.InvocationError)(failure) || Schema.is(ExecutionError)(failure)
    ? error('faulted', 'task.toolInvocation', failure.message, failure)
    : invocationFailure(failure)
const subagentFailure = (failure: Subagent.Failure): import('../TaskError.js').Failure =>
  Schema.is(Subagent.SubagentError)(failure) ||
  Schema.is(HarnessError)(failure) ||
  Schema.is(ExecutionError)(failure)
    ? error('faulted', 'task.subagent', failure.message, failure)
    : invocationFailure(failure)
const sameJson = Schema.toEquivalence(Schema.Json)

const { ownedLive, hold } = Supervision

export const make = Effect.fnUntraced(function* <R = never>(
  session: Session.Session,
  options: Scheduler.Options<R> = {},
): Effect.fn.Return<Scheduler.Service, Scheduler.Failure, Registry | Scope.Scope | R> {
  const registry = yield* Registry
  const nativeRun = options.nativeRun
  if (nativeRun !== undefined) {
    yield* Task.validate(nativeRun.definition)
    if (nativeRun.definition.kind !== 'harness.run' || nativeRun.definition.version !== 1)
      return yield* error(
        'invalidDefinition',
        'scheduler.make',
        'Native adapter must declare harness.run v1',
      )
  }
  const adopts = (record: Record.Task) => record.kind === 'harness.run' && nativeRun !== undefined
  const inspectRecord = (lease: RegistryScope.Lease, record: Record.Task) =>
    adopts(record) &&
    lease.snapshot.tasks.some(
      (item) => item.declaration.kind === 'harness.run' && item.owner.enabled,
    )
      ? nativeRun!.inspect(record)
      : lease.inspectTask(record, migrations.get(record.id))
  // One parent finalizer seals writes before closing any scheduler-owned fiber.
  const ownerScope = yield* Scope.make()
  const hostContext = yield* Effect.context<R>()
  const state = yield* SessionRuntime.get(session)
  const reopened = new Set(
    (yield* Session.scanTasks(session).pipe(Stream.runCollect)).map((record) => record.id),
  )
  let recovered = false
  const settings = yield* Settings.getOrDefault
  const liveSettings = yield* Effect.serviceOption(Settings.Settings)
  const identity = {}
  yield* SessionRuntime.line(session, 'scheduler.make', () =>
    Effect.gen(function* () {
      if (state.scheduler !== undefined)
        return yield* error(
          'invalidDefinition',
          'scheduler.make',
          'Session already has an active custom-task scheduler',
        )
      state.scheduler = identity
    }),
  )
  const invocations = new Map<Record.TaskId, Invocation>()
  const supervisorDone = yield* Deferred.make<void>()
  const migrations = new Map<Record.TaskId, TaskResolution.MigrationFailure>()
  const signals = yield* SessionRuntime.reconcile(session)
  const publications = yield* PubSub.sliding<void>(1)
  let enabled = false
  let closed = false
  let failed: Scheduler.Failure | undefined
  const wake = Queue.offer(signals, undefined).pipe(Effect.asVoid)
  const check = Effect.suspend(() =>
    closed
      ? Effect.fail(error('revoked', 'scheduler.access', 'Task scheduler scope has ended'))
      : Effect.void,
  )
  const notify = PubSub.publish(publications, undefined).pipe(Effect.asVoid)
  const reconciliationIdentity = {}
  let reconciliationEnded = false
  let pendingCoordinatorEnd:
    | {
        readonly token: object
        readonly coordinator: typeof reconciliationIdentity
        readonly session: typeof state
        readonly poison: StorageError | SessionError
        readonly cause: Cause.Cause<unknown>
      }
    | undefined
  type Occurrence = {
    readonly token: object
    readonly diagnostic: TaskError
    readonly session: typeof state
    readonly operation: 'task.read' | 'task.scan' | 'task.conversationGuard'
    readonly cause: Cause.Cause<unknown>
  }
  let nativeDiagnostic:
    | {
        readonly provenance: object
        readonly cause: Cause.Cause<unknown>
        readonly diagnostic: TaskError
        readonly occurrences: WeakMap<Cause.Fail<unknown>, Occurrence>
        readonly poison: StorageError | SessionError | undefined
      }
    | undefined
  const samePoison = () =>
    nativeDiagnostic !== undefined &&
    failed === nativeDiagnostic.diagnostic &&
    nativeDiagnostic.poison !== undefined &&
    state.poison === nativeDiagnostic.poison
  const health = (failure: Scheduler.Failure, occurrence?: Occurrence) =>
    Effect.sync(() => {
      if (
        occurrence !== undefined &&
        occurrence.session === state &&
        occurrence.diagnostic === nativeDiagnostic?.diagnostic &&
        samePoison() &&
        failure === nativeDiagnostic?.poison
      )
        return
      if (failure !== nativeDiagnostic?.diagnostic) nativeDiagnostic = undefined
      failed = failure
    }).pipe(Effect.andThen(notify))
  const observeProjection =
    (operation: Occurrence['operation']) =>
    <A, E>(exit: Exit.Exit<A, E>): Effect.Effect<void> =>
      Effect.sync(() => {
        const diagnostic = nativeDiagnostic
        if (diagnostic === undefined || !samePoison() || Exit.isSuccess(exit)) return
        if (exit.cause.reasons.length !== 1) return
        const reason = exit.cause.reasons[0]!
        if (!nativeReason(reason) || reason.error !== diagnostic.poison) return
        diagnostic.occurrences.set(reason, {
          token: {},
          diagnostic: diagnostic.diagnostic,
          session: state,
          operation,
          cause: exit.cause,
        })
      }).pipe(Effect.ignoreCause)
  const occurrence = (reason: Cause.Reason<unknown>): Occurrence | undefined => {
    if (!Cause.isFailReason(reason)) return undefined
    const found = nativeDiagnostic?.occurrences.get(reason)
    nativeDiagnostic?.occurrences.delete(reason)
    return found
  }
  const observeCommit =
    (provenance: object, operation: 'task.commit' | 'task.conversationCommit') =>
    <A, E>(exit: Exit.Exit<A, E>): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (Exit.isSuccess(exit) || exit.cause.reasons.length < 2) return
        const native = exit.cause.reasons.flatMap((reason) =>
          Cause.isFailReason(reason) &&
          (Schema.is(StorageError)(reason.error) || Schema.is(SessionError)(reason.error))
            ? [reason.error]
            : [],
        )
        if (native.length === 0) return
        const diagnostic = error('faulted', operation, Cause.pretty(exit.cause), exit.cause)
        const poison = native.find((failure) => failure === state.poison)
        if (poison !== undefined && !reconciliationEnded)
          pendingCoordinatorEnd = {
            token: {},
            coordinator: reconciliationIdentity,
            session: state,
            poison,
            cause: exit.cause,
          }
        nativeDiagnostic = {
          provenance,
          cause: exit.cause,
          diagnostic,
          occurrences: new WeakMap(),
          poison,
        }
        yield* health(diagnostic)
      }).pipe(Effect.ignoreCause)

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      closed = true
      enabled = false
      // Sealing is synchronous before waiting for any invocation cleanup.
      for (const invocation of invocations.values()) invocation.ended = true
      yield* Deferred.succeed(supervisorDone, undefined)
      yield* Scope.close(ownerScope, Exit.void)
      if (state.scheduler === identity) state.scheduler = undefined
      yield* PubSub.shutdown(publications)
    }),
  )

  const read = (id: Record.TaskId) =>
    Session.task(session, id).pipe(
      Effect.onExit(observeProjection('task.read')),
      Effect.flatMap((found) =>
        Effect.fromOption(found).pipe(
          Effect.mapError(() => error('notFound', 'task.read', 'Task is absent')),
        ),
      ),
    )
  const outcome = Effect.fnUntraced(function* <T extends Task.Any>(
    ref: Task.Ref<T>,
  ): Effect.fn.Return<
    Option.Option<Task.Outcome<T>>,
    Scheduler.Failure,
    T['success']['DecodingServices'] | T['failure']['DecodingServices']
  > {
    const record = yield* read(ref.id)
    yield* check
    if (record.kind !== ref.definition.kind)
      return yield* error(
        'invalidDefinition',
        'task.outcome',
        'Task reference contract differs from the stored task',
      )
    if (record.state.status !== 'terminal') return Option.none()
    if (record.version !== ref.definition.version)
      return yield* error(
        'invalidDefinition',
        'task.outcome',
        'Terminal task version differs from the reference contract',
      )
    const wire = yield* Task.decode(Record.TaskOutcome, record.state.outcome)
    if (wire.status === 'completed')
      return Option.some({
        status: 'completed',
        result: yield* Task.decode<T['success']>(ref.definition.success, wire.result),
      })
    if (wire.status === 'failed')
      return Option.some({
        status: 'failed',
        error: wire.error,
        result: yield* Task.decode<T['failure']>(ref.definition.failure, wire.result),
      })
    return Option.some(wire)
  })
  const guard = check.pipe(Effect.andThen(state.requireSchedulerFormat), Effect.asVoid)
  const resumeInvocation = Effect.fn('TaskScheduler.resume')(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan('scheduler.recovery', !recovered)
        yield* restore(guard)
        yield* restore(
          Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const records = yield* Transaction.scanTasks(tx).pipe(Stream.runCollect)
              for (const record of records) {
                if (
                  (record.kind === 'harness.run' && nativeRun === undefined) ||
                  record.state.status === 'terminal' ||
                  invocations.has(record.id)
                )
                  continue
                let next = record
                if (!recovered && reopened.has(record.id)) next = TaskPolicy.recover(record)
                else if (record.state.status === 'running')
                  next = { ...record, state: { ...record.state, status: 'pending' } }
                if (next !== record) yield* Transaction.putTask(tx, next)
              }
            }),
          ),
        )
        recovered = true
        failed = undefined
        enabled = true
        yield* wake
      }),
    ),
  )
  const resume = resumeInvocation()
  const wait: Scheduler.Service['wait'] = (ref) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* check
        const subscription = yield* PubSub.subscribe(publications)
        yield* resume
        for (;;) {
          const found = yield* outcome(ref)
          if (Option.isSome(found)) return found.value
          if (failed !== undefined) return yield* failed
          yield* PubSub.take(subscription)
        }
      }),
    )

  const waitForTask = (id: Record.TaskId) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* check
        const subscription = yield* PubSub.subscribe(publications)
        yield* resume
        for (;;) {
          const record = yield* read(id)
          yield* check
          if (record.state.status === 'terminal') return record
          if (failed !== undefined) return yield* failed
          yield* PubSub.take(subscription)
        }
      }),
    )
  const outcomes = (ids: ReadonlyArray<Record.TaskId>) =>
    check.pipe(
      Effect.andThen(
        Session.commit(session, (tx) =>
          Effect.forEach(ids, (id) =>
            Effect.gen(function* () {
              const record = yield* task(tx, id)
              if (record.state.status !== 'terminal')
                return yield* error(
                  'invalidCheckpoint',
                  'task.outcomes',
                  'Task receipt is not terminal',
                )
              return yield* Task.decode(Record.TaskOutcome, record.state.outcome)
            }),
          ),
        ),
      ),
    )
  const abortSet = (
    id: Record.TaskId,
    includeSelf: boolean,
    abortOptions?: { readonly includeBackground?: boolean },
    gate?: (tx: Transaction.Transaction) => Effect.Effect<void, import('../TaskError.js').Failure>,
  ) =>
    Effect.gen(function* () {
      yield* resume
      const captured = yield* Session.commit(session, (tx) =>
        Effect.gen(function* () {
          yield* check
          if (gate !== undefined) yield* gate(tx)
          const root = yield* task(tx, id)
          const tree = yield* Tree.load(tx)
          const members = Tree.owned(tree, id, abortOptions?.includeBackground).filter(
            (record) => record.state.status !== 'terminal',
          )
          if (includeSelf && root.state.status !== 'terminal') members.unshift(root)
          for (const record of members)
            yield* Transaction.putTask(tx, TaskPolicy.requestAbort(record))
          return members.map((record) => record.id)
        }),
      )
      enabled = true
      yield* wake
      for (const member of captured) yield* waitForTask(member)
    }).pipe(Effect.mapError(invocationFailure))
  const abort: Scheduler.Service['abort'] = (id, options) => abortSet(id, true, options)
  const markConversation = (
    tx: Transaction.Transaction,
    id: Record.ConversationId,
    options?: { readonly includeBackground?: boolean },
    gate?: (tx: Transaction.Transaction) => Effect.Effect<void, import('../TaskError.js').Failure>,
  ) =>
    Effect.gen(function* () {
      if (gate !== undefined) yield* gate(tx)
      yield* check
      if (Option.isNone(yield* Transaction.conversation(tx, id)))
        return yield* error('notFound', 'task.abortConversation', 'Conversation is absent')
      const tree = yield* Tree.load(tx)
      const members = [...tree.tasks.values()].filter(
        (record) =>
          record.state.status !== 'terminal' &&
          Tree.inConversation(tree, record, id, options?.includeBackground),
      )
      for (const record of members) yield* Transaction.putTask(tx, TaskPolicy.requestAbort(record))
      const submissions = yield* Transaction.scanSubmissions(tx, { status: 'queued' }).pipe(
        Stream.runCollect,
      )
      for (const submission of submissions) {
        if (submission._tag !== 'InputQueued') continue
        const ownerId = tree.conversations.get(submission.conversationId)?.owner?.taskId
        const owner = ownerId === undefined ? undefined : tree.tasks.get(ownerId)
        const included =
          submission.conversationId === id ||
          (owner !== undefined && Tree.inConversation(tree, owner, id, options?.includeBackground))
        if (included)
          yield* Transaction.putSubmission(tx, {
            ...submission,
            _tag: 'InputUnanswered',
            status: 'unanswered',
            reason: 'aborted',
          })
      }
      return members.map((record) => record.id)
    })
  const joinMarked = (ids: ReadonlyArray<Record.TaskId>) =>
    Effect.gen(function* () {
      yield* wake
      for (const id of ids) yield* waitForTask(id)
    })
  const abortConversation: Scheduler.Service['abortConversation'] = (id, options) =>
    Effect.gen(function* () {
      yield* resume
      const captured = yield* Session.commit(session, (tx) => markConversation(tx, id, options))
      yield* joinMarked(captured)
    })
  const graph = check.pipe(
    Effect.andThen(
      Session.commit(session, (tx) => Tree.load(tx).pipe(Effect.map(Supervision.graph))),
    ),
  )
  const watchGraph = Stream.unwrap(
    Effect.gen(function* () {
      yield* check
      const captured = yield* SessionRuntime.line(session, 'task.watchGraph', (state) =>
        Effect.gen(function* () {
          const subscriber = yield* SessionRuntime.subscribe(state)
          const tasks = yield* state.storage.scanTasks().pipe(Stream.runCollect)
          const conversations = yield* state.storage.scanConversations().pipe(Stream.runCollect)
          return { subscriber, tasks, conversations }
        }),
      )
      const tasks = new Map(captured.tasks.map((record) => [record.id, record]))
      const conversations = new Map(captured.conversations.map((record) => [record.id, record]))
      const snapshot = () => Supervision.graph({ tasks, conversations })
      return Stream.concat(
        Stream.succeed<Scheduler.GraphFrame>({ snapshot: snapshot() }),
        Stream.fromQueue(captured.subscriber.queue).pipe(
          Stream.filter((event) => event._tag === 'commit'),
          Stream.map((event): Scheduler.GraphFrame => {
            for (const change of event.value.changes) {
              if (change._tag === 'task') tasks.set(change.value.id, change.value)
              else if (change._tag === 'conversation')
                conversations.set(change.value.id, change.value)
            }
            return { snapshot: snapshot(), seq: event.value.seq }
          }),
        ),
      )
    }),
  ).pipe(
    Stream.interruptWhen(
      Deferred.await(supervisorDone).pipe(
        Effect.andThen(
          Effect.fail(error('revoked', 'task.watchGraph', 'Task supervisor scope has ended')),
        ),
      ),
    ),
  )

  const createRegistered = Effect.fnUntraced(function* <T extends Task.Any>(
    tx: Transaction.Transaction,
    lease: RegistryScope.Lease,
    definition: T,
    input: Task.Input<T>,
    options: {
      readonly conversationId: Record.ConversationId
      readonly owner?: Record.TaskId
      readonly background: boolean
      readonly restartPolicy?: TaskPolicy.RestartPolicy
    },
  ): Effect.fn.Return<Task.Ref<T>, import('../TaskError.js').Failure> {
    if (options.owner !== undefined) {
      const owner = yield* task(tx, options.owner)
      if (options.background || options.conversationId !== owner.conversationId)
        return yield* error(
          'invalidDefinition',
          'task.createChild',
          'Owned children must share their owner conversation and cannot be background',
        )
    }
    const installed = yield* lease
      .resolveTask(definition.kind)
      .pipe(Effect.mapError(invocationFailure))
    const contract = installed.declaration
    if (
      contract.version !== definition.version ||
      contract.input !== definition.input ||
      contract.checkpoint !== definition.checkpoint ||
      contract.success !== definition.success ||
      contract.failure !== definition.failure
    )
      return yield* error(
        'invalidDefinition',
        'task.createChild',
        'Child requires the installed task contract',
      )
    const wire = yield* lease
      .prepareTask(definition.kind, input)
      .pipe(Effect.mapError(invocationFailure))
    const created = yield* Transaction.createTask(tx, {
      conversationId: options.conversationId,
      ...(options.owner === undefined ? {} : { owner: options.owner }),
      kind: definition.kind,
      version: definition.version,
      input: wire.input,
      state: { status: 'pending', checkpoint: wire.checkpoint },
      background: options.background,
      restartPolicy: options.restartPolicy ?? 'resume',
      schedulerFormat: { version: 1, minimumReader: 1 },
      abortRequested: false,
    })
    return { id: created.id, definition }
  })

  const current = (tx: Transaction.Transaction, invocation: Invocation) =>
    Effect.gen(function* () {
      if (closed || invocations.get(invocation.taskId) !== invocation)
        return yield* error('revoked', 'task.invocation', 'Task reservation is no longer active')
      const record = yield* task(tx, invocation.taskId)
      yield* Transitions.gate(record, invocation)
      return record
    })
  const executionFailure = (failure: import('../TaskError.js').Failure): ExecutionFailure =>
    Schema.is(TaskError)(failure)
      ? new ExecutionError({
          reason: failure.reason === 'revoked' ? 'revoked' : 'invalid',
          operation: failure.operation,
          message: failure.message,
          cause: failure,
        })
      : failure
  const hookAccess = (
    invocation: Invocation,
    native: Access,
  ): Context.Service.Shape<typeof HookExecution> => {
    const available = check.pipe(
      Effect.andThen(
        Effect.suspend(() =>
          invocations.get(invocation.taskId) === invocation
            ? Effect.void
            : Effect.fail(error('revoked', 'task.hookAccess', 'Task hook invocation has ended')),
        ),
      ),
      Effect.mapError(executionFailure),
    )
    return {
      taskId: invocation.taskId,
      conversationId: invocation.conversationId,
      snapshot: (document, target) =>
        available.pipe(Effect.andThen(Session.snapshot(session, document, target))),
      watch: (document, target) =>
        Stream.unwrap(available.pipe(Effect.as(Session.watch(session, document, target)))).pipe(
          Stream.interruptWhen(
            Deferred.await(invocation.done).pipe(
              Effect.andThen(
                Effect.fail(
                  new ExecutionError({
                    reason: 'revoked',
                    operation: 'task.hookWatch',
                    message: 'Task hook invocation has ended',
                  }),
                ),
              ),
            ),
          ),
        ),
      commit: (change) =>
        native
          .commit(change)
          .pipe(
            Effect.mapError((failure) =>
              Schema.is(TaskError)(failure) ? executionFailure(failure) : failure,
            ),
          ),
    }
  }
  const access = (
    invocation: Invocation,
    lease: RegistryScope.Lease,
    host: Scheduler.HostAccess,
  ): Access => {
    const provenance = {}
    const active = check.pipe(
      Effect.andThen(
        Effect.suspend(() =>
          invocation.ended || invocations.get(invocation.taskId) !== invocation
            ? Effect.fail(error('revoked', 'task.invocation', 'Task invocation has ended'))
            : Effect.void,
        ),
      ),
    )
    const withRead = <A, E, RRead>(effect: Effect.Effect<A, E, RRead>) =>
      active.pipe(Effect.andThen(effect))
    const transaction = (tx: Transaction.Transaction) =>
      Effect.gen(function* () {
        if (!invocation.transactions.has(tx))
          return yield* error(
            'revoked',
            'task.transaction',
            'Use the Transaction supplied by this invocation commit',
          )
        return yield* current(tx, invocation)
      })
    const scopedStream = <A, E, RStream>(stream: Stream.Stream<A, E, RStream>) =>
      Stream.unwrap(active.pipe(Effect.as(stream))).pipe(
        Stream.interruptWhen(Deferred.await(invocation.done)),
      )
    const declaration =
      invocation.kind === 'harness.run' && nativeRun !== undefined
        ? nativeRun.definition
        : lease.snapshot.tasks.find((item) => item.declaration.kind === invocation.kind)!
            .declaration
    const checkDefinition = (definition: Task.Contract) =>
      definition.kind === invocation.kind &&
      definition.version === invocation.version &&
      definition.input === declaration.input &&
      definition.checkpoint === declaration.checkpoint &&
      definition.success === declaration.success &&
      definition.failure === declaration.failure
        ? Effect.void
        : Effect.fail(
            error(
              'invalidDefinition',
              'task.progress',
              'Progress contract differs from the reservation',
            ),
          )
    const native: Access = {
      invocation: ToolInvocation.unsupported,
      subagent: Subagent.unsupported,
      taskId: invocation.taskId,
      conversationId: invocation.conversationId,
      registry: {
        tasks: lease.snapshot.tasks.map((item) => item.declaration),
        extensions: lease.snapshot.extensions.map((item) => item.declaration),
        tools: lease.snapshot.tools.map((item) => item.declaration),
        hooks: lease.snapshot.hooks.map((item) => item.declaration),
      },
      agent: withRead(
        host.agent ??
          Session.snapshot(
            session,
            AgentState.document,
            AgentState.target(invocation.conversationId),
          ).pipe(
            Effect.map(Option.match({ onNone: () => ({}), onSome: (snapshot) => snapshot.value })),
          ),
      ),
      settings: withRead(
        host.settings ??
          (Option.isSome(liveSettings) ? liveSettings.value.get : Effect.succeed(settings)),
      ),
      conversation: (id) =>
        withRead(
          host.conversation === undefined
            ? Effect.fail(unavailable('conversation'))
            : host.conversation(id),
        ),
      commit: (change) =>
        active.pipe(
          Effect.andThen(
            Session.commit(
              session,
              (tx) =>
                Effect.gen(function* () {
                  yield* current(tx, invocation)
                  invocation.transactions.add(tx)
                  return yield* Effect.suspend(() => change(tx)).pipe(
                    // Scope closure can occur while a caller masks interruption inside its
                    // callback. Revoke the whole draft before Session flushes domain writes.
                    Effect.tap(() => active),
                    Effect.ensuring(Effect.sync(() => invocation.transactions.delete(tx))),
                  )
                }),
              { taskId: invocation.taskId, conversationId: invocation.conversationId },
            ).pipe(Effect.onExit(observeCommit(provenance, 'task.commit'))),
          ),
        ),
      progress: <T extends Task.Contract>(
        tx: Transaction.Transaction,
        definition: T,
        next: Task.NextState<T>,
      ) =>
        Effect.gen(function* () {
          const record = yield* transaction(tx)
          yield* checkDefinition(definition)
          if (invocation.mode === 'abort' && next.status !== 'completing')
            return yield* error(
              'invalidCheckpoint',
              'task.progress',
              'Abort cleanup must commit a terminal outcome',
            )
          if (next.status === 'completing') {
            yield* NestedInvocation.stageClose(
              invocation.toolInvocation ?? ToolInvocation.unsupported,
              tx,
            ).pipe(Effect.mapError(toolInvocationFailure))
            if (next.outcome.status !== 'completed')
              yield* ConversationInvocation.stageClose(invocation.subagent, tx).pipe(
                Effect.mapError(subagentFailure),
              )
            const encoded = yield* Task.encode(
              Task.outcomeSchema<T['success'], T['failure']>(definition),
              next.outcome,
            )
            const outcome = yield* Task.decode(Record.TaskOutcome, encoded)
            yield* hold(tx, record, outcome)
          } else {
            const checkpoint = yield* Task.encodeCheckpoint<T['checkpoint']>(
              definition.checkpoint,
              next.checkpoint,
            )
            if (next.status === 'waiting') {
              yield* Supervision.validateWait(tx, record, next)
              if (host.validateWait !== undefined) yield* host.validateWait(tx, record, next)
            }
            yield* Transaction.putTask(
              tx,
              TaskPolicy.withState(
                record,
                next.status === 'pending'
                  ? { status: 'pending', checkpoint }
                  : { status: 'waiting', checkpoint, on: next.on, policy: next.policy },
              ),
            )
          }
        }),
      memo: (key, schema) =>
        withRead(
          read(invocation.taskId).pipe(
            Effect.flatMap((record) => {
              const memo = Transitions.memo(record, key)
              return Option.isNone(memo)
                ? Effect.succeedNone
                : Task.decode(schema, memo.value).pipe(Effect.asSome)
            }),
          ),
        ),
      memoize: (tx, key, schema, value) =>
        Effect.gen(function* () {
          const record = yield* transaction(tx)
          const memo = yield* Transitions.memoize(record, key, schema, value)
          yield* Transaction.putTask(tx, memo.record)
          return memo.value
        }),
      createChild: (tx, definition, input, options) =>
        transaction(tx).pipe(
          Effect.flatMap((record) =>
            createRegistered(tx, lease, definition, input, {
              conversationId: options?.conversationId ?? record.conversationId,
              owner: record.id,
              background: options?.background ?? false,
              ...(options?.restartPolicy === undefined
                ? {}
                : { restartPolicy: options.restartPolicy }),
            }),
          ),
        ),
      createBackgroundSupervisor: (tx, definition, input, options) =>
        transaction(tx).pipe(
          Effect.flatMap((record) =>
            createRegistered(tx, lease, definition, input, {
              conversationId: record.conversationId,
              background: true,
              ...(options?.restartPolicy === undefined
                ? {}
                : { restartPolicy: options.restartPolicy }),
            }),
          ),
        ),
      createConversation: (tx, options) =>
        transaction(tx).pipe(
          Effect.flatMap((record) => Supervision.createConversation(tx, record, options)),
        ),
      createConversationTask: (tx, conversationId, definition, input, options) =>
        transaction(tx).pipe(
          Effect.flatMap((record) =>
            Effect.gen(function* () {
              const conversation = yield* Transaction.conversation(tx, conversationId)
              if (Option.isNone(conversation) || conversation.value.owner?.taskId !== record.id)
                return yield* error(
                  'invalidDefinition',
                  'task.createConversationTask',
                  'Conversation must be owned by the invoking task',
                )
              return yield* createRegistered(tx, lease, definition, input, {
                conversationId,
                background: options?.background ?? false,
                ...(options?.restartPolicy === undefined
                  ? {}
                  : { restartPolicy: options.restartPolicy }),
              })
            }),
          ),
        ),
      outcomes: (ids) => withRead(outcomes(ids)),
      outcome: (ref) => nativeBoundary(withRead(outcome(ref))),
      wait: (ref) =>
        Effect.raceFirst(
          nativeBoundary(withRead(wait(ref))),
          Deferred.await(invocation.done).pipe(Effect.andThen(Effect.interrupt)),
        ),
      abort: (id, options) =>
        Effect.raceFirst(
          withRead(
            abortSet(id, true, options, (tx) =>
              Effect.gen(function* () {
                yield* current(tx, invocation)
                const record = yield* task(tx, id)
                if (record.owner !== invocation.taskId)
                  return yield* error(
                    'invalidDefinition',
                    'task.abortOwned',
                    'Abort requires a directly owned task',
                  )
              }),
            ),
          ),
          Deferred.await(invocation.done).pipe(Effect.andThen(Effect.interrupt)),
        ),
      abortOwned: (options) =>
        Effect.raceFirst(
          withRead(
            abortSet(invocation.taskId, false, options, (tx) =>
              current(tx, invocation).pipe(Effect.asVoid),
            ),
          ),
          Deferred.await(invocation.done).pipe(Effect.andThen(Effect.interrupt)),
        ),
      entry: (id) =>
        withRead(
          Session.entry(session, id, { conversationId: invocation.conversationId }).pipe(
            Effect.map(Option.map((stored) => stored.entry)),
          ),
        ),
      entries: scopedStream(
        Session.scanEntries(session, { conversationId: invocation.conversationId }),
      ),
      context: withRead(
        host.context ??
          ContextProjection.read(session, invocation.conversationId).pipe(
            Effect.map((view) => view.activeEntries),
            Effect.mapError((cause) =>
              error('faulted', 'task.context', 'Context projection failed', cause),
            ),
          ),
      ),
      snapshot: (document, target) => withRead(Session.snapshot(session, document, target)),
      snapshotAsOf: (document, target, at) =>
        withRead(Session.snapshotAsOf(session, document, target, at)),
      watch: (document, target) => scopedStream(Session.watch(session, document, target)),
      dispatchHook: (event) =>
        withRead(
          host.dispatchHook === undefined
            ? lease.dispatchHook(event).pipe(
                Effect.provideService(HookExecution, hookAccess(invocation, native)),
                Effect.mapError((cause) =>
                  error('faulted', 'task.dispatchHook', 'Native hook dispatch failed', cause),
                ),
              )
            : host.dispatchHook(event),
        ),
      sleepUntil: (instant) =>
        Effect.raceFirst(
          withRead(
            Effect.gen(function* () {
              for (;;) {
                const remaining = DateTime.toEpochMillis(instant) - (yield* Clock.currentTimeMillis)
                if (remaining <= 0) return
                yield* Effect.sleep(Math.min(remaining, 2147483647))
                yield* active
              }
            }),
          ),
          Deferred.await(invocation.done).pipe(Effect.andThen(Effect.interrupt)),
        ),
    }
    NestedInvocation.registerGate(native, (tx) => transaction(tx).pipe(Effect.asVoid))
    ConversationInvocation.registerControl(native, {
      gate: (tx) => transaction(tx).pipe(Effect.asVoid),
      savepoint: (tx, run) =>
        transaction(tx).pipe(
          Effect.andThen(
            Inbox.atomic(tx, (derived) =>
              Effect.gen(function* () {
                invocation.transactions.add(derived)
                return yield* transaction(derived).pipe(
                  Effect.andThen(Effect.suspend(() => run(derived))),
                  Effect.tap(() => transaction(derived)),
                  Effect.ensuring(Effect.sync(() => invocation.transactions.delete(derived))),
                )
              }),
            ),
          ),
        ),
      mutation: (change) =>
        Effect.suspend(() => {
          let callbackEntered = false
          return Session.commit(session, (tx) =>
            Effect.gen(function* () {
              callbackEntered = true
              yield* current(tx, invocation)
              invocation.transactions.add(tx)
              return yield* Effect.suspend(() => change(tx)).pipe(
                Effect.tap(() => current(tx, invocation)),
                Effect.ensuring(Effect.sync(() => invocation.transactions.delete(tx))),
              )
            }),
          ).pipe(
            Effect.onExit(observeCommit(provenance, 'task.conversationCommit')),
            Effect.onExit((exit) =>
              callbackEntered ? Effect.void : observeProjection('task.conversationGuard')(exit),
            ),
          )
        }),
      markConversation: (tx, id, options) =>
        Effect.gen(function* () {
          yield* transaction(tx)
          const tree = yield* Tree.load(tx)
          if (
            Tree.inConversation(
              tree,
              yield* task(tx, invocation.taskId),
              id,
              options?.includeBackground,
            )
          )
            return yield* error(
              'invalidDefinition',
              'task.abortConversation',
              'Abort would wait for its own caller',
            )
          const ids = yield* markConversation(tx, id, options, (tx) =>
            transaction(tx).pipe(Effect.asVoid),
          )
          return { ids, join: joinMarked(ids).pipe(Effect.mapError(invocationFailure)) }
        }),
      ended: Deferred.await(invocation.done),
    })
    return native
  }

  const finish = (invocation: Invocation, before: Record.Task, result: TaskRegistry.PhaseResult) =>
    Session.commit(session, (tx) =>
      Effect.gen(function* () {
        const record = yield* task(tx, invocation.taskId)
        if (closed || invocations.get(record.id) !== invocation || invocation.ended) return
        // Commit's current status and durable abort mark win over the handler's result.
        if (
          record.state.status === 'terminal' ||
          record.state.status === 'completing' ||
          record.state.status === 'waiting'
        )
          return
        if (invocation.mode === 'run' && record.abortRequested) return
        if (result._tag === 'interrupted') {
          if (record.state.status === 'running')
            yield* Transaction.putTask(tx, {
              ...record,
              state: { ...record.state, status: 'pending' },
            })
          return
        }
        if (
          record.state.status === 'pending' &&
          !sameJson(before.state.checkpoint!, record.state.checkpoint!)
        )
          return
        if (result._tag === 'outcome') yield* hold(tx, record, result.outcome)
        else
          yield* hold(tx, record, {
            status: 'faulted',
            error: {
              message:
                invocation.mode === 'abort'
                  ? 'Abort handler returned without a terminal outcome'
                  : 'Task phase returned without durable checkpoint progress',
            },
          })
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            invocation.ended = true
          }),
        ),
      ),
    )

  // Cause identity is used by durable fault provenance; add a span without stack annotations.
  const run = Effect.fnUntraced(function* (
    invocation: Invocation,
    record: Record.Task,
    lease: RegistryScope.Lease,
  ) {
    yield* Effect.annotateCurrentSpan({
      'task.id': record.id,
      'conversation.id': record.conversationId,
      'task.kind': record.kind,
      'task.version': record.version,
      'task.mode': invocation.mode,
    })
    let cleanupExit: Exit.Exit<unknown, unknown> = Exit.void
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const host =
          options.host === undefined
            ? {}
            : yield* restore(
                options.host
                  .open({
                    taskId: invocation.taskId,
                    conversationId: invocation.conversationId,
                    registry: lease.snapshot,
                    mode: invocation.mode,
                  })
                  .pipe(
                    Effect.updateContext((_context: Context.Context<never>) =>
                      Context.add(hostContext, Scope.Scope, invocation.scope),
                    ),
                  ),
              )
        const reserved = access(invocation, lease, host)
        const toolInvocation =
          options.invocation === undefined
            ? ToolInvocation.unsupported
            : yield* restore(
                options.invocation.open(reserved, lease, invocation.mode).pipe(
                  Effect.updateContext((_context: Context.Context<never>) =>
                    Context.add(hostContext, Scope.Scope, invocation.scope),
                  ),
                  Effect.mapError(toolInvocationFailure),
                ),
              )
        const subagent =
          options.subagent === undefined
            ? Subagent.unsupported
            : yield* restore(
                options.subagent.open(reserved, lease, invocation.mode).pipe(
                  Effect.updateContext((_context: Context.Context<never>) =>
                    Context.add(hostContext, Scope.Scope, invocation.scope),
                  ),
                  Effect.mapError(subagentFailure),
                ),
              )
        const native: Access = {
          ...reserved,
          invocation: toolInvocation,
          subagent,
          ...(options.subagent === undefined
            ? {}
            : {
                conversation: (id: Record.ConversationId) =>
                  subagent.conversation(id).pipe(Effect.mapError(subagentFailure)),
              }),
        }
        invocation.subagent = subagent
        invocation.toolInvocation = toolInvocation
        const checkpoint = yield* Task.decode(
          Schema.Struct({ phase: Schema.NonEmptyString }),
          record.state.checkpoint,
        )
        yield* Effect.annotateCurrentSpan('task.phase', checkpoint.phase)
        // These notifications describe an invocation's lifecycle, including a held
        // completion. Task terminal state remains exclusively a persisted transition.
        const lifecycle = (stage: TaskHook.Stage, detail?: Schema.Json) =>
          lease
            .dispatchTaskHook({
              kind: record.kind,
              phase: checkpoint.phase,
              stage,
              taskId: record.id,
              conversationId: record.conversationId,
              input: record.input,
              checkpoint: record.state.checkpoint!,
              ...(detail === undefined ? {} : { detail }),
            })
            .pipe(
              Effect.provideService(HookExecution, hookAccess(invocation, native)),
              Effect.mapError((cause) =>
                error('faulted', 'task.lifecycleHook', 'Native task lifecycle hook failed', cause),
              ),
            )
        yield* restore(lifecycle(invocation.mode === 'abort' ? 'abort' : 'beforePhase'))
        const exit = yield* restore(
          (adopts(record)
            ? nativeRun!
                .invoke(record, invocation.mode, lease)
                .pipe(
                  Effect.updateContext((input: Context.Context<TaskExecution | Scope.Scope>) =>
                    Context.merge(hostContext, input),
                  ),
                )
            : lease.invokeTask(record, invocation.mode)
          ).pipe(Effect.provideService(TaskExecution, native), Scope.provide(invocation.scope)),
        ).pipe(Effect.exit)
        if (
          Exit.isFailure(exit) &&
          exit.cause.reasons.length === 1 &&
          Cause.isFailReason(exit.cause.reasons[0]!)
        ) {
          yield* health(exit.cause.reasons[0].error, occurrence(exit.cause.reasons[0]))
          return
        }
        let result: TaskRegistry.PhaseResult
        if (Exit.isSuccess(exit)) result = exit.value
        else if (Cause.hasInterruptsOnly(exit.cause)) result = { _tag: 'interrupted' }
        else
          result = {
            _tag: 'outcome',
            outcome: { status: 'faulted', error: { message: Cause.pretty(exit.cause) } },
          }
        yield* Effect.annotateCurrentSpan(
          'task.result',
          result._tag === 'outcome' ? result.outcome.status : result._tag,
        )
        const after = yield* read(invocation.taskId)
        yield* restore(
          NestedInvocation.close(
            native.invocation,
            after.state.status === 'completing' || result._tag === 'outcome',
          ).pipe(Effect.mapError(toolInvocationFailure)),
        )
        const detail = result._tag === 'outcome' ? result.outcome : after.state.outcome
        if (detail !== undefined && Schema.is(Record.TaskOutcome)(detail))
          yield* Effect.annotateCurrentSpan('task.outcome', detail.status)
        yield* restore(
          ConversationInvocation.close(
            native.subagent,
            detail !== undefined &&
              Schema.is(Record.TaskOutcome)(detail) &&
              detail.status !== 'completed',
          ).pipe(Effect.mapError(subagentFailure)),
        )
        yield* restore(
          lifecycle(
            detail !== undefined &&
              Schema.is(Record.TaskOutcome)(detail) &&
              detail.status !== 'completed'
              ? 'failure'
              : 'afterPhase',
            detail,
          ),
        )
        yield* finish(invocation, record, result)
        yield* restore(lifecycle('settled', (yield* read(invocation.taskId)).state.outcome))
        if (result._tag === 'interrupted' && !closed) {
          const latest = yield* read(invocation.taskId)
          if (!latest.abortRequested)
            yield* health(
              error(
                'revoked',
                'scheduler.invoke',
                'Task phase was interrupted; resume retries its durable checkpoint',
              ),
            )
        }
      }),
    ).pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          cleanupExit = exit
        }),
      ),
      Effect.catchCause((cause) => {
        if (cause.reasons.length === 1 && Cause.isFailReason(cause.reasons[0]!)) {
          const reason = cause.reasons[0]
          return health(reason.error, occurrence(reason))
        }
        return Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : health(error('faulted', 'scheduler.invoke', Cause.pretty(cause), cause))
      }),
      Effect.ensuring(
        Effect.uninterruptible(
          Effect.gen(function* () {
            invocation.ended = true
            const released = yield* Scope.close(invocation.scope, cleanupExit).pipe(Effect.exit)
            invocations.delete(invocation.taskId)
            yield* Deferred.succeed(invocation.done, undefined)
            if (Exit.isFailure(released))
              yield* health(
                error('faulted', 'scheduler.release', Cause.pretty(released.cause), released.cause),
              )
            yield* notify
            if (!closed) yield* wake
          }),
        ),
      ),
    )
  }, Tracing.withSpan('TaskScheduler.invoke'))

  const reserve = (id: Record.TaskId) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const scope = yield* Scope.make()
        let invocation: Invocation | undefined
        let dispatched = false
        yield* Effect.gen(function* () {
          const lease = yield* Scope.provide(registry.acquire, scope)
          const reserved = yield* restore(
            Session.commit(session, (tx) =>
              Effect.gen(function* () {
                if (closed || !enabled || failed !== undefined || invocations.has(id)) return
                let record = yield* task(tx, id)
                if (
                  record.state.status === 'terminal' ||
                  (record.kind === 'harness.run' && nativeRun === undefined)
                )
                  return
                // An ancestor may have committed a cancellation since the reconciliation
                // pass. Mark it on this same line before admitting any phase.
                const owner = Tree.cancellingOwner(yield* Tree.load(tx), record)
                if (owner !== undefined) {
                  const marked = Tree.mark(
                    record,
                    TaskPolicy.abortIntent(owner)?._tag === 'restart' && !Tree.failedOutcome(owner),
                  )
                  if (marked !== record) {
                    yield* Transaction.putTask(tx, marked)
                    return
                  }
                }
                if (record.abortRequested && (yield* ownedLive(tx, id))) return
                if (record.state.status === 'completing') {
                  if (!(yield* ownedLive(tx, id)))
                    yield* Transaction.putTask(tx, {
                      ...record,
                      state: { status: 'terminal', outcome: record.state.outcome! },
                    })
                  return
                }
                const inspection = yield* inspectRecord(lease, record)
                if (inspection._tag === 'blocked' || inspection._tag === 'inactive') {
                  if (record.abortRequested && TaskPolicy.abortIntent(record)?._tag === 'request')
                    yield* hold(tx, record, {
                      status: 'orphaned',
                      reason: inspection._tag === 'blocked' ? inspection.error.reason : 'inactive',
                    })
                  return
                }
                if (inspection._tag === 'migrationRequired') {
                  const migrated = yield* lease.upgradeTask(tx, record).pipe(
                    Effect.catchCauseIf(
                      (cause) =>
                        !Cause.hasInterruptsOnly(cause) && cause.reasons.some(Cause.isDieReason),
                      (cause) =>
                        Effect.fail(
                          TaskResolution.blocked(
                            record,
                            'migrationFailed',
                            'Task migration defect prevented a compatible record',
                            cause,
                          ),
                        ),
                    ),
                    Effect.result,
                  )
                  if (migrated._tag === 'Failure') {
                    if (migrated.failure._tag === 'TaskResolutionBlocked') {
                      migrations.set(id, {
                        taskId: id,
                        definition: inspection.definition,
                        error: migrated.failure,
                      })
                      if (
                        record.abortRequested &&
                        TaskPolicy.abortIntent(record)?._tag === 'request'
                      )
                        yield* hold(tx, record, {
                          status: 'orphaned',
                          reason: migrated.failure.reason,
                        })
                      return
                    }
                    return yield* migrated.failure
                  }
                  // Migration keeps the persisted status intact. Its commit wake reserves
                  // the upgraded record on the next pass; never combine migration with running.
                  return
                }
                if (record.state.status === 'waiting' && !record.abortRequested) return
                if (options.host?.reconcile !== undefined) {
                  if (
                    !(yield* options.host
                      .reconcile(tx, record)
                      .pipe(
                        Effect.updateContext((context: Context.Context<never>) =>
                          Context.add(
                            hostContext,
                            SessionRuntime.Transactions,
                            Context.get(context, SessionRuntime.Transactions),
                          ),
                        ),
                      ))
                  )
                    return
                  record = yield* task(tx, id)
                  if (
                    record.state.status === 'terminal' ||
                    record.state.status === 'completing' ||
                    (record.state.status === 'waiting' && !record.abortRequested)
                  )
                    return
                  if ((yield* inspectRecord(lease, record))._tag !== 'compatible') return
                }
                migrations.delete(id)
                record = {
                  ...record,
                  state: { status: 'running', checkpoint: record.state.checkpoint! },
                }
                const done = yield* Deferred.make<void>()
                invocation = {
                  taskId: record.id,
                  conversationId: record.conversationId,
                  kind: record.kind,
                  version: record.version,
                  mode: record.abortRequested ? 'abort' : 'run',
                  ended: false,
                  scope,
                  done,
                  transactions: new WeakSet(),
                }
                invocations.set(id, invocation)
                yield* Transaction.putTask(tx, record)
                return { invocation, record }
              }),
            ),
          )
          if (reserved === undefined) return
          reserved.invocation.fiber = yield* run(reserved.invocation, reserved.record, lease).pipe(
            Effect.forkIn(ownerScope),
          )
          dispatched = true
        }).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              if (dispatched) return
              if (invocation !== undefined) {
                invocation.ended = true
                invocations.delete(id)
              }
              yield* Scope.close(scope, Exit.void)
            }),
          ),
        )
      }),
    )

  const reconcile = Effect.gen(function* () {
    yield* notify
    if (!enabled || closed || failed !== undefined) return
    yield* guard
    yield* Session.commit(session, Supervision.reconcile)
    const records = yield* Session.scanTasks(session).pipe(
      Stream.runCollect,
      Effect.onExit(observeProjection('task.scan')),
    )
    for (const record of records) {
      const invocation = invocations.get(record.id)
      if (invocation !== undefined) {
        if (record.abortRequested && invocation.mode === 'run' && invocation.fiber !== undefined)
          yield* Fiber.interrupt(invocation.fiber).pipe(Effect.forkIn(ownerScope))
      } else if (
        (record.kind !== 'harness.run' || nativeRun !== undefined) &&
        record.state.status !== 'terminal'
      )
        yield* reserve(record.id)
    }
  }).pipe(
    Effect.catch((failure) => health(failure)),
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.void
        : health(error('faulted', 'scheduler.reconcile', Cause.pretty(cause), cause)),
    ),
  )
  yield* registry.listen(() => wake)
  yield* Stream.fromQueue(signals).pipe(
    Stream.runForEach(() => reconcile.pipe(Effect.andThen(Effect.yieldNow))),
    Effect.ensuring(
      Effect.suspend(() => {
        const proof = pendingCoordinatorEnd
        pendingCoordinatorEnd = undefined
        reconciliationEnded = true
        if (closed) return Effect.void
        if (
          proof !== undefined &&
          proof.coordinator === reconciliationIdentity &&
          proof.session === state &&
          proof.poison === state.poison &&
          failed !== undefined
        )
          return notify
        return health(error('revoked', 'scheduler.session', 'Session coordinator has ended'))
      }),
    ),
    Effect.forkIn(ownerScope),
  )

  return {
    resume,
    waitForTask,
    outcomes,
    abort,
    abortConversation,
    graph,
    watchGraph,
    wait,
    outcome,
    start: (definition, input, startOptions) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          yield* restore(guard)
          if (!recovered) yield* restore(resume)
          const ref = yield* Effect.scoped(
            Effect.gen(function* () {
              const lease = yield* registry.acquire
              const selected = yield* lease.resolveTask(definition.kind)
              const contract = selected.declaration
              if (
                contract.version !== definition.version ||
                contract.input !== definition.input ||
                contract.checkpoint !== definition.checkpoint ||
                contract.success !== definition.success ||
                contract.failure !== definition.failure
              )
                return yield* error(
                  'invalidDefinition',
                  'task.start',
                  'Start requires the installed task contract',
                )
              const wire = yield* restore(lease.prepareTask(definition.kind, input))
              const created = yield* Session.commit(session, (tx) =>
                check.pipe(
                  Effect.andThen(
                    Transaction.createTask(tx, {
                      conversationId: startOptions.conversationId,
                      kind: contract.kind,
                      version: contract.version,
                      input: wire.input,
                      background: startOptions.background ?? false,
                      abortRequested: false,
                      restartPolicy: startOptions.restartPolicy ?? 'resume',
                      schedulerFormat: { version: 1, minimumReader: 1 },
                      state: { status: 'pending', checkpoint: wire.checkpoint },
                    }),
                  ),
                ),
              )
              return { id: created.id, definition }
            }),
          )
          enabled = true
          yield* wake
          return ref
        }),
      ),
    inspect: (id) =>
      check.pipe(
        Effect.andThen(
          Effect.scoped(
            Effect.gen(function* () {
              const lease = yield* registry.acquire
              const { record, tree } = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  return { record: yield* task(tx, id), tree: yield* Tree.load(tx) }
                }),
              )
              const resolution = yield* inspectRecord(lease, record)
              const invoking = invocations.has(id)
              const waitingOn = record.abortRequested
                ? Tree.liveOwned(tree, id).map((child) => child.id)
                : (record.state.on ?? []).filter(
                    (member) => tree.tasks.get(member)?.state.status !== 'terminal',
                  )
              let scheduling: Scheduler.Inspection['scheduling'] = 'ready'
              if (record.state.status === 'terminal') scheduling = 'terminal'
              else if (record.state.status === 'completing') scheduling = 'completing'
              else if (invoking) scheduling = 'running'
              else if (resolution._tag === 'blocked') scheduling = 'blocked'
              else if (waitingOn.length > 0) scheduling = 'waiting'
              return { record, resolution, invoking, scheduling, waitingOn }
            }),
          ),
        ),
      ),
    failure: Effect.sync(() => Option.fromUndefinedOr(failed)),
    changes: Stream.fromPubSub(publications),
  }
})
