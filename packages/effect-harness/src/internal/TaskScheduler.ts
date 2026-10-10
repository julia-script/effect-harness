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
import { RegistryError } from '../RegistryError.js'
import { StorageFormatError } from '../StorageFormat.js'
import { Registry } from '../Registry.js'
import type * as RegistryScope from '../RegistryScope.js'
import * as Session from '../Session.js'
import { TaskError } from '../TaskError.js'
import * as Task from '../Task.js'
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
const invocationFailure = (failure: Scheduler.Failure): import('../TaskError.js').Failure =>
  Schema.is(RegistryError)(failure) || Schema.is(StorageFormatError)(failure)
    ? error('faulted', 'task.hostBoundary', 'Task scheduler boundary failed', failure)
    : failure
const sameJson = Schema.toEquivalence(Schema.Json)

/** Hold receipts until ordinary direct tasks and work in owned conversations have drained. */
const ownedLive = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  id: Record.TaskId,
  includeBackground = false,
) {
  const conversations = yield* Transaction.scanConversations(tx, { ownerTaskId: id }).pipe(
    Stream.runCollect,
  )
  const ownedConversations = new Set(conversations.map((conversation) => conversation.id))
  const tasks = yield* Transaction.scanTasks(tx).pipe(Stream.runCollect)
  return tasks.some(
    (candidate) =>
      candidate.state.status !== 'terminal' &&
      (includeBackground || !candidate.background) &&
      (candidate.owner === id || ownedConversations.has(candidate.conversationId)),
  )
})
const hold = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  current: Record.Task,
  outcome: Record.TaskOutcome,
) {
  const state: Record.TaskState = {
    status: (yield* ownedLive(tx, current.id)) ? 'completing' : 'terminal',
    outcome,
  }
  yield* Transaction.putTask(tx, TaskPolicy.withState(current, state))
})

export const make = Effect.fnUntraced(function* <R = never>(
  session: Session.Session,
  options: { readonly host?: Scheduler.Host<R> } = {},
): Effect.fn.Return<Scheduler.Service, Scheduler.Failure, Registry | Scope.Scope | R> {
  const registry = yield* Registry
  // One parent finalizer seals writes before closing any scheduler-owned fiber.
  const ownerScope = yield* Scope.make()
  const hostContext = yield* Effect.context<R>()
  const state = yield* SessionRuntime.get(session)
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
  const health = (failure: Scheduler.Failure) =>
    Effect.sync(() => {
      failed = failure
    }).pipe(Effect.andThen(notify))

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      closed = true
      enabled = false
      // Sealing is synchronous before waiting for any invocation cleanup.
      for (const invocation of invocations.values()) invocation.ended = true
      yield* Scope.close(ownerScope, Exit.void)
      if (state.scheduler === identity) state.scheduler = undefined
      yield* PubSub.shutdown(publications)
    }),
  )

  const read = (id: Record.TaskId) =>
    Session.task(session, id).pipe(
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
  const resume = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      yield* restore(guard)
      yield* restore(
        Session.commit(session, (tx) =>
          Effect.gen(function* () {
            const records = yield* Transaction.scanTasks(tx).pipe(Stream.runCollect)
            for (const record of records) {
              if (
                record.kind === 'harness.run' ||
                record.state.status === 'terminal' ||
                invocations.has(record.id)
              )
                continue
              // Restart abandonment needs the later host's ownership cascade; do not silently run it.
              if (TaskPolicy.restartPolicy(record) === 'abandon' && !record.abortRequested) continue
              const recovered = TaskPolicy.recover(record)
              yield* Transaction.putTask(tx, recovered)
            }
          }),
        ),
      )
      failed = undefined
      enabled = true
      yield* wake
    }),
  )
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

  const current = (tx: Transaction.Transaction, invocation: Invocation) =>
    Effect.gen(function* () {
      if (closed || invocations.get(invocation.taskId) !== invocation)
        return yield* error('revoked', 'task.invocation', 'Task reservation is no longer active')
      const record = yield* task(tx, invocation.taskId)
      yield* Transitions.gate(record, invocation)
      return record
    })
  const access = (
    invocation: Invocation,
    lease: RegistryScope.Lease,
    host: Scheduler.HostAccess,
  ): Access => {
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
    const declaration = lease.snapshot.tasks.find(
      (item) => item.declaration.kind === invocation.kind,
    )!.declaration
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
    return {
      taskId: invocation.taskId,
      conversationId: invocation.conversationId,
      registry: {
        tasks: lease.snapshot.tasks.map((item) => item.declaration),
        extensions: lease.snapshot.extensions.map((item) => item.declaration),
        tools: lease.snapshot.tools.map((item) => item.declaration),
        hooks: lease.snapshot.hooks.map((item) => item.declaration),
      },
      agent: withRead(host.agent ?? Effect.fail(unavailable('agent'))),
      settings: withRead(host.settings ?? Effect.fail(unavailable('settings'))),
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
            ),
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
              if (host.validateWait === undefined) return yield* unavailable('validateWait')
              yield* host.validateWait(tx, record, next)
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
          Effect.andThen(
            host.createChild === undefined
              ? Effect.fail(unavailable('createChild'))
              : host.createChild(tx, definition, input, options),
          ),
        ),
      createConversation: (tx) =>
        transaction(tx).pipe(
          Effect.andThen(
            host.createConversation === undefined
              ? Effect.fail(unavailable('createConversation'))
              : host.createConversation(tx),
          ),
        ),
      outcome: (ref) => withRead(outcome(ref)).pipe(Effect.mapError(invocationFailure)),
      wait: (ref) =>
        Effect.raceFirst(
          withRead(wait(ref)).pipe(Effect.mapError(invocationFailure)),
          Deferred.await(invocation.done).pipe(Effect.andThen(Effect.interrupt)),
        ),
      abort: (id, options) =>
        withRead(
          host.abort === undefined ? Effect.fail(unavailable('abort')) : host.abort(id, options),
        ),
      abortOwned: (options) =>
        withRead(
          host.abortOwned === undefined
            ? Effect.fail(unavailable('abortOwned'))
            : host.abortOwned(options),
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
      context: withRead(host.context ?? Effect.fail(unavailable('context'))),
      snapshot: (document, target) => withRead(Session.snapshot(session, document, target)),
      snapshotAsOf: (document, target, at) =>
        withRead(Session.snapshotAsOf(session, document, target, at)),
      watch: (document, target) => scopedStream(Session.watch(session, document, target)),
      dispatchHook: (event) =>
        withRead(
          host.dispatchHook === undefined
            ? Effect.fail(unavailable('dispatchHook'))
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

  const run = (invocation: Invocation, record: Record.Task, lease: RegistryScope.Lease) => {
    let cleanupExit: Exit.Exit<unknown, unknown> = Exit.void
    return Effect.uninterruptibleMask((restore) =>
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
        const exit = yield* restore(
          lease
            .invokeTask(record, invocation.mode)
            .pipe(
              Effect.provideService(TaskExecution, access(invocation, lease, host)),
              Scope.provide(invocation.scope),
            ),
        ).pipe(Effect.exit)
        if (
          Exit.isFailure(exit) &&
          exit.cause.reasons.length === 1 &&
          Cause.isFailReason(exit.cause.reasons[0]!)
        ) {
          yield* health(exit.cause.reasons[0].error)
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
        yield* finish(invocation, record, result)
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
      Effect.catch((failure) => health(failure)),
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : health(error('faulted', 'scheduler.invoke', Cause.pretty(cause), cause)),
      ),
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
  }

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
                if (record.state.status === 'terminal' || record.kind === 'harness.run') return
                if (record.state.status === 'completing') {
                  if (!(yield* ownedLive(tx, id)))
                    yield* Transaction.putTask(tx, {
                      ...record,
                      state: { status: 'terminal', outcome: record.state.outcome! },
                    })
                  return
                }
                const inspection = yield* lease.inspectTask(record, migrations.get(id))
                if (inspection._tag === 'blocked' || inspection._tag === 'inactive') return
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
                      return
                    }
                    return yield* migrated.failure
                  }
                  // Migration keeps the persisted status intact. Its commit wake reserves
                  // the upgraded record on the next pass; never combine migration with running.
                  return
                }
                // Ownership-dependent admission must be supplied by an explicit supervisor.
                const conversation = yield* Transaction.conversation(tx, record.conversationId)
                const requiresHost =
                  (Option.isSome(conversation) && conversation.value.owner !== undefined) ||
                  record.owner !== undefined ||
                  record.state.status === 'waiting' ||
                  (TaskPolicy.restartPolicy(record) === 'abandon' && !record.abortRequested) ||
                  (record.abortRequested && (yield* ownedLive(tx, id, true)))
                if (requiresHost) {
                  if (options.host?.reconcile === undefined) return
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
                    record.state.status === 'waiting' ||
                    record.state.status === 'terminal' ||
                    record.state.status === 'completing'
                  )
                    return
                  // A supplied adapter may change version, identity or abort intent.
                  if ((yield* lease.inspectTask(record, migrations.get(id)))._tag !== 'compatible')
                    return
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
    const records = yield* Session.scanTasks(session).pipe(Stream.runCollect)
    for (const record of records) {
      const invocation = invocations.get(record.id)
      if (invocation !== undefined) {
        if (record.abortRequested && invocation.mode === 'run' && invocation.fiber !== undefined)
          yield* Fiber.interrupt(invocation.fiber).pipe(Effect.forkIn(ownerScope))
      } else if (record.kind !== 'harness.run' && record.state.status !== 'terminal')
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
      Effect.suspend(() =>
        closed
          ? Effect.void
          : health(error('revoked', 'scheduler.session', 'Session coordinator has ended')),
      ),
    ),
    Effect.forkIn(ownerScope),
  )

  return {
    resume,
    wait,
    outcome,
    start: (definition, input, startOptions) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          yield* restore(guard)
          if (startOptions.restartPolicy === 'abandon' && options.host?.reconcile === undefined)
            return yield* unavailable('restartAbandonment')
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
              const conversation = yield* Session.conversation(session, startOptions.conversationId)
              if (
                options.host?.reconcile === undefined &&
                Option.isSome(conversation) &&
                conversation.value.owner !== undefined
              )
                return yield* unavailable('ownedConversationAdmission')
              const wire = yield* restore(lease.prepareTask(definition.kind, input))
              const created = yield* Session.commit(session, (tx) =>
                check.pipe(
                  Effect.andThen(
                    Transaction.createTask(tx, {
                      conversationId: startOptions.conversationId,
                      kind: contract.kind,
                      version: contract.version,
                      input: wire.input,
                      background: false,
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
              const record = yield* read(id)
              let resolution = yield* lease.inspectTask(record, migrations.get(id))
              if (
                options.host?.reconcile === undefined &&
                (resolution._tag === 'compatible' || resolution._tag === 'migrationRequired')
              ) {
                const conversation = yield* Session.conversation(session, record.conversationId)
                let requiresHost =
                  (Option.isSome(conversation) && conversation.value.owner !== undefined) ||
                  record.owner !== undefined ||
                  record.state.status === 'waiting' ||
                  TaskPolicy.restartPolicy(record) === 'abandon'
                if (!requiresHost && record.abortRequested) {
                  const conversations = yield* Session.scanConversations(session, {
                    ownerTaskId: id,
                  }).pipe(Stream.runCollect)
                  const owned = new Set(conversations.map((conversation) => conversation.id))
                  const children = yield* Session.scanTasks(session).pipe(Stream.runCollect)
                  requiresHost = children.some(
                    (child) =>
                      child.state.status !== 'terminal' &&
                      (child.owner === id || owned.has(child.conversationId)),
                  )
                }
                if (requiresHost)
                  resolution = {
                    _tag: 'blocked',
                    error: TaskResolution.blocked(
                      record,
                      'incompatible',
                      'Task requires the supplied ownership supervisor for children, joins, abort cascades or restart abandonment',
                    ),
                  }
              }
              return { record, resolution, invoking: invocations.has(id) }
            }),
          ),
        ),
      ),
    failure: Effect.sync(() => Option.fromUndefinedOr(failed)),
    changes: Stream.fromPubSub(publications),
  }
})
