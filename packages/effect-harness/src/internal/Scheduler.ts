/** Embedded scheduler: persisted task records are authoritative; fibers are disposable. */
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Record from '../Record.ts'
import { StorageError } from '../StorageError.ts'
import {
  ClosedError,
  ConflictError,
  CorruptError,
  NotFoundError,
  RevokedError,
  rejected,
} from '../StorageError.ts'
import * as Task from '../Task.ts'
import { TaskRuntime } from '../TaskRuntime.ts'
import type * as Session from './Session.ts'
import * as Ownership from './Ownership.ts'

interface Invocation {
  readonly taskId: Record.TaskId
  readonly abort: boolean
  ended: boolean
  fiber?: Fiber.Fiber<void, StorageError>
}

export interface Scheduler {
  readonly create: (
    definition: Task.BoundDefinition,
    input: unknown,
    options: {
      readonly conversationId: Record.ConversationId
      readonly owner?: Record.TaskId
      readonly background?: boolean
    },
  ) => Effect.Effect<Record.TaskId, StorageError | Schema.SchemaError>
  /** Opening reconciles recovery state; only resume starts previously saved tasks. */
  readonly resume: Effect.Effect<void, StorageError>
  readonly wake: Effect.Effect<void>
  readonly admit: (id: Record.TaskId) => Effect.Effect<void>
  readonly install: (definitions: ReadonlyArray<Task.BoundDefinition>) => Effect.Effect<void>
  readonly abort: (id: Record.TaskId) => Effect.Effect<void, StorageError>
  readonly abortConversation: (
    id: Record.ConversationId,
    background?: boolean,
  ) => Effect.Effect<void, StorageError>
  readonly await: (id: Record.TaskId) => Effect.Effect<Record.Task, StorageError>
  readonly idle: (
    id: Record.ConversationId,
    background?: boolean,
  ) => Effect.Effect<void, StorageError>
  readonly close: Effect.Effect<void>
}

const jsonEqual = Schema.toEquivalence(Schema.Json)
const taskEqual = Schema.toEquivalence(Record.Task)
const outcomeJson = (outcome: Task.Outcome<Schema.Json>): Schema.Json => outcome
const terminalState = (outcome: Schema.Json): Record.Task['state'] => ({
  status: 'terminal',
  outcome,
})
const completingState = (outcome: Schema.Json): Record.Task['state'] => ({
  status: 'completing',
  outcome,
})
const withoutMemos = (record: Record.Task, state: Record.Task['state']): Record.Task => {
  const { memos: _memos, ...rest } = record
  return { ...rest, state }
}

/** Acquire a scheduler in the harness Scope; closing pauses rather than cancels tasks. */
export const make = Effect.fn('Scheduler.make')(function* (
  session: Session.Session.Service,
  definitions: ReadonlyArray<Task.BoundDefinition>,
) {
  // Own a child Scope so the harness finalizer seals commits before interrupting jobs.
  const scope = yield* Scope.make()
  const wakeups = yield* Queue.make<void>({ capacity: 1, strategy: 'dropping' })
  const registry = new Map(definitions.map((definition) => [definition.name, definition]))
  const active = new Map<Record.TaskId, Invocation>()
  const waiters = new Map<Record.TaskId, Set<Deferred.Deferred<Record.Task, StorageError>>>()
  let failure: StorageError | undefined
  const admitted = new Set<Record.TaskId>()
  const idleWaiters = new Set<{
    readonly conversationId: Record.ConversationId
    readonly background: boolean
    readonly deferred: Deferred.Deferred<void, StorageError>
  }>()
  let enabled = false
  let closing = false
  let closed = false
  const wake = Queue.offer(wakeups, undefined).pipe(Effect.asVoid)
  const checkOpen = Effect.suspend(() => {
    if (failure !== undefined) return Effect.fail(failure)
    if (closing) return Effect.fail(rejected('Task scheduler is closed', ClosedError))
    return Effect.void
  })
  const tasks = (tx: Session.Transaction) =>
    Effect.forEach(['pending', 'running', 'waiting', 'completing'] as const, (status) =>
      Stream.runCollect(tx.scanTasks({ status })),
    ).pipe(Effect.map((groups) => groups.flat()))
  const graph = Effect.fn('Scheduler.graph')(function* (tx: Session.Transaction) {
    const records = yield* tasks(tx)
    const taskMap = new Map(records.map((record) => [record.id, record]))
    const conversations = new Map<Record.ConversationId, Record.Conversation>()
    const visited = new Set<Record.TaskId>()
    const pending = [...records]
    for (let index = 0; index < pending.length; index++) {
      const record = pending[index]
      if (record === undefined || visited.has(record.id)) continue
      visited.add(record.id)
      if (!conversations.has(record.conversationId)) {
        const conversation = yield* tx.conversation(record.conversationId)
        if (Option.isSome(conversation))
          conversations.set(record.conversationId, conversation.value)
      }
      const owner = record.owner ?? conversations.get(record.conversationId)?.owner?.taskId
      const needed = owner === undefined ? [] : [owner]
      if (record.state.status === 'waiting') needed.push(...(record.state.on ?? []))
      for (const id of needed) {
        let saved = taskMap.get(id)
        if (saved === undefined) {
          const found = yield* tx.task(id)
          if (Option.isSome(found)) {
            saved = found.value
            taskMap.set(id, saved)
          }
        }
        if (saved !== undefined) pending.push(saved)
      }
    }
    return { tasks: taskMap, conversations }
  })
  const waitTask = Effect.fn('Scheduler.await')(function* (id: Record.TaskId) {
    const waiter = yield* Deferred.make<Record.Task, StorageError>()
    const found = yield* session.transaction(
      Effect.fn('Scheduler.registerWaiter')(function* (tx) {
        yield* checkOpen
        const value = yield* tx.task(id)
        if (Option.isNone(value)) return yield* rejected(`Task ${id} does not exist`, NotFoundError)
        if (value.value.state.status === 'terminal') return value.value
        const existing = waiters.get(id) ?? new Set<Deferred.Deferred<Record.Task, StorageError>>()
        existing.add(waiter)
        waiters.set(id, existing)
        return undefined
      }),
    )
    if (found !== undefined) return found
    yield* wake
    return yield* Deferred.await(waiter).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          const existing = waiters.get(id)
          existing?.delete(waiter)
          if (existing?.size === 0) waiters.delete(id)
        }),
      ),
    )
  })
  const checkInvocation = Effect.fn('Scheduler.checkInvocation')(function* (
    invocation: Invocation,
    tx: Session.Transaction,
  ) {
    yield* checkOpen
    if (invocation.ended || active.get(invocation.taskId) !== invocation)
      return yield* rejected('Task invocation has ended', RevokedError)
    const value = yield* tx.task(invocation.taskId)
    if (Option.isNone(value)) return yield* rejected('Invocation task is missing', NotFoundError)
    const record = value.value
    if (record.state.status !== 'running' || (!invocation.abort && record.abortRequested))
      return yield* rejected('Task invocation cannot commit in its current state', RevokedError)
    return record
  })
  const cleanup = Effect.fn('Scheduler.cleanup')(function* (
    tx: Session.Transaction,
    record: Record.Task,
  ) {
    if (record.state.outcome !== undefined) {
      const outcome = yield* Schema.decodeUnknownEffect(Task.Outcome)(record.state.outcome).pipe(
        Effect.mapError((cause) => rejected('Task outcome is malformed', CorruptError, cause)),
      )
      const definition = registry.get(record.kind)
      if (definition?.onTerminal !== undefined) yield* definition.onTerminal(tx, record, outcome)
    }
    const documents = yield* Stream.runCollect(
      tx.scanDocuments({ scope: { _tag: 'task', taskId: record.id } }),
    )
    for (const document of documents) yield* tx.write({ _tag: 'document.retire', id: document.id })
  })
  const stageTransition = Effect.fn('Scheduler.stageTransition')(function* (
    tx: Session.Transaction,
    record: Record.Task,
    transition: Task.Transition<Schema.Json, Schema.Json>,
  ) {
    if (transition.status === 'terminal') {
      const ownership = yield* graph(tx)
      const owns = (Ownership.ownedLive(ownership).get(record.id)?.length ?? 0) > 0
      const outcome = outcomeJson(transition.outcome)
      if (!owns) yield* cleanup(tx, withoutMemos(record, terminalState(outcome)))
      yield* tx.write({
        _tag: 'task',
        value: withoutMemos(record, owns ? completingState(outcome) : terminalState(outcome)),
      })
      return
    }
    if (transition.status === 'waiting') {
      const ownership = yield* graph(tx)
      const ancestors = new Set(
        Array.from(Ownership.ancestors(ownership, record), (owner) => owner.id),
      )
      for (const id of transition.on) {
        if (id === record.id || ancestors.has(id) || !ownership.tasks.has(id))
          return yield* rejected('A task cannot wait on itself, an owner, or a missing task')
      }
    }
    yield* tx.write({ _tag: 'task', value: { ...record, state: transition } })
  })
  const runtime = (invocation: Invocation, record: Record.Task): TaskRuntime['Service'] => {
    const transaction = <A, E, R>(
      change: (tx: Session.Transaction, current: Record.Task) => Effect.Effect<A, E, R>,
    ) =>
      Effect.suspend(() => {
        const created: Array<Record.TaskId> = []
        return session
          .transaction(
            Effect.fn('TaskRuntime.transaction')(function* (tx) {
              const current = yield* checkInvocation(invocation, tx)
              const scoped: Session.Transaction = {
                ...tx,
                createTask: (input) =>
                  tx.createTask(input).pipe(
                    Effect.tap((id) =>
                      Effect.sync(() => {
                        created.push(id)
                      }),
                    ),
                  ),
              }
              return yield* change(scoped, current)
            }),
          )
          .pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                for (const id of created) admitted.add(id)
              }),
            ),
            Effect.tap(() => (created.length === 0 ? Effect.void : wake)),
          )
      })
    const commit = <E, R>(
      change: (
        tx: Session.Transaction,
        current: Record.Task,
      ) => Effect.Effect<Task.Transition<Schema.Json, Schema.Json> | void, E, R>,
    ) =>
      transaction(
        Effect.fn('TaskRuntime.commit')(function* (tx, current) {
          const transition = yield* change(tx, current)
          if (transition !== undefined) yield* stageTransition(tx, current, transition)
        }),
      )
    const check = transaction(() => Effect.void)
    return {
      taskId: record.id,
      conversationId: record.conversationId,
      transaction,
      commit,
      checkpoint: (checkpoint) => commit(() => Effect.succeed(Task.continueWith(checkpoint))),
      memo: (name, candidate) =>
        transaction(
          Effect.fn('TaskRuntime.memo')(function* (tx, current) {
            const saved =
              current.memos !== undefined && Object.hasOwn(current.memos, name)
                ? current.memos[name]
                : undefined
            if (saved !== undefined || candidate === undefined) return saved
            yield* tx.write({
              _tag: 'task',
              value: { ...current, memos: { ...current.memos, [name]: candidate } },
            })
            return candidate
          }),
        ),
      task: (id) => check.pipe(Effect.andThen(session.task(id))),
      outcomes: (ids) =>
        check.pipe(
          Effect.andThen(
            Effect.forEach(
              ids,
              Effect.fn('TaskRuntime.outcome')(function* (id) {
                const value = yield* session.task(id)
                if (
                  Option.isNone(value) ||
                  value.value.state.status !== 'terminal' ||
                  value.value.state.outcome === undefined
                )
                  return yield* rejected(`Task ${id} is not terminal`, ConflictError)
                return value.value.state.outcome
              }),
            ),
          ),
        ),
      now: check.pipe(Effect.andThen(Clock.currentTimeMillis)),
      sleepUntil: Effect.fn('TaskRuntime.sleepUntil')(function* (until) {
        yield* check
        const now = yield* Clock.currentTimeMillis
        if (until > now) yield* Effect.sleep(until - now)
        yield* check
      }),
    }
  }
  const execute = Effect.fn('Scheduler.execute')(function* (invocation: Invocation) {
    for (;;) {
      const current = yield* session.transaction((tx) => checkInvocation(invocation, tx))
      const definition = registry.get(current.kind)
      if (definition === undefined || definition.version !== current.version) {
        yield* session.transaction(
          Effect.fn('Scheduler.block')(function* (tx) {
            const record = yield* checkInvocation(invocation, tx)
            if (invocation.abort)
              yield* stageTransition(tx, record, {
                status: 'terminal',
                outcome: { status: 'orphaned', reason: 'missing_task' },
              })
            else
              yield* tx.write({
                _tag: 'task',
                value: {
                  ...record,
                  state: {
                    status: 'pending',
                    ...(record.state.checkpoint === undefined
                      ? {}
                      : { checkpoint: record.state.checkpoint }),
                  },
                },
              })
          }),
        )
        return
      }
      const before = current.state.checkpoint
      const exit = yield* Effect.exit(
        Effect.scoped(
          definition
            .run(current, invocation.abort)
            .pipe(Effect.provideService(TaskRuntime, runtime(invocation, current))),
        ),
      )
      if (Exit.isFailure(exit)) {
        if (Cause.hasInterrupts(exit.cause)) return
        const error = Cause.findErrorOption(exit.cause)
        if (
          Option.isSome(error) &&
          error.value instanceof StorageError &&
          (error.value.certainty === 'uncertain' || error.value.reason._tag === 'PoisonedError')
        )
          return yield* error.value
        yield* session.transaction(
          Effect.fn('Scheduler.fault')(function* (tx) {
            const record = yield* checkInvocation(invocation, tx)
            yield* stageTransition(tx, record, {
              status: 'terminal',
              outcome: { status: 'faulted', error: { message: Cause.pretty(exit.cause) } },
            })
          }),
        )
        return
      }
      const stopped = yield* session.transaction(
        Effect.fn('Scheduler.advance')(function* (tx) {
          yield* checkOpen
          const saved = yield* tx.task(invocation.taskId)
          if (Option.isNone(saved) || saved.value.state.status !== 'running') return true
          const record = yield* checkInvocation(invocation, tx)
          if (exit.value !== undefined) yield* stageTransition(tx, record, exit.value)
          return exit.value !== undefined && exit.value.status !== 'running'
        }),
      )
      if (stopped) return
      const after = yield* session.task(invocation.taskId)
      if (
        Option.isSome(after) &&
        after.value.state.checkpoint !== undefined &&
        before !== undefined &&
        jsonEqual(after.value.state.checkpoint, before)
      ) {
        yield* session.transaction(
          Effect.fn('Scheduler.noProgress')(function* (tx) {
            const record = yield* checkInvocation(invocation, tx)
            yield* stageTransition(tx, record, {
              status: 'terminal',
              outcome: {
                status: 'faulted',
                error: { message: `Task ${record.kind} returned without durable progress` },
              },
            })
          }),
        )
        return
      }
    }
  })
  const reconcile = Effect.fn('Scheduler.reconcile')(function* (tx: Session.Transaction) {
    const ownership = yield* graph(tx)
    for (const record of ownership.tasks.values()) {
      if (record.state.status === 'terminal') continue
      if (!record.abortRequested && Ownership.belowCancelled(ownership, record))
        ownership.tasks.set(record.id, { ...record, abortRequested: true })
      if (
        record.state.status === 'waiting' &&
        record.state.policy === 'failFast' &&
        record.state.on?.some((id) => {
          const child = ownership.tasks.get(id)
          return child !== undefined && Ownership.failed(child)
        })
      ) {
        for (const id of record.state.on) {
          const child = ownership.tasks.get(id)
          if (child !== undefined && child.state.status !== 'terminal' && !Ownership.failed(child))
            ownership.tasks.set(id, { ...child, abortRequested: true })
        }
      }
    }
    let changed = true
    while (changed) {
      changed = false
      const owned = Ownership.ownedLive(ownership)
      for (const record of ownership.tasks.values()) {
        if (record.state.status === 'completing' && (owned.get(record.id)?.length ?? 0) === 0) {
          yield* cleanup(tx, withoutMemos(record, terminalState(record.state.outcome ?? null)))
          ownership.tasks.set(
            record.id,
            withoutMemos(record, terminalState(record.state.outcome ?? null)),
          )
          changed = true
        }
      }
    }
    const committed = yield* tasks(tx)
    for (const record of committed) {
      const next = ownership.tasks.get(record.id)
      if (next !== undefined && next !== record && !taskEqual(next, record))
        yield* tx.write({ _tag: 'task', value: next })
    }
    return ownership
  })
  const pump = Effect.fn('Scheduler.pump')(function* () {
    if (failure !== undefined) return yield* failure
    if (closing) return
    const reservations: Array<{ readonly invocation: Invocation; readonly record: Record.Task }> =
      []
    const exit = yield* Effect.exit(
      session.transaction(
        Effect.fn('Scheduler.reserve')(function* (tx) {
          const ownership = yield* reconcile(tx)
          const owned = Ownership.ownedLive(ownership)
          for (const record of ownership.tasks.values()) {
            if (
              record.state.status === 'terminal' ||
              record.state.status === 'completing' ||
              active.has(record.id) ||
              (!enabled &&
                !admitted.has(record.id) &&
                !Array.from(Ownership.ancestors(ownership, record)).some((owner) =>
                  admitted.has(owner.id),
                ))
            )
              continue
            if (record.abortRequested && (owned.get(record.id)?.length ?? 0) > 0) continue
            if (
              !record.abortRequested &&
              record.state.status === 'waiting' &&
              record.state.on?.some((id) => ownership.tasks.get(id)?.state.status !== 'terminal')
            )
              continue
            const definition = registry.get(record.kind)
            if (
              !record.abortRequested &&
              (definition === undefined || definition.version !== record.version)
            )
              continue
            const invocation: Invocation = {
              taskId: record.id,
              abort: record.abortRequested,
              ended: false,
            }
            const reserved = {
              ...record,
              state: {
                status: 'running' as const,
                ...(record.state.checkpoint === undefined
                  ? {}
                  : { checkpoint: record.state.checkpoint }),
              },
            }
            yield* tx.write({ _tag: 'task', value: reserved })
            active.set(record.id, invocation)
            reservations.push({ invocation, record: reserved })
          }
        }),
      ),
    )
    if (Exit.isFailure(exit)) {
      for (const { invocation } of reservations) {
        invocation.ended = true
        active.delete(invocation.taskId)
      }
      return yield* Effect.failCause(exit.cause)
    }
    // Abort marks are persisted before run fibers receive interruption.
    for (const invocation of active.values()) {
      if (invocation.abort || invocation.fiber === undefined) continue
      const record = yield* session.task(invocation.taskId)
      if (Option.isSome(record) && record.value.abortRequested)
        yield* Fiber.interrupt(invocation.fiber)
    }
    if (idleWaiters.size > 0) {
      // Pair each waiter with the graph on the mutation line; a newly registered waiter
      // must not be completed against a snapshot taken before its conversation work existed.
      const ready = yield* session.transaction(
        Effect.fn('Scheduler.readyIdle')(function* (tx) {
          const ownership = yield* graph(tx)
          return Array.from(idleWaiters).filter(
            (waiter) =>
              !Array.from(ownership.tasks.values()).some(
                (record) =>
                  record.state.status !== 'terminal' &&
                  Ownership.inConversation(
                    ownership,
                    record,
                    waiter.conversationId,
                    waiter.background,
                  ),
              ),
          )
        }),
      )
      for (const waiter of ready) {
        yield* Deferred.succeed(waiter.deferred, undefined)
        idleWaiters.delete(waiter)
      }
    }
    for (const [id, pending] of waiters) {
      const record = yield* session.task(id)
      if (Option.isSome(record) && record.value.state.status === 'terminal') {
        for (const waiter of pending) yield* Deferred.succeed(waiter, record.value)
        waiters.delete(id)
      }
    }
    for (const { invocation } of reservations) {
      invocation.fiber = yield* execute(invocation).pipe(
        Effect.catch(
          Effect.fn('Scheduler.invocationFailure')(function* (error) {
            if (error.reason._tag === 'RevokedError' || error.reason._tag === 'ClosedError') return
            failure = error
            yield* wake
          }),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            invocation.ended = true
            if (active.get(invocation.taskId) === invocation) active.delete(invocation.taskId)
          }).pipe(Effect.andThen(wake)),
        ),
        Effect.forkIn(scope),
      )
    }
  })
  // Recovery never runs task code until resume; interrupted records retain their checkpoint.
  yield* session.transaction(
    Effect.fn('Scheduler.recover')(function* (tx) {
      const records = yield* tasks(tx)
      for (const record of records) {
        if (record.state.status === 'running')
          yield* tx.write({
            _tag: 'task',
            value: {
              ...record,
              state: {
                status: 'pending',
                ...(record.state.checkpoint === undefined
                  ? {}
                  : { checkpoint: record.state.checkpoint }),
              },
            },
          })
      }
      yield* reconcile(tx)
    }),
  )
  let pumpFiber: Fiber.Fiber<void, StorageError> | undefined
  const close = Effect.gen(function* () {
    if (closed) return
    closed = true
    closing = true
    const error = failure ?? rejected('Task scheduler is closed', ClosedError)
    for (const pending of waiters.values())
      for (const waiter of pending) yield* Deferred.fail(waiter, error)
    waiters.clear()
    for (const waiter of idleWaiters) yield* Deferred.fail(waiter.deferred, error)
    idleWaiters.clear()
    if (pumpFiber !== undefined) yield* Fiber.interrupt(pumpFiber)
    yield* Fiber.interruptAll(
      Array.from(active.values()).flatMap((invocation) =>
        invocation.fiber === undefined ? [] : [invocation.fiber],
      ),
    )
    yield* Queue.shutdown(wakeups)
    yield* Scope.close(scope, Exit.void)
  })
  yield* Effect.addFinalizer(() => close)
  pumpFiber = yield* Effect.forever(Queue.take(wakeups).pipe(Effect.andThen(pump))).pipe(
    Effect.catch(
      Effect.fn('Scheduler.stopOnFailure')(function* (error) {
        failure = error
        closing = true
        for (const pending of waiters.values())
          for (const waiter of pending) yield* Deferred.fail(waiter, error)
        waiters.clear()
        for (const waiter of idleWaiters) yield* Deferred.fail(waiter.deferred, error)
        idleWaiters.clear()
        yield* Fiber.interruptAll(
          Array.from(active.values()).flatMap((invocation) =>
            invocation.fiber === undefined ? [] : [invocation.fiber],
          ),
        )
      }),
    ),
    Effect.forkIn(scope),
  )
  yield* Stream.runForEach(session.commits, () => wake).pipe(Effect.forkIn(scope))
  const scheduler: Scheduler = {
    wake,
    admit: (id) =>
      Effect.sync(() => {
        admitted.add(id)
      }).pipe(Effect.andThen(wake)),
    close,
    resume: checkOpen.pipe(
      Effect.andThen(
        Effect.sync(() => {
          enabled = true
        }),
      ),
      Effect.andThen(wake),
    ),
    install: (next) =>
      Effect.sync(() => {
        registry.clear()
        for (const definition of next) registry.set(definition.name, definition)
      }).pipe(Effect.andThen(wake)),
    create: Effect.fn('Scheduler.create')(function* (definition, input, options) {
      yield* checkOpen
      const prepared = yield* definition.prepare(input)
      const id = yield* session.transaction((tx) =>
        tx.createTask({
          ...options,
          background: options.background ?? false,
          kind: definition.name,
          version: definition.version,
          input: prepared.input,
          abortRequested: false,
          state: { status: 'pending', checkpoint: prepared.checkpoint },
        }),
      )
      admitted.add(id)
      registry.set(definition.name, definition)
      yield* wake
      return id
    }),
    await: waitTask,
    abort: Effect.fn('Scheduler.abort')(function* (id) {
      yield* checkOpen
      yield* session.transaction(
        Effect.fn('Scheduler.markAbort')(function* (tx) {
          const value = yield* tx.task(id)
          if (Option.isNone(value))
            return yield* rejected(`Task ${id} does not exist`, NotFoundError)
          if (value.value.state.status !== 'terminal')
            yield* tx.write({ _tag: 'task', value: { ...value.value, abortRequested: true } })
        }),
      )
      admitted.add(id)
      yield* wake
      yield* waitTask(id)
    }),
    abortConversation: Effect.fn('Scheduler.abortConversation')(function* (id, background = false) {
      yield* checkOpen
      yield* session.transaction(
        Effect.fn('Scheduler.markConversationAbort')(function* (tx) {
          const ownership = yield* graph(tx)
          for (const record of ownership.tasks.values()) {
            if (
              record.state.status !== 'terminal' &&
              Ownership.inConversation(ownership, record, id, background)
            ) {
              yield* tx.write({ _tag: 'task', value: { ...record, abortRequested: true } })
              admitted.add(record.id)
            }
          }
        }),
      )
      yield* wake
      yield* scheduler.idle(id, background)
    }),
    idle: Effect.fn('Scheduler.idle')(function* (id, background = false) {
      const deferred = yield* Deferred.make<void, StorageError>()
      const waiter = { conversationId: id, background, deferred }
      const idle = yield* session.transaction(
        Effect.fn('Scheduler.registerIdle')(function* (tx) {
          yield* checkOpen
          const ownership = yield* graph(tx)
          const ready = !Array.from(ownership.tasks.values()).some(
            (record) =>
              record.state.status !== 'terminal' &&
              Ownership.inConversation(ownership, record, id, background),
          )
          if (!ready) idleWaiters.add(waiter)
          return ready
        }),
      )
      if (idle) return
      yield* wake
      yield* Deferred.await(deferred).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            idleWaiters.delete(waiter)
          }),
        ),
      )
    }),
  }
  return scheduler
})
