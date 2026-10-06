import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Ownership from '../Ownership.ts'
import type * as Session from '../Session.ts'
import { ExecutionError } from './ExecutionError.ts'

/** Owner-local capabilities supplement native engine cancellation without replacing its journal. */
export class Cancellation extends Context.Service<
  Cancellation,
  {
    readonly register: (
      identity: Ownership.Identity,
      cancel: Effect.Effect<void>,
    ) => Effect.Effect<() => void>
    readonly cancel: (sessionId: string, reached: Ownership.Reached) => Effect.Effect<void>
  }
>()('@effect-harness/durable/Cancellation') {}

export const layer = Layer.sync(Cancellation, () => {
  const live = new Map<string, Set<Effect.Effect<void>>>()
  const key = (sessionId: string, id: number) => JSON.stringify([sessionId, id])
  return Cancellation.of({
    register: (identity, cancel) =>
      Effect.sync(() => {
        const address = key(identity.sessionId, identity.taskId)
        const registrations = live.get(address) ?? new Set<Effect.Effect<void>>()
        registrations.add(cancel)
        live.set(address, registrations)
        return () => {
          registrations.delete(cancel)
          if (registrations.size === 0) live.delete(address)
        }
      }),
    cancel: (sessionId, reached) =>
      Effect.forEach(
        reached.tasks,
        (task) =>
          Effect.forEach(live.get(key(sessionId, task.id)) ?? [], (cancel) => cancel, {
            discard: true,
          }),
        { discard: true },
      ),
  })
})

/** Commit the complete bottom-up reach before any owner-local cancellation is signalled. */
export const mark = Effect.fnUntraced(function* (
  session: Session.Service,
  target: Ownership.Target,
  options?: { readonly background?: boolean },
) {
  return yield* session.transaction(
    Effect.fnUntraced(function* (tx) {
      const graph = yield* Ownership.readGraph(tx)
      const reached = Ownership.reach(graph, target, options?.background)
      if (reached === undefined)
        return yield* new ExecutionError({
          reason: 'invalid_state',
          message: 'Abort target is absent',
        })
      for (const task of reached.tasks)
        if (!task.abortRequested)
          yield* tx.write({ type: 'task', value: { ...task, abortRequested: true } })
      return reached
    }),
  )
})

export const cancel = Effect.fnUntraced(function* (sessionId: string, reached: Ownership.Reached) {
  yield* (yield* Cancellation).cancel(sessionId, reached)
})

/**
 * Scope an Activity body to its Session without reading committed storage.
 * Use inside a native Activity execute effect, including transaction-annotated
 * hooks. Closing requests public native suspension before interrupting the
 * body and joins its resource finalizers; no domain outcome is written here.
 */
export const activity = <A, E, R>(
  identity: Ownership.Identity,
  session: Session.Service,
  body: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | import('../StorageError.ts').StorageError, Exclude<R, Ownership.Current>> =>
  Effect.scoped(
    Effect.gen(function* () {
      const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
      const pause = Option.isSome(instance) ? Workflow.suspend(instance.value) : Effect.interrupt
      if (yield* session.isClosed) return yield* pause
      let closing = false
      const closed = yield* Deferred.make<void>()
      let fiber: Fiber.Fiber<A, E> | undefined
      const unregister = yield* session
        .onClose(
          Effect.gen(function* () {
            closing = true
            yield* Deferred.succeed(closed, undefined)
            if (fiber !== undefined) yield* Fiber.await(fiber)
          }),
        )
        .pipe(
          Effect.catchIf(
            (error) => error.reason === 'closed',
            () => pause,
          ),
        )
      yield* Effect.addFinalizer(() => Effect.sync(unregister))
      if (closing || (yield* session.isClosed)) return yield* pause
      fiber = yield* body.pipe(
        Effect.provide(Ownership.layerCurrent(identity, session)),
        Effect.raceFirst(Deferred.await(closed).pipe(Effect.andThen(pause))),
        Effect.interruptible,
        Effect.forkScoped,
      )
      const exit = yield* Fiber.await(fiber)
      if (closing || (yield* session.isClosed)) return yield* pause
      return yield* exit
    }),
  )

/**
 * Wrap an invocation with physical abort fencing and a scoped monitor.
 * The Activity guard owns close suspension; abort still joins body finalizers
 * and reports a typed aborted result for the executor's domain settlement.
 * Do not call this physical-read boundary inside a SQL-annotated Activity;
 * use activity there and retain the enclosing invocation's abort monitor.
 */
export const run = <A, E, R>(
  identity: Ownership.Identity,
  session: Session.Service,
  body: Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | ExecutionError | import('../StorageError.ts').StorageError,
  Exclude<R, Ownership.Current> | Cancellation
> =>
  activity(
    identity,
    session,
    Effect.scoped(
      Effect.gen(function* () {
        const capabilities = yield* Cancellation
        const initial = (yield* session.committed).tasks.find((task) => task.id === identity.taskId)
        if (initial === undefined || initial.abortRequested || initial.state.status === 'terminal')
          return yield* new ExecutionError({
            reason: 'aborted',
            message: 'Task cannot enter an invocation',
          })
        if (initial.conversationId !== identity.conversationId)
          return yield* new ExecutionError({
            reason: 'invalid_state',
            message: 'Invocation task belongs to another conversation',
          })
        let aborted = false
        const fiber = yield* body.pipe(Effect.interruptible, Effect.forkScoped)
        const stop = Effect.gen(function* () {
          if (yield* session.isClosed) return
          const task = (yield* session.committed).tasks.find((task) => task.id === identity.taskId)
          if (task !== undefined && !task.abortRequested) return
          aborted = true
          yield* Fiber.interrupt(fiber)
        }).pipe(
          Effect.catchIf(
            (error) => error.reason === 'closed',
            () => Effect.void,
          ),
          Effect.orDie,
        )
        const unregister = yield* capabilities.register(identity, stop)
        yield* Effect.addFinalizer(() => Effect.sync(unregister))
        const monitor = yield* Effect.forever(
          Effect.gen(function* () {
            if (yield* session.isClosed) return yield* Effect.never
            const state = yield* session.committed.pipe(
              Effect.catchIf(
                (error) => error.reason === 'closed',
                () => Effect.never,
              ),
            )
            const task = state.tasks.find((task) => task.id === identity.taskId)
            if (task === undefined || task.abortRequested) yield* stop
            yield* Effect.sleep('20 millis')
          }),
        ).pipe(Effect.forkScoped)
        const exit = yield* Effect.raceFirst(Fiber.await(fiber), Fiber.join(monitor))
        if (aborted)
          return yield* new ExecutionError({
            reason: 'aborted',
            message: 'Task has a durable abort mark',
          })
        return yield* exit
      }),
    ),
  )
