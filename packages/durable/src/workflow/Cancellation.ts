import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as FiberHandle from 'effect/FiberHandle'
import * as Scope from 'effect/Scope'
import * as Ref from 'effect/Ref'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Ownership from '../Ownership.ts'
import * as Session from '../Session.ts'
import { ExecutionError, InvalidState, Aborted } from './ExecutionError.ts'

/** Owner-local capabilities supplement native engine cancellation without replacing its journal. */
export class Cancellation extends Context.Service<
  Cancellation,
  {
    readonly register: (
      identity: Ownership.Identity,
      cancel: Effect.Effect<void>,
    ) => Effect.Effect<void, never, Scope.Scope>
    readonly cancel: (sessionId: string, reached: Ownership.Reached) => Effect.Effect<void>
  }
>()('@effect-harness/durable/Cancellation') {}

export const layer = Layer.sync(Cancellation, () => {
  const live = new Map<string, Set<Effect.Effect<void>>>()
  const key = (sessionId: string, id: number) => JSON.stringify([sessionId, id])
  return Cancellation.of({
    register: (identity, cancel) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const registration = Effect.suspend(() => cancel)
          const address = key(identity.sessionId, identity.taskId)
          const registrations = live.get(address) ?? new Set<Effect.Effect<void>>()
          registrations.add(registration)
          live.set(address, registrations)
          return { address, registrations, registration }
        }),
        ({ address, registrations, registration }) =>
          Effect.sync(() => {
            registrations.delete(registration)
            if (registrations.size === 0 && live.get(address) === registrations)
              live.delete(address)
          }),
      ).pipe(Effect.asVoid),
    cancel: (sessionId, reached) =>
      Effect.forEach(
        reached.tasks,
        (task) =>
          Effect.forEach([...(live.get(key(sessionId, task.id)) ?? [])], (cancel) => cancel, {
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
          reason: new InvalidState({ message: 'Abort target is absent' }),
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
      const completed = yield* Deferred.make<void>()
      // Published once: None means the invocation closed before body startup.
      const ready = yield* Deferred.make<Option.Option<Fiber.Fiber<A, E>>>()
      const launched = yield* Ref.make(false)
      return yield* Effect.gen(function* () {
        yield* session
          .onClose(
            Effect.gen(function* () {
              closing = true
              yield* Deferred.succeed(closed, undefined)
              // Actual exit must precede receipt observation: an early receipt can
              // interrupt the enclosing native Activity before it records suspension.
              const bodyFiber = yield* Deferred.await(ready)
              if (Option.isSome(bodyFiber)) yield* Fiber.await(bodyFiber.value)
              yield* Deferred.await(completed)
            }),
          )
          .pipe(
            Effect.catchIf(
              (error) => error.reason._tag === 'Closed',
              () => pause,
            ),
          )
        if (closing || (yield* session.isClosed)) return yield* pause
        // Acquired after registration: Scope joins the body handle before removing
        // its cleanup membership, including external invocation interruption.
        const handle = yield* FiberHandle.make<A, E>()
        // The race first requests native suspension, then interrupts and joins body
        // finalizers. Keep this immutable fiber independently of handle occupancy.
        const fiber = yield* Effect.uninterruptible(
          Effect.gen(function* () {
            yield* Ref.set(launched, true)
            const fiber = yield* FiberHandle.run(
              handle,
              Effect.scoped(
                body.pipe(
                  Effect.provide(
                    Ownership.layerCurrent(identity).pipe(
                      Layer.provide(Layer.succeed(Session.Session, session)),
                    ),
                  ),
                ),
              ).pipe(
                Effect.raceFirst(Deferred.await(closed).pipe(Effect.andThen(pause))),
                Effect.interruptible,
              ),
              { startImmediately: false },
            )
            yield* Deferred.succeed(ready, Option.some(fiber))
            // Acquire and install this join without an interruption gap, including
            // interruption before the deferred body has entered any onExit handler.
            yield* Effect.addFinalizer(() =>
              Fiber.interrupt(fiber).pipe(Effect.andThen(Deferred.succeed(completed, undefined))),
            )
            return fiber
          }),
        )
        const exit = yield* Fiber.await(fiber)
        if (closing || (yield* session.isClosed)) return yield* pause
        return yield* exit
      }).pipe(
        Effect.ensuring(
          Ref.get(launched).pipe(
            Effect.flatMap((started) =>
              started
                ? Effect.void
                : Deferred.succeed(ready, Option.none()).pipe(
                    Effect.andThen(Deferred.succeed(completed, undefined)),
                  ),
            ),
          ),
        ),
      )
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
            reason: new Aborted({ message: 'Task cannot enter an invocation' }),
          })
        if (initial.conversationId !== identity.conversationId)
          return yield* new ExecutionError({
            reason: new InvalidState({
              message: 'Invocation task belongs to another conversation',
            }),
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
            (error) => error.reason._tag === 'Closed',
            () => Effect.void,
          ),
          Effect.orDie,
        )
        yield* capabilities.register(identity, stop)
        const monitor = yield* Effect.forever(
          Effect.gen(function* () {
            if (yield* session.isClosed) return yield* Effect.never
            const state = yield* session.committed.pipe(
              Effect.catchIf(
                (error) => error.reason._tag === 'Closed',
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
            reason: new Aborted({ message: 'Task has a durable abort mark' }),
          })
        return yield* exit
      }),
    ),
  )
