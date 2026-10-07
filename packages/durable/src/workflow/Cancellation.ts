/**
 * Scoped physical invocation cancellation and suspension fencing.
 *
 * @since 0.0.0
 */
import * as Arr from 'effect/Array'
import { constant, identity } from 'effect/Function'
import type { StorageError } from '../StorageError.ts'
import type * as Identity from '../Identity.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as FiberHandle from 'effect/FiberHandle'
import type * as Scope from 'effect/Scope'
import * as Ref from 'effect/Ref'
import * as HashMap from 'effect/HashMap'
import * as HashSet from 'effect/HashSet'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Ownership from '../Ownership.ts'
import * as Session from '../Session.ts'
import { ExecutionError, InvalidState, Aborted } from './ExecutionError.ts'

/**
 * Owner-local capabilities supplement native engine cancellation without replacing its journal.
 *
 * @category services
 * @since 0.0.0
 */
export class Cancellation extends Context.Service<
  Cancellation,
  {
    readonly register: (
      identity: Ownership.Identity,
      cancel: Effect.Effect<void>,
    ) => Effect.Effect<void, never, Scope.Scope>
    readonly cancel: (
      sessionId: Identity.SessionId,
      reached: Ownership.Reached,
    ) => Effect.Effect<void>
  }
>()('@effect-harness/durable/workflow/Cancellation') {}

/**
 * layer service Layer.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer: Layer.Layer<Cancellation> = Layer.effect(
  Cancellation,
  Effect.gen(function* () {
    const live = yield* Ref.make(HashMap.empty<string, HashSet.HashSet<Effect.Effect<void>>>())
    const key = (sessionId: Identity.SessionId, id: number) => JSON.stringify([sessionId, id])
    return Cancellation.of({
      register: (identity, cancel) =>
        Effect.acquireRelease(
          Effect.gen(function* () {
            const registration = Effect.suspend(() => cancel)
            const address = key(identity.sessionId, identity.taskId)
            yield* Ref.update(live, (entries) =>
              HashMap.set(
                entries,
                address,
                HashSet.add(
                  Option.getOrElse(HashMap.get(entries, address), () => HashSet.empty()),
                  registration,
                ),
              ),
            )
            return { address, registration }
          }),
          ({ address, registration }) =>
            Ref.update(live, (entries) => {
              const registrations = Option.getOrElse(HashMap.get(entries, address), () =>
                HashSet.empty(),
              )
              const remaining = HashSet.remove(registrations, registration)
              return HashSet.size(remaining) === 0
                ? HashMap.remove(entries, address)
                : HashMap.set(entries, address, remaining)
            }),
        ).pipe(Effect.asVoid),
      cancel: Effect.fnUntraced(function* (sessionId, reached) {
        const entries = yield* Ref.get(live)
        yield* Effect.forEach(
          reached.tasks,
          (task) =>
            Effect.forEach(
              Option.getOrElse(HashMap.get(entries, key(sessionId, task.id)), () =>
                HashSet.empty(),
              ),
              identity,
              { discard: true },
            ),
          { discard: true },
        )
      }),
    })
  }),
)

/**
 * Commits the complete bottom-up reach before any owner-local cancellation is signalled.
 *
 * @category combinators
 * @since 0.0.0
 */
export const mark = (
  session: Session.Service,
  target: Ownership.Target,
  options?: { readonly background?: boolean | undefined },
): Effect.Effect<Ownership.Reached, StorageError | ExecutionError> =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const graph = yield* Ownership.readGraph(tx)
      const reachedOption = Ownership.reach(graph, target, options?.background)
      if (Option.isNone(reachedOption))
        return yield* new ExecutionError({
          reason: new InvalidState({ message: 'Abort target is absent' }),
        })
      const reached = reachedOption.value
      for (const task of reached.tasks)
        if (!task.abortRequested)
          yield* tx.write({ _tag: 'task', type: 'task', value: { ...task, abortRequested: true } })
      return reached
    }),
  )

/**
 * Interrupts registered invocations after durable abort admission.
 *
 * @category combinators
 * @since 0.0.0
 */
export const cancel = Effect.fnUntraced(function* (
  sessionId: Identity.SessionId,
  reached: Ownership.Reached,
): Effect.fn.Return<void, never, Cancellation> {
  yield* (yield* Cancellation).cancel(sessionId, reached)
})

/**
 * Scopes an Activity body to its Session without reading committed storage.
 *
 * **Details**
 *
 * Use inside a native Activity execute effect, including transaction-annotated hooks. Closing requests public native suspension before interrupting the body and joins its resource finalizers; no domain outcome is written here.
 *
 * @category combinators
 * @since 0.0.0
 */
export const activity = <A, E, R>(
  identity: Ownership.Identity,
  session: Session.Service,
  body: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | import('../StorageError.ts').StorageError, Exclude<R, Ownership.Current>> =>
  Effect.scoped(
    Effect.gen(function* () {
      const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
      const pause = Option.match(instance, {
        onNone: () => Effect.interrupt,
        onSome: Workflow.suspend,
      })
      if (yield* session.isClosed) return yield* pause
      const closing = yield* Ref.make(false)
      const closed = yield* Deferred.make<void>()
      const completed = yield* Deferred.make<void>()
      // Published once: None means the invocation closed before body startup.
      const ready = yield* Deferred.make<Option.Option<Fiber.Fiber<A, E>>>()
      const launched = yield* Ref.make(false)
      return yield* Effect.gen(function* () {
        yield* session
          .onClose(
            Effect.gen(function* () {
              yield* Ref.set(closing, true)
              yield* Deferred.succeed(closed, undefined)
              // Actual exit must precede receipt observation: an early receipt can
              // interrupt the enclosing native Activity before it records suspension.
              const bodyFiber = yield* Deferred.await(ready)
              if (Option.isSome(bodyFiber)) yield* Fiber.await(bodyFiber.value)
              yield* Deferred.await(completed)
            }),
          )
          .pipe(Effect.catchIf((error) => error.reason._tag === 'Closed', constant(pause)))
        if ((yield* Ref.get(closing)) || (yield* session.isClosed)) return yield* pause
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
                    // Invocation identity belongs only to this body; captured
                    // parent scopes must not retain the temporary Current layer.
                    { local: true },
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
        if ((yield* Ref.get(closing)) || (yield* session.isClosed)) return yield* pause
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
 * Wraps an invocation with physical abort fencing and a scoped monitor.
 *
 * **Details**
 *
 * The Activity guard owns close suspension; abort still joins body finalizers and reports a typed aborted result for the executor's domain settlement. Do not call this physical-read boundary inside a SQL-annotated Activity; use activity there and retain the enclosing invocation's abort monitor.
 *
 * @category combinators
 * @since 0.0.0
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
        const initial = Arr.findFirst(
          (yield* session.committed).tasks,
          (task) => task.id === identity.taskId,
        )
        if (
          Option.isNone(initial) ||
          initial.value.abortRequested ||
          initial.value.state.status === 'terminal'
        )
          return yield* new ExecutionError({
            reason: new Aborted({ message: 'Task cannot enter an invocation' }),
          })
        if (initial.value.conversationId !== identity.conversationId)
          return yield* new ExecutionError({
            reason: new InvalidState({
              message: 'Invocation task belongs to another conversation',
            }),
          })
        const aborted = yield* Ref.make(false)
        const fiber = yield* body.pipe(Effect.interruptible, Effect.forkScoped)
        const stop = Effect.gen(function* () {
          if (yield* session.isClosed) return
          const task = Arr.findFirst(
            (yield* session.committed).tasks,
            (task) => task.id === identity.taskId,
          )
          if (Option.isSome(task) && !task.value.abortRequested) return
          yield* Ref.set(aborted, true)
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
            const task = Arr.findFirst(state.tasks, (task) => task.id === identity.taskId)
            if (Option.isNone(task) || task.value.abortRequested) yield* stop
            yield* Effect.sleep('20 millis')
          }),
        ).pipe(Effect.forkScoped)
        const exit = yield* Effect.raceFirst(Fiber.await(fiber), Fiber.join(monitor))
        if (yield* Ref.get(aborted))
          return yield* new ExecutionError({
            reason: new Aborted({ message: 'Task has a durable abort mark' }),
          })
        return yield* exit
      }),
    ),
  )
