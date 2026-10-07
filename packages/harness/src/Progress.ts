/**
 * Scoped progress pacing and terminal acknowledgement settlement.
 *
 * @since 0.0.0
 */
import { constant } from 'effect/Function'
import { constUndefined } from 'effect/Function'
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
import type * as Types from 'effect/Types'
import { identity } from 'effect/Function'
import * as Cause from 'effect/Cause'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Queue from 'effect/Queue'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as Time from './Time.ts'

/**
 * Default progress throughput allowance in bytes per second.
 *
 * @category constants
 * @since 0.0.0
 */
export const bytesPerSecond = 100 * 1024
const TypeId = '~@effect-harness/harness/Progress'
/**
 * Progress progress contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Progress<in out E> extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: { readonly _E: Types.Invariant<E> }
  readonly mark: Effect.Effect<void>
  readonly markAndWait: Effect.Effect<void, E>
  /** Stop future writes and join any admitted write. The final domain commit must settle returned waiters. */
  readonly stop: Effect.Effect<ReadonlyArray<Deferred.Deferred<void, E>>>
}
/**
 * Checks whether an unknown value satisfies the Progress contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isProgress = (u: unknown): u is Progress<unknown> => Predicate.hasProperty(u, TypeId)
/**
 * Creates a fresh progress handle without evaluating its command getters.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeProgress = <E>(
  input: Omit<Progress<E>, typeof TypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable>,
): Progress<E> => {
  const handle: Progress<E> = Object.create(ProgressProto)
  Object.defineProperties(handle, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(handle, TypeId, { value: { _E: identity }, enumerable: false })
  return handle
}
interface State<E> {
  readonly dirty: boolean
  readonly stopped: boolean
  readonly nextAt: DateTime.Utc | undefined
  readonly waiters: ReadonlyArray<Deferred.Deferred<void, E>>
}
/**
 * Scoped, one-in-flight progress writer.
 *
 * **Details**
 *
 * First change is immediate; later changes coalesce behind time/size pacing.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = Effect.fnUntraced(function* <E, R>(
  write: Effect.Effect<number, E, R>,
  options: make.Options<E>,
): Effect.fn.Return<Progress<E>, Schema.SchemaError, R | Scope.Scope> {
  // Standalone pacing accepted finite fractional intervals before this migration;
  // Settings/window codecs retain their own integer/range policies at admission.
  const { minIntervalMs, report = () => Effect.void } = options
  const interval = yield* Time.duration(minIntervalMs)
  const services = yield* Effect.context<R>()
  const wake = yield* Queue.unbounded<void>()
  const state = yield* Ref.make<State<E>>({
    dirty: false,
    stopped: false,
    nextAt: undefined,
    waiters: [],
  })
  const worker = yield* Effect.forever(
    Effect.gen(function* () {
      yield* Queue.take(wake)
      while (true) {
        const current = yield* Ref.get(state)
        if (!current.dirty || current.stopped) break
        const now = yield* DateTime.now
        if (current.nextAt !== undefined && DateTime.isGreaterThan(current.nextAt, now))
          yield* Effect.sleep(DateTime.distance(now, current.nextAt))
        // Admission, native write and waiter receipts form one uninterruptible settlement.
        yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const pending = yield* Ref.modify(state, (value) =>
              value.stopped || !value.dirty
                ? ([undefined, value] as const)
                : ([value.waiters, { ...value, dirty: false, waiters: [] }] as const),
            )
            if (pending === undefined) return
            const started = yield* DateTime.now
            const outcome = yield* Effect.exit(write.pipe(Effect.provideContext(services)))
            const spacing = Duration.max(
              interval,
              Duration.millis(
                Exit.match(outcome, {
                  onSuccess: (self) => (self * 1000) / bytesPerSecond,
                  onFailure: constant(0),
                }),
              ),
            )
            yield* Ref.update(state, (value) => ({
              ...value,
              nextAt: DateTime.addDuration(started, spacing),
            }))
            const receipt = Exit.map(outcome, constUndefined)
            for (const waiter of pending) yield* Deferred.done(waiter, receipt)
            yield* Exit.match(outcome, { onSuccess: () => Effect.void, onFailure: report })
          }),
        )
      }
    }),
  ).pipe(Effect.forkScoped)
  const mark = Ref.modify(state, (value) =>
    value.stopped ? ([false, value] as const) : ([true, { ...value, dirty: true }] as const),
  ).pipe(
    Effect.flatMap((admitted) =>
      admitted ? Queue.offer(wake, undefined).pipe(Effect.asVoid) : Effect.void,
    ),
  )
  const markAndWait = Effect.gen(function* () {
    const waiter = yield* Deferred.make<void, E>()
    const stopped = yield* Ref.modify(
      state,
      (value) =>
        [
          value.stopped,
          {
            ...value,
            dirty: value.stopped ? value.dirty : true,
            waiters: [...value.waiters, waiter],
          },
        ] as const,
    )
    if (!stopped) yield* Queue.offer(wake, undefined)
    yield* Deferred.await(waiter)
  })
  const stop = Effect.uninterruptible(
    Effect.gen(function* () {
      yield* Ref.update(state, (value) => ({ ...value, stopped: true }))
      yield* Fiber.interrupt(worker)
      return yield* Ref.modify(
        state,
        (value) => [value.waiters, { ...value, waiters: [] }] as const,
      )
    }),
  )
  return makeProgress({ mark, markAndWait, stop })
})
/**
 * Completes admitted progress acknowledgements with the exact terminal exit.
 *
 * @category combinators
 * @since 0.0.0
 */
export const settle = <E>(
  waiters: ReadonlyArray<Deferred.Deferred<void, E>>,
  outcome: Exit.Exit<void, E>,
): Effect.Effect<void> =>
  Effect.forEach(waiters, (waiter) => Deferred.done(waiter, outcome), { discard: true })

/**
 * Type contracts owned by `make`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace make {
  /**
   * Configuration accepted by make.
   *
   * @category models
   * @since 0.0.0
   */
  interface Options<E> {
    readonly minIntervalMs: Duration.Input
    readonly report?: ((cause: Cause.Cause<E>) => Effect.Effect<void>) | undefined
  }
}

const ProgressProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return { _id: '@effect-harness/harness/Progress/Progress' }
  },
}
