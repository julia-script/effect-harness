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

export const bytesPerSecond = 100 * 1024
const TypeId = '~@effect-harness/harness/Progress'
export interface Progress<in out E> {
  readonly [TypeId]: { readonly _E: Types.Invariant<E> }
  readonly mark: Effect.Effect<void>
  readonly markAndWait: Effect.Effect<void, E>
  /** Stop future writes and join any admitted write. The final domain commit must settle returned waiters. */
  readonly stop: Effect.Effect<ReadonlyArray<Deferred.Deferred<void, E>>>
}
export const isProgress = (input: unknown): input is Progress<unknown> =>
  Predicate.hasProperty(input, TypeId)
export const makeProgress = <E>(input: Omit<Progress<E>, typeof TypeId>): Progress<E> => {
  const handle: Progress<E> = {
    [TypeId]: { _E: identity },
    get mark() {
      return input.mark
    },
    get markAndWait() {
      return input.markAndWait
    },
    get stop() {
      return input.stop
    },
  }
  Object.defineProperties(handle, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(handle, TypeId, { enumerable: false })
  return handle
}
interface State<E> {
  readonly dirty: boolean
  readonly stopped: boolean
  readonly nextAt: DateTime.Utc | undefined
  readonly waiters: ReadonlyArray<Deferred.Deferred<void, E>>
}
/** Scoped, one-in-flight progress writer. First change is immediate; later changes coalesce behind time/size pacing. */
export const make = Effect.fnUntraced(function* <E, R>(
  write: Effect.Effect<number, E, R>,
  minIntervalMs: Duration.Input,
  report: (cause: Cause.Cause<E>) => Effect.Effect<void> = () => Effect.void,
): Effect.fn.Return<Progress<E>, Schema.SchemaError, R | Scope.Scope> {
  // Standalone pacing accepted finite fractional intervals before this migration;
  // Settings/window codecs retain their own integer/range policies at admission.
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
                outcome._tag === 'Success' ? (outcome.value * 1000) / bytesPerSecond : 0,
              ),
            )
            yield* Ref.update(state, (value) => ({
              ...value,
              nextAt: DateTime.addDuration(started, spacing),
            }))
            const receipt = Exit.map(outcome, () => undefined)
            for (const waiter of pending) yield* Deferred.done(waiter, receipt)
            if (outcome._tag === 'Failure') yield* report(outcome.cause)
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
export const settle = <E>(
  waiters: ReadonlyArray<Deferred.Deferred<void, E>>,
  outcome: Exit.Exit<void, E>,
): Effect.Effect<void> =>
  Effect.asVoid(Effect.forEach(waiters, (waiter) => Deferred.done(waiter, outcome)))
