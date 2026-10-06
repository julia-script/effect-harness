import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Queue from 'effect/Queue'
import type * as Scope from 'effect/Scope'

export const bytesPerSecond = 100 * 1024
export interface Progress<E> {
  readonly mark: Effect.Effect<void>
  readonly markAndWait: Effect.Effect<void, E>
  /** Stop future writes and join any admitted write. The final domain commit must settle returned waiters. */
  readonly stop: Effect.Effect<ReadonlyArray<Deferred.Deferred<void, E>>>
}
/** Scoped, one-in-flight progress writer. First change is immediate; later changes coalesce behind time/size pacing. */
export const make = <E, R>(
  write: Effect.Effect<number, E, R>,
  minIntervalMs: number,
  report: (cause: Cause.Cause<E>) => Effect.Effect<void> = () => Effect.void,
): Effect.Effect<Progress<E>, never, R | Scope.Scope> =>
  Effect.gen(function* () {
    const services = yield* Effect.context<R>()
    const wake = yield* Queue.unbounded<void>()
    let dirty = false
    let stopped = false
    let nextAt = 0
    let waiters: Array<Deferred.Deferred<void, E>> = []
    const worker = yield* Effect.forever(
      Effect.gen(function* () {
        yield* Queue.take(wake)
        while (dirty && !stopped) {
          const now = yield* Clock.currentTimeMillis
          if (nextAt > now) yield* Effect.sleep(nextAt - now)
          if (stopped) break
          dirty = false
          const pending = waiters
          waiters = []
          const started = yield* Clock.currentTimeMillis
          // Domain progress writers must settle atomically once admitted. Stop cannot interrupt an in-flight writer.
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              const outcome = yield* Effect.exit(write.pipe(Effect.provideContext(services)))
              nextAt =
                started +
                Math.max(
                  minIntervalMs,
                  outcome._tag === 'Success' ? (outcome.value * 1000) / bytesPerSecond : 0,
                )
              const receipt = Exit.map(outcome, () => undefined)
              for (const waiter of pending) yield* Deferred.done(waiter, receipt)
              if (outcome._tag === 'Failure') yield* report(outcome.cause)
            }),
          )
        }
      }),
    ).pipe(Effect.forkScoped)
    const mark = Effect.suspend(() => {
      if (stopped) return Effect.void
      dirty = true
      return Effect.asVoid(Queue.offer(wake, undefined))
    })
    const markAndWait = Effect.gen(function* () {
      const waiter = yield* Deferred.make<void, E>()
      waiters.push(waiter)
      yield* mark
      yield* Deferred.await(waiter)
    })
    const stop = Effect.gen(function* () {
      stopped = true
      yield* Fiber.interrupt(worker)
      const pending = waiters
      waiters = []
      return pending
    })
    return { mark, markAndWait, stop }
  })
export const settle = <E>(
  waiters: ReadonlyArray<Deferred.Deferred<void, E>>,
  outcome: Exit.Exit<void, E>,
): Effect.Effect<void> =>
  Effect.asVoid(Effect.forEach(waiters, (waiter) => Deferred.done(waiter, outcome)))
