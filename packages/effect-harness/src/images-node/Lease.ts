import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'

/** Explicit Scope.close can run on another fiber despite the caller's interruption mask.
 * Register users synchronously before exposing native work, reject new users on close,
 * and keep borrowed bytes/handles alive until all admitted users actually settle.
 */
export interface Lease<A> {
  readonly value: A
  readonly pending: Set<Deferred.Deferred<void>>
  closed: boolean
}

export const acquire = <A, E, R>(effect: Effect.Effect<A, E, R>, destroy: (a: A) => void) =>
  Effect.acquireRelease(
    Effect.map(effect, (value): Lease<A> => ({ value, pending: new Set(), closed: false })),
    (lease) =>
      Effect.gen(function* () {
        const pending = yield* Effect.sync(() => {
          lease.closed = true
          return Array.from(lease.pending)
        })
        yield* Effect.forEach(pending, Deferred.await, { discard: true })
        yield* Effect.sync(() => destroy(lease.value))
      }),
  )

export const use = <A, B, E, R, E2>(
  lease: Lease<A>,
  closed: () => E2,
  effect: (a: A) => Effect.Effect<B, E, R>,
): Effect.Effect<B, E | E2, R> =>
  Effect.uninterruptible(
    Effect.gen(function* () {
      const done = yield* Deferred.make<void>()
      const admitted = yield* Effect.sync(() => {
        if (lease.closed) return false
        lease.pending.add(done)
        return true
      })
      if (!admitted) return yield* Effect.fail(closed())
      return yield* Effect.suspend(() => effect(lease.value)).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            yield* Effect.sync(() => {
              lease.pending.delete(done)
            })
            yield* Deferred.succeed(done, undefined)
          }),
        ),
      )
    }),
  )
