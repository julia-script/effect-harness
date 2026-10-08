// effect-nit-allow P8-tests-import-public-specifiers: same-package tests delegate the null-exported View transition owner; all consumer proofs and private export denials retain actual package resolution.
// effect-nit-allow P9-no-internal-cross-import: same-package tests delegate the null-exported View transition owner; all consumer proofs and private export denials retain actual package resolution.
import * as ViewTransition from '../../../src/durable/internal/ViewTransition.ts'
/** Causal witnesses at the real SQL key/value read boundary after Store reader admission. */
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'
import * as TestStore from './TestStore.ts'

export class ReadAdmission extends Context.Service<
  ReadAdmission,
  {
    readonly track: (fiberId: number) => Effect.Effect<{
      readonly entered: Effect.Effect<void>
      readonly settled: Effect.Effect<void>
    }>
    readonly readStarted: (fiberId: number) => Effect.Effect<void>
    readonly readSettled: (fiberId: number) => Effect.Effect<void>
  }
>()('effect-harness/test/durable/storage/ReadAdmissionFixture/ReadAdmission') {
  static layer = Layer.effect(ReadAdmission)(
    Effect.sync(() => {
      const tracked = new Map<
        number,
        { readonly entered: Deferred.Deferred<void>; readonly settled: Deferred.Deferred<void> }
      >()
      return ReadAdmission.of({
        track: (id) =>
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>()
            const settled = yield* Deferred.make<void>()
            tracked.set(id, { entered, settled })
            return { entered: Deferred.await(entered), settled: Deferred.await(settled) }
          }),
        readStarted: (id) =>
          Effect.suspend(() => {
            const current = tracked.get(id)
            return current === undefined
              ? Effect.void
              : Deferred.succeed(current.entered, undefined).pipe(Effect.asVoid)
          }),
        readSettled: (id) =>
          Effect.suspend(() => {
            const current = tracked.get(id)
            return current === undefined
              ? Effect.void
              : Deferred.succeed(current.settled, undefined).pipe(Effect.asVoid)
          }),
      })
    }),
  )
}
const values = Layer.effect(KeyValueStore.KeyValueStore)(
  Effect.gen(function* () {
    const original = yield* KeyValueStore.KeyValueStore
    const admission = yield* ReadAdmission
    return KeyValueStore.make({
      ...original,
      get: (key) =>
        Effect.withFiber((fiber) =>
          admission.readStarted(fiber.id).pipe(
            Effect.andThen(original.get(key)),
            Effect.onExit(() => admission.readSettled(fiber.id)),
          ),
        ),
    })
  }),
).pipe(Layer.provideMerge(TestStore.persistence))
export const layer = SnapshotStore.layer.pipe(Layer.provideMerge(values))

const transitions = Layer.effect(ViewTransition.ViewTransition)(
  Effect.gen(function* () {
    const admission = yield* ReadAdmission
    return ViewTransition.ViewTransition.of({
      make: <A>(initial: A) =>
        Effect.map(ViewTransition.native.make(initial), (transition) => ({
          ...transition,
          modify: <B, E, R>(f: (state: A) => Effect.Effect<readonly [B, A], E, R>) =>
            Effect.withFiber((fiber) =>
              admission.readStarted(fiber.id).pipe(
                Effect.andThen(transition.modify(f)),
                Effect.onExit(() => admission.readSettled(fiber.id)),
              ),
            ),
        })),
    })
  }),
)
export const controls = transitions.pipe(Layer.provideMerge(ReadAdmission.layer))
