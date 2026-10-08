import * as TestClock from 'effect/testing/TestClock'
import type * as Scope from 'effect/Scope'
import * as Fiber from 'effect/Fiber'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as Event from 'effect-harness/durable/Event'
import * as View from 'effect-harness/durable/View'
import * as Session from 'effect-harness/durable/Session'
import * as Memory from 'effect-harness/durable/storage/Memory'

// Advance modeled journal polling only after its wait/consumer fiber has been admitted.
const awaitObserved = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | Scope.Scope> =>
  Effect.gen(function* () {
    const fiber = yield* effect.pipe(Effect.forkScoped)
    for (let attempt = 0; attempt < 100; attempt++) {
      if (fiber.pollUnsafe() !== undefined) return yield* Fiber.join(fiber)
      yield* TestClock.adjust('20 millis')
    }
    return yield* Effect.die('Admitted observation did not settle')
  })
describe('EventParity', () => {
  it.effect(
    'ordinary failed receipts do not emit task_failed; faulted and orphaned receipts do',
    () =>
      Effect.gen(function* () {
        const session = yield* Session.Session
        const root = yield* session.root()
        const watch = yield* (yield* Event.Event).watch(root.id)
        for (const status of ['failed', 'faulted', 'orphaned'])
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              yield* tx.createTask({
                conversationId: root.id,
                kind: 'custom',
                version: 1,
                input: null,
                background: false,
                abortRequested: false,
                state: { status: 'terminal', outcome: { status, detail: status } },
              })
              yield* tx.appendEntry(root.id, { kind: 'boundary', data: { status } })
            }),
          )
        const batches = yield* awaitObserved(
          Stream.runCollect(watch.changes.pipe(Stream.take(3))),
        ).pipe(Effect.timeout('3 seconds'))
        assert.strictEqual(
          batches[0]?.some((event) => event.type === 'task_failed'),
          false,
        )
        assert.deepStrictEqual(
          batches
            .slice(1)
            .flatMap((batch) =>
              batch.filter((event) => event.type === 'task_failed').map((event) => event.message),
            ),
          ['faulted', 'orphaned'],
        )
      }).pipe(
        Effect.provide(
          Event.layer.pipe(
            Layer.provideMerge(View.layer),
            Layer.provideMerge(Session.layer),
            Layer.provide(Memory.layer),
          ),
        ),
      ),
  )
})
