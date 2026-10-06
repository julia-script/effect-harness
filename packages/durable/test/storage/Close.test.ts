import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as Store from '../../src/Store.ts'
import * as Backend from '../../src/storage/Backend.ts'
import { rejected } from '../../src/StorageError.ts'

describe('sealed Session close with scoped cleanup', () => {
  it.live(
    'rejects admission immediately, finishes admitted work and keeps cleanup after a close waiter is cancelled',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let saved: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          let closes = 0
          const backend: Backend.Backend = {
            load: Effect.sync(() => saved),
            committed: Effect.sync(() => saved),
            save: (value) =>
              Effect.sync(() => {
                saved = value
              }),
            atomic: (effect) => effect,
            close: Effect.sync(() => {
              closes++
            }),
          }
          const store = yield* Backend.make(backend)
          const session = yield* Session.make().pipe(Effect.provideService(Store.Store, store))
          const root = yield* session.root()
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const admitted = yield* session
            .transaction(
              Effect.fnUntraced(function* (tx) {
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
                return yield* tx.appendEntry(root.id, { kind: 'admitted' })
              }),
            )
            .pipe(Effect.forkScoped)
          yield* Deferred.await(entered)
          const first = yield* session.close.pipe(Effect.forkScoped)
          yield* Effect.yieldNow
          const read = yield* session.committed.pipe(Effect.result)
          assert.strictEqual(read._tag, 'Failure')
          if (read._tag === 'Failure') assert.strictEqual(read.failure.reason, 'closed')
          const rejectedWrite = yield* session
            .transaction((tx) => tx.appendEntry(root.id, { kind: 'late' }))
            .pipe(Effect.result)
          assert.strictEqual(rejectedWrite._tag, 'Failure')
          assert.strictEqual(closes, 0)
          yield* Fiber.interrupt(first)
          assert.strictEqual((yield* Fiber.await(first))._tag, 'Failure')
          const second = yield* session.close.pipe(Effect.forkScoped)
          yield* Deferred.succeed(release, undefined)
          const entry = yield* Fiber.join(admitted)
          yield* Fiber.join(second)
          yield* session.close
          assert.strictEqual(closes, 1)
          assert.deepStrictEqual(
            saved.state.entries.map((item) => item.entry.id),
            [entry.id],
          )
          assert.strictEqual((yield* session.committed.pipe(Effect.result))._tag, 'Failure')
        }),
      ),
  )

  it.live(
    'waits for an admitted read and every close caller observes the same cleanup failure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let closes = 0
          const value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          const store = yield* Backend.make({
            load: Effect.succeed(value),
            committed: Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(value),
            ),
            save: () => Effect.void,
            atomic: (effect) => effect,
            close: Effect.sync(() => {
              closes++
            }).pipe(Effect.andThen(Effect.fail(rejected('close fixture', 'io')))),
          })
          const reader = yield* store.committed.pipe(Effect.forkScoped)
          yield* Deferred.await(entered)
          const first = yield* store.close.pipe(Effect.result, Effect.forkScoped)
          yield* Effect.yieldNow
          assert.strictEqual(closes, 0)
          yield* Deferred.succeed(release, undefined)
          assert.deepStrictEqual(yield* Fiber.join(reader), value.state)
          const result = yield* Fiber.join(first)
          const repeat = yield* store.close.pipe(Effect.result)
          assert.strictEqual(result._tag, 'Failure')
          assert.deepStrictEqual(repeat, result)
          assert.strictEqual(closes, 1)
        }),
      ),
  )
})
