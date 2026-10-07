import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Schema from 'effect/Schema'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import type { StorageError } from '@effect-harness/durable/StorageError'
import { makeCandidate, Store } from '@effect-harness/durable/Store'
import * as Memory from '@effect-harness/durable/storage/Memory'
import { sessionLayer } from '@effect-harness/durable/testing/Storage'

class Decoder extends Context.Service<Decoder, { readonly wait: Effect.Effect<void> }>()(
  'test/SessionMint/Decoder',
) {}
const delayedId = Record.TaskId.pipe(
  Schema.middlewareDecoding((effect) =>
    Effect.gen(function* () {
      yield* (yield* Decoder).wait
      return yield* effect
    }),
  ),
)
const layers = sessionLayer(Memory.layer)

describe('Session.mint', () => {
  it.effect('serializes effectful decoding with its required services and replays receipts', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      let calls = 0
      const mint = session.transaction(
        (tx) => Effect.forEach([1, 2, 3], () => tx.mint(delayedId), { concurrency: 'unbounded' }),
        { key: 'minted' },
      )
      const decoder = Decoder.of({
        wait: Effect.sync(() => {
          calls++
        }).pipe(Effect.andThen(Effect.yieldNow)),
      })
      assert.deepStrictEqual(yield* mint.pipe(Effect.provideService(Decoder, decoder)), [2, 3, 4])
      assert.deepStrictEqual(yield* mint.pipe(Effect.provideService(Decoder, decoder)), [2, 3, 4])
      assert.strictEqual(calls, 3)
      assert.strictEqual((yield* session.committed).nextId, 5)
    }).pipe(Effect.provide(layers)),
  )

  it.effect('does not consume rejected identities and preserves allocator exhaustion', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const store = yield* Store
      const ids = yield* session.transaction((tx) =>
        Effect.gen(function* () {
          const rejected = yield* tx.mint(Schema.Literal(999)).pipe(Effect.flip)
          assert.strictEqual(rejected.reason._tag, 'Invalid')
          return yield* Effect.forEach([1, 2, 3], () => tx.mint(Record.TaskId), {
            concurrency: 'unbounded',
          })
        }),
      )
      assert.deepStrictEqual(ids, [2, 3, 4])
      yield* store.transact((state) =>
        Effect.succeed(
          makeCandidate({
            state: { ...state, nextId: Number.MAX_SAFE_INTEGER },
            writes: [],
            result: null,
          }),
        ),
      )
      const last = yield* session.transaction((tx) => tx.mint(Record.TaskId))
      assert.strictEqual(last, Number.MAX_SAFE_INTEGER)
      const exhausted = yield* session.transaction((tx) => tx.mint(Record.TaskId)).pipe(Effect.flip)
      assert.strictEqual(exhausted.reason._tag, 'Invalid')
      assert.strictEqual((yield* session.committed).nextId, Number.MAX_SAFE_INTEGER + 1)
    }).pipe(Effect.provide(layers)),
  )

  it.effect('tracks queued mints and checks revocation after obtaining the permit', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const queued = yield* Deferred.make<void>()
      let decodes = 0
      let holder: Fiber.Fiber<Record.TaskId, StorageError> | undefined
      let waiter: Fiber.Fiber<Record.TaskId, StorageError> | undefined
      const error = yield* session
        .transaction((tx) =>
          Effect.gen(function* () {
            holder = yield* tx.mint(delayedId).pipe(
              Effect.provideService(Decoder, {
                wait: Effect.sync(() => {
                  decodes++
                }).pipe(
                  Effect.andThen(Deferred.succeed(entered, undefined)),
                  Effect.andThen(Deferred.await(release)),
                ),
              }),
              Effect.forkScoped,
            )
            yield* Deferred.await(entered)
            waiter = yield* Deferred.succeed(queued, undefined).pipe(
              Effect.andThen(tx.mint(delayedId)),
              Effect.provideService(Decoder, {
                wait: Effect.sync(() => {
                  decodes++
                }),
              }),
              Effect.forkScoped,
            )
            yield* Deferred.await(queued)
            yield* Effect.yieldNow
          }),
        )
        .pipe(Effect.flip)
      assert.strictEqual(
        error.message,
        'Transaction callback settled before its pending operations',
      )
      yield* Deferred.succeed(release, undefined)
      assert.ok(holder)
      assert.ok(waiter)
      yield* Fiber.join(holder)
      const revoked = yield* Fiber.join(waiter).pipe(Effect.flip)
      assert.strictEqual(revoked.reason._tag, 'Revoked')
      assert.strictEqual(decodes, 1)
      assert.strictEqual((yield* session.committed).nextId, 2)
    }).pipe(Effect.provide(layers)),
  )
})
