import { assertFailure } from '@effect/vitest/utils'
import { assert, describe, it } from '@effect/vitest'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Fiber from 'effect/Fiber'

import type * as Layer from 'effect/Layer'

import * as KeyValueStore from 'effect/persistence/KeyValueStore'

import type * as EventJournal from 'effect/eventlog/EventJournal'

import * as Record from 'effect-harness/durable/Record'

import * as Store from 'effect-harness/durable/Store'

import * as SnapshotStore from 'effect-harness/durable/storage/SnapshotStore'

import type * as SqlError from 'effect/sql/SqlError'

export const cases = (
  name: string,
  environment: Layer.Layer<
    KeyValueStore.KeyValueStore | EventJournal.EventJournal,
    SqlError.SqlError
  >,
) => {
  describe(`Snapshot reads (${name})`, () => {
    it.effect('a paused read retains a coherent saved value while another writer commits', () =>
      Effect.gen(function* () {
        const values = yield* KeyValueStore.KeyValueStore
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let gate = false
        const wrapped = KeyValueStore.make({
          ...values,
          get: (key) =>
            values.get(key).pipe(
              Effect.tap(() => {
                if (!gate) return Effect.void
                gate = false
                return Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                )
              }),
            ),
        })
        const store = yield* SnapshotStore.make().pipe(
          Effect.provideService(KeyValueStore.KeyValueStore, wrapped),
        )
        gate = true
        const reading = yield* store.read.pipe(Effect.forkScoped)
        yield* Deferred.await(entered)
        yield* store.commit([], { key: 'new-receipt' })
        yield* Deferred.succeed(release, undefined)
        const snapshot = yield* Fiber.join(reading)
        assert.strictEqual(snapshot.nextSeq, 1)
        assert.deepStrictEqual(snapshot.receipts, [])
        const saved = yield* store.journal(0)
        assert.strictEqual(saved.state.nextSeq, 2)
        assert.deepStrictEqual(
          saved.state.receipts.map((receipt) => receipt.key),
          ['new-receipt'],
        )
        assert.strictEqual(saved.frames[0]?.seq, 1)
      }).pipe(Effect.provide(environment)),
    )

    it.effect('reads never expose a failed transaction candidate', () =>
      Effect.gen(function* () {
        const store = yield* SnapshotStore.make()
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const writing = yield* store
          .transact(
            (state) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(Effect.fail('candidate rejected')),
                Effect.as(Store.makeCandidate({ state, writes: [], result: undefined })),
              ),
            { key: 'not-published' },
          )
          .pipe(Effect.result, Effect.forkScoped)
        yield* Deferred.await(entered)
        const reading = yield* store.committed.pipe(Effect.forkScoped)
        yield* Deferred.succeed(release, undefined)
        assertFailure(yield* Fiber.join(writing), 'candidate rejected')
        assert.deepStrictEqual(yield* Fiber.join(reading), Record.emptyState())
        assert.deepStrictEqual((yield* store.journal(0)).frames, [])
      }).pipe(Effect.provide(environment)),
    )
  })
}
