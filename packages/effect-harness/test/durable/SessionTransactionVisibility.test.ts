import { assertFailure } from '@effect/vitest/utils'
import { assert, describe, it } from '@effect/vitest'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Fiber from 'effect/Fiber'

import * as Schema from 'effect/Schema'

import * as Option from 'effect/Option'

import * as Document from 'effect-harness/durable/Document'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

const counter = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})

describe('SessionTransactionVisibility', () => {
  it.effect('publishes a transaction candidate only after its callback succeeds', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      yield* session.transaction((tx) => tx.doc(counter).pipe(Effect.asVoid))
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const writing = yield* session
        .transaction(
          Effect.fnUntraced(function* (tx) {
            ;(yield* tx.doc(counter)).count = 7
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
            return yield* Effect.fail('rollback')
          }),
        )
        .pipe(Effect.result, Effect.forkScoped)
      yield* Deferred.await(entered)
      assert.strictEqual(
        (yield* session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
        0,
      )
      yield* Deferred.succeed(release, undefined)
      assertFailure(yield* Fiber.join(writing), 'rollback')
      assert.strictEqual(
        (yield* session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
        0,
      )
    }),
  )
})
