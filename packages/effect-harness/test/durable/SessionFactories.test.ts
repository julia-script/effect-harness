import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Document from 'effect-harness/durable/Document'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

describe('SessionFactories', () => {
  it.effect(
    'lazy factory values allocate independent stores, sessions and migration identity caches',
    () =>
      Effect.gen(function* () {
        const first = yield* Store.makeMemory
        const second = yield* Store.makeMemory
        assert.notStrictEqual(first, second)
        const one = yield* Session.make.pipe(Effect.provideService(Store.Store, first))
        const two = yield* Session.make.pipe(Effect.provideService(Store.Store, second))
        yield* one.root()
        assert.deepStrictEqual((yield* two.committed).conversations, [])
        assert.notStrictEqual(
          yield* Document.makeMigrationCache,
          yield* Document.makeMigrationCache,
        )
      }),
  )
})
