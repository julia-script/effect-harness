import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Store from 'effect-harness/durable/Store'

import * as Session from 'effect-harness/durable/Session'

import * as View from 'effect-harness/durable/View'

import * as Event from 'effect-harness/durable/Event'

describe('EventConstruction', () => {
  it.effect(
    'constructs View and Event from the provided services with the same committed mount',
    () =>
      Effect.gen(function* () {
        const store = yield* Store.makeMemory
        const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        const root = yield* session.root()
        const views = yield* View.make.pipe(Effect.provideService(Store.Store, store))
        const events = yield* Event.make.pipe(Effect.provideService(View.View, views))
        const structural = yield* views.watch(root.id)
        const semantic = yield* events.watch(root.id)
        assert.strictEqual(semantic.snapshot.entries, structural.value.entries)
        assert.strictEqual(semantic.snapshot.entries.length, 0)
      }),
  )
})
