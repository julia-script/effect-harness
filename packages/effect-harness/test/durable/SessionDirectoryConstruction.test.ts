import * as Identity from 'effect-harness/durable/Identity'

import { assert, describe, it } from '@effect/vitest'

import * as Context from 'effect/Context'

import * as Effect from 'effect/Effect'

import * as Layer from 'effect/Layer'

import * as Option from 'effect/Option'

import * as Store from 'effect-harness/durable/Store'

import * as Session from 'effect-harness/durable/Session'

import * as SessionDirectory from 'effect-harness/durable/SessionDirectory'

describe('SessionDirectoryConstruction', () => {
  it.effect(
    'snapshots explicit registrations while preserving exact scoped Session references',
    () =>
      Effect.gen(function* () {
        const store = yield* Store.makeMemory
        const first = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        const second = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        const registrations = new Map([[Identity.SessionId.make('first'), first]])
        const context = yield* Layer.build(SessionDirectory.layer).pipe(
          Effect.provideService(SessionDirectory.Registrations, registrations),
        )
        const directory = Context.get(context, SessionDirectory.SessionDirectory)
        registrations.set(Identity.SessionId.make('first'), second)
        registrations.set(Identity.SessionId.make('late'), second)
        assert.strictEqual(yield* directory.resolve(Identity.SessionId.make('first')), first)
        assert.strictEqual(
          (yield* directory.resolve(Identity.SessionId.make('late')).pipe(Effect.flip)).reason._tag,
          'NotFoundError',
        )
        const root = yield* first.root()
        assert.strictEqual(
          (yield* (yield* directory.resolve(Identity.SessionId.make('first')))
            .conversation(root.id)
            .pipe(Effect.map(Option.getOrUndefined)))?.id,
          root.id,
        )
        const single = yield* SessionDirectory.SessionDirectory.pipe(
          Effect.provide(SessionDirectory.layerSingle(Identity.SessionId.make('single'))),
          Effect.provideService(Session.Session, second),
        )
        assert.strictEqual(yield* single.resolve(Identity.SessionId.make('single')), second)
      }),
  )
})
