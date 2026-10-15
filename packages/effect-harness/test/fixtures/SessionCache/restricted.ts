import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Document from '../../../src/Document.js'
import * as Session from '../../../src/Session.js'
import * as Storage from '../../../src/Storage.js'

export const verify = Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* Storage.Storage
      let finds = 0
      const counted: Storage.Storage['Service'] = {
        ...storage,
        findDocument: (address, at) =>
          Effect.suspend(() => {
            finds++
            return storage.findDocument(address, at)
          }),
      }
      const session = yield* Session.make({ documentRetentionMs: 25 }).pipe(
        Effect.provideService(Storage.Storage, counted),
      )
      const document = Document.define({
        kind: 'restricted-cache',
        scope: 'session',
        version: 1,
        schema: Schema.Struct({ count: Schema.Natural }),
        initial: () => ({ count: 0 }),
      })
      const target = { scope: { _tag: 'session' } } as const
      yield* Session.snapshot(session, document, target)
      yield* Effect.sleep(100)
      yield* Session.commit(session, () => Effect.void)
      yield* Session.snapshot(session, document, target)
      if (finds !== 2) return yield* Effect.die('Commit did not sweep the expired cached miss')
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)
