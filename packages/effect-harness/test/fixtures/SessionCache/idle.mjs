import { NodeServices } from '@effect/platform-node'
import * as FileSystem from 'effect/FileSystem'
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'

if (typeof Bun === 'undefined') {
  const { register } = await import('node:module')
  register(new URL('../images-native/SourceLoader.mjs', import.meta.url))
}
const Session = await import('../../../src/Session.ts')
const Storage = await import('../../../src/Storage.ts')
const live = process.argv[2] === 'eviction'
const Document = await import('../../../src/Document.ts')

await Effect.runPromise(
  Effect.gen(function* () {
    // Deliberately independently owned: process exit must not require closing this scope.
    const scope = yield* Scope.make()
    const fs = yield* FileSystem.FileSystem
    const directory = yield* fs.makeTempDirectoryScoped()
    const services = yield* Layer.build(
      Storage.layerJsonl({ filePath: `${directory}/main.jsonl` }),
    ).pipe(Effect.provideService(Scope.Scope, scope))
    const storage = Context.get(services, Storage.Storage)
    let finds = 0
    const counted = {
      ...storage,
      findDocument: (address, at) =>
        Effect.suspend(() => {
          finds++
          return storage.findDocument(address, at)
        }),
    }
    const session = yield* Session.make(live ? { documentRetentionMs: 25 } : {}).pipe(
      Effect.provideService(Storage.Storage, counted),
      Effect.provideService(Scope.Scope, scope),
    )
    const document = Document.define({
      kind: 'idle-cache',
      scope: 'session',
      version: 1,
      schema: Schema.Struct({ count: Schema.Natural }),
      initial: () => ({ count: 0 }),
    })
    yield* Session.snapshot(session, document, { scope: { _tag: 'session' } })
    if (live) {
      yield* Session.snapshot(session, document, { scope: { _tag: 'session' } })
      if (finds !== 1) throw new Error('Missing document was not cached')
      yield* Effect.sleep(100)
      yield* Session.snapshot(session, document, { scope: { _tag: 'session' } })
      if (finds !== 2) throw new Error('Live timer did not evict the cached miss')
      console.log('live eviction observed')
    } else console.log('idle session ready')
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
)
