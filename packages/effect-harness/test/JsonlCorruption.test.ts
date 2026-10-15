import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Storage from 'effect-harness/Storage'

it.live('invalid UTF-8 in a committed sidecar fails before tail truncation or orphan cleanup', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'sidecar-corruption-' })
      const filename = `${directory}/store`
      const layer = Storage.layerJsonl({ filePath: filename })
      yield* Effect.service(Storage.Storage).pipe(
        Effect.flatMap((storage) =>
          storage.commit([
            {
              _tag: 'document.create',
              record: {
                id: Storage.DocumentId.make(2),
                kind: 'image',
                scope: { _tag: 'session' },
              },
              content: { _tag: 'base', version: 1, value: { image: 'image-marker' } },
            },
          ]),
        ),
        Effect.provide(layer),
      )
      const body = `${filename}.parts/2-1.revision`
      const bytes = yield* fs.readFile(body)
      const offset = new TextDecoder().decode(bytes).indexOf('image-marker')
      assert.isAtLeast(offset, 0)
      bytes[offset] = 0xff // Still valid JSON after lossy UTF-8 replacement, but altered content.
      yield* fs.writeFile(body, bytes)
      yield* fs.writeFileString(filename, '{torn', { flag: 'a' })
      yield* fs.writeFileString(`${filename}.parts/9-9.row`, 'orphan evidence')
      const journal = yield* fs.readFile(filename)
      const error = yield* Effect.service(Storage.Storage).pipe(Effect.provide(layer), Effect.flip)
      assert.strictEqual(error.reason, 'corrupt')
      assert.deepEqual(yield* fs.readFile(filename), journal)
      assert.deepEqual(yield* fs.readFile(body), bytes)
      assert.strictEqual(yield* fs.readFileString(`${filename}.parts/9-9.row`), 'orphan evidence')
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
