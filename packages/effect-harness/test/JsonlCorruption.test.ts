import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Storage from 'effect-harness/Storage'

for (const boundary of ['invalid-utf8', 'missing-payload', 'unexpected-file'] as const)
  it.live(`${boundary} fails before tail truncation or orphan cleanup`, () =>
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
        const body = `${filename}.parts/doc-2.jsonl`
        const bytes = yield* fs.readFile(body)
        const offset = new TextDecoder().decode(bytes).indexOf('image-marker')
        assert.isAtLeast(offset, 0)
        if (boundary === 'invalid-utf8') {
          bytes[offset] = 0xff // Lossy UTF-8 decoding would silently alter valid JSON content.
          yield* fs.writeFile(body, bytes)
        } else if (boundary === 'missing-payload') yield* fs.remove(body)
        else yield* fs.writeFileString(`${filename}.parts/unrecognized`, 'evidence')
        yield* fs.writeFileString(filename, '{torn', { flag: 'a' })
        yield* fs.writeFileString(`${filename}.parts/doc-999.jsonl`, 'orphan evidence')
        const journal = yield* fs.readFile(filename)
        const error = yield* Effect.service(Storage.Storage).pipe(
          Effect.provide(layer),
          Effect.flip,
        )
        assert.strictEqual(error.reason, 'corrupt')
        assert.deepEqual(yield* fs.readFile(filename), journal)
        if (boundary === 'missing-payload') assert.isFalse(yield* fs.exists(body))
        else assert.deepEqual(yield* fs.readFile(body), bytes)
        assert.strictEqual(
          yield* fs.readFileString(`${filename}.parts/doc-999.jsonl`),
          'orphan evidence',
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live('payload streams without their main log are preserved and rejected', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped()
      const filename = `${directory}/missing.jsonl`
      yield* fs.makeDirectory(`${filename}.parts`)
      yield* fs.writeFileString(`${filename}.parts/doc-2.jsonl`, 'evidence')
      const error = yield* Effect.service(Storage.Storage).pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
        Effect.flip,
      )
      assert.strictEqual(error.reason, 'corrupt')
      assert.isFalse(yield* fs.exists(filename))
      assert.strictEqual(yield* fs.readFileString(`${filename}.parts/doc-2.jsonl`), 'evidence')
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
