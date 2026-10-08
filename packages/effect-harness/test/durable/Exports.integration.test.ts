import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import { fileURLToPath } from 'node:url'

class EmittedPackage extends Context.Service<EmittedPackage, { readonly directory: string }>()(
  'effect-harness/test/durable/Exports/EmittedPackage',
) {
  static layer = Layer.effect(EmittedPackage)(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const original = fileURLToPath(new URL('../../', import.meta.url))
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'durable-exports-' })
      yield* fs.copyFile(path.join(original, 'package.json'), path.join(directory, 'package.json'))
      yield* fs.copy(path.join(original, 'dist'), path.join(directory, 'dist'))
      yield* fs.symlink(path.join(original, 'node_modules'), path.join(directory, 'node_modules'))
      yield* fs.copyFile(
        fileURLToPath(new URL('./ExportsFixture.mjs', import.meta.url)),
        path.join(directory, 'exports.mjs'),
      )
      yield* fs.makeDirectory(path.join(directory, 'dist/durable/storage/internal/nested/deeper'), {
        recursive: true,
      })
      yield* fs.writeFileString(
        path.join(directory, 'dist/durable/storage/internal/nested/deeper/state.js'),
        'export const privateProbe = true\n',
      )
      return EmittedPackage.of({ directory })
    }),
  )
}

describe('Exports', () => {
  // effect-nit-allow P8-it-live-or-withLive-for-real-time: a fresh native Node process resolves actual emitted JavaScript and export-map denials; TestClock and source aliases cannot run this consumer proof.
  it.live('blocks actual private Node package imports', () =>
    Effect.gen(function* () {
      const fixture = yield* EmittedPackage
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
      const child = yield* spawner.spawn(
        ChildProcess.make(process.execPath, ['exports.mjs'], { cwd: fixture.directory }),
      )
      const [stdout, stderr] = yield* Effect.all(
        [
          child.stdout.pipe(
            Stream.decodeText(),
            Stream.runCollect,
            Effect.map((parts) => parts.join('')),
          ),
          child.stderr.pipe(
            Stream.decodeText(),
            Stream.runCollect,
            Effect.map((parts) => parts.join('')),
          ),
        ],
        { concurrency: 2 },
      )
      assert.strictEqual(yield* child.exitCode, 0, stderr)
      assert.include(stdout, '"publicStoreImported":true')
      assert.include(stdout, '"publicCloneErrorImported":true')
    }).pipe(Effect.provide(EmittedPackage.layer.pipe(Layer.provideMerge(NodeServices.layer)))),
  )
})
