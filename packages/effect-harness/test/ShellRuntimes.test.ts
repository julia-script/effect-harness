import { assert, describe, it } from '@effect/vitest'
import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'

const native = NodeChildProcessSpawner.layer.pipe(
  Layer.provideMerge(Layer.merge(NodeFileSystem.layer, NodePath.layer)),
)
describe('Bun shell runtime', () => {
  it.live(
    'runs actual native Bash, raw spill, Unicode, timeout and cancellation assertions in Bun',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const path = yield* Path.Path
          const root = yield* path.fromFileUrl(new URL('../../../', import.meta.url))
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
          const handle = yield* spawner.spawn(
            ChildProcess.make(
              'bun',
              [
                '--tsconfig-override',
                path.join(root, 'tsconfig.test.json'),
                path.join(root, 'packages/effect-harness/test/fixtures/ShellTools/verify.ts'),
              ],
              { cwd: root, forceKillAfter: '100 millis' },
            ),
          )
          const [exit, output, errors] = yield* Effect.all(
            [
              handle.exitCode,
              handle.stdout.pipe(Stream.decodeText, Stream.mkString),
              handle.stderr.pipe(Stream.decodeText, Stream.mkString),
            ],
            { concurrency: 'unbounded' },
          )
          assert.strictEqual(exit, 0, errors)
          assert.include(output, 'Bun shell conformance:')
        }),
      ).pipe(Effect.provide(native)),
    15000,
  )
})
