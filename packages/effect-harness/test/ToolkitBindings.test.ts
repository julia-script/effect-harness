import { assert, describe, it } from '@effect/vitest'
import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as Result from 'effect/Result'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as ToolkitFileSystem from '../src/ToolkitFileSystem.ts'
import * as ToolkitProcess from '../src/ToolkitProcess.ts'

const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const backend = NodeChildProcessSpawner.layer.pipe(Layer.provideMerge(files))
const bound = (options: ToolkitProcess.Options) =>
  Layer.merge(ToolkitFileSystem.layer(options), ToolkitProcess.layer(options)).pipe(
    Layer.provide(backend),
  )
const node = (
  script: string,
  args: ReadonlyArray<string> = [],
  options?: ChildProcess.CommandOptions,
) => ChildProcess.make(process.execPath, ['-e', script, '--', ...args], options)
const temp = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  return yield* fs.realPath(directory)
})
const failure = <A>(result: Result.Result<A, PlatformError.PlatformError>) => {
  assert.isTrue(Result.isFailure(result))
  if (Result.isFailure(result)) return result.failure
  assert.fail('expected a platform failure')
}

describe('Toolkit bound native services', () => {
  it.effect(
    'keeps identical tags isolated concurrently with coherent paths, cwd and literal argv',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const parent = yield* temp
          const hostile = '$(touch SHOULD_NOT_EXIST); "quoted" & * | > output'
          const probe = (root: string, value: string) =>
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const path = yield* Path.Path
              const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
              yield* fs.writeFileString('same.txt', value)
              const output = yield* spawner.string(
                node(
                  "const fs = require('node:fs'); process.stdout.write(fs.readFileSync('same.txt', 'utf8') + '\\n' + process.env.BINDING + '\\n' + process.argv.at(-1))",
                  [hostile],
                ),
              )
              assert.strictEqual(output, `${value}\n${value}\n${hostile}`)
              assert.strictEqual(path.resolve('same.txt'), path.join(root, 'same.txt'))
              assert.strictEqual(
                (yield* path.toFileUrl('same.txt')).pathname,
                path.join(root, 'same.txt'),
              )
              assert.isFalse(yield* fs.exists('SHOULD_NOT_EXIST'))
              return yield* fs.readFileString('same.txt')
            }).pipe(Effect.provide(bound({ root, env: { BINDING: value } })))
          const roots = [path.join(parent, 'a'), path.join(parent, 'b')]
          yield* Effect.forEach(roots, (root) => fs.makeDirectory(root))
          const values = yield* Effect.all([probe(roots[0]!, 'a'), probe(roots[1]!, 'b')], {
            concurrency: 'unbounded',
          })
          assert.deepStrictEqual(values, ['a', 'b'])
          assert.strictEqual(yield* fs.readFileString(path.join(parent, 'a', 'same.txt')), 'a')
          assert.strictEqual(yield* fs.readFileString(path.join(parent, 'b', 'same.txt')), 'b')
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'honors relative and absolute caller cwd, replaces default env and preserves pipelines',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const root = yield* temp
          const elsewhere = yield* temp
          yield* fs.writeFileString(path.join(elsewhere, 'absolute.txt'), 'absolute')
          yield* fs.makeDirectory(path.join(root, 'sub'))
          const options = { root, env: { BINDING: 'default', OMIT: 'default' } }
          const services = bound(options)
          // Constructor snapshots remain stable if application configuration changes.
          options.root = elsewhere
          options.env.BINDING = 'mutated'
          yield* Effect.gen(function* () {
            const path = yield* Path.Path
            const fs = yield* FileSystem.FileSystem
            const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
            assert.strictEqual(
              yield* fs.readFileString(path.join(elsewhere, 'absolute.txt')),
              'absolute',
            )
            const script =
              "process.stdout.write(process.cwd() + '\\n' + process.env.BINDING + '\\n' + String(process.env.OMIT))"
            const command = node(script, [], {
              cwd: 'sub',
              env: { BINDING: 'caller' },
              extendEnv: false,
            })
            const before = command.options.cwd
            assert.strictEqual(
              yield* spawner.string(command),
              `${path.join(root, 'sub')}\ncaller\nundefined`,
            )
            assert.strictEqual(command.options.cwd, before)
            assert.strictEqual(
              yield* spawner.string(node(script, [], { cwd: elsewhere })),
              `${elsewhere}\ndefault\ndefault`,
            )
            const pipeline = ChildProcess.pipeTo(
              node('process.stdout.write(process.env.BINDING)'),
              node(
                "process.stdin.on('data', data => process.stdout.write(data.toString().toUpperCase()))",
              ),
            )
            assert.strictEqual(yield* spawner.string(pipeline), 'DEFAULT')
          }).pipe(Effect.provide(services))
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect(
    'binds native streams, sinks, file handles, two-path operations and temporary resources',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const root = yield* temp
          yield* Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const path = yield* Path.Path
            yield* fs.makeDirectory('nested')
            yield* Stream.make(new TextEncoder().encode('hello')).pipe(
              Stream.run(fs.sink('nested/source.txt')),
            )
            yield* fs.copyFile('nested/source.txt', 'copy.txt')
            yield* fs.rename('copy.txt', 'renamed.txt')
            yield* fs.link('renamed.txt', 'hard.txt')
            yield* fs.symlink('renamed.txt', 'nested/symbolic.txt')
            assert.strictEqual(yield* fs.readFileString('nested/symbolic.txt'), 'hello')
            const file = yield* fs.open('hard.txt')
            const bytes = new Uint8Array(2)
            yield* file.seek(1n, 'start')
            assert.strictEqual(yield* file.read(bytes), 2)
            assert.strictEqual(new TextDecoder().decode(bytes), 'el')
            assert.strictEqual(
              yield* fs
                .stream('renamed.txt', { offset: 1, bytesToRead: 3 })
                .pipe(Stream.decodeText, Stream.mkString),
              'ell',
            )
            const temporary = yield* fs.makeTempFileScoped()
            assert.strictEqual(path.dirname(path.dirname(temporary)), root)
            assert.isTrue(yield* fs.exists(temporary))
            assert.deepStrictEqual((yield* fs.readDirectory('nested')).sort(), [
              'source.txt',
              'symbolic.txt',
            ])
            yield* fs.remove('hard.txt')
            assert.isFalse(yield* fs.exists('hard.txt'))
          }).pipe(Effect.provide(bound({ root })))
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect('propagates exact unsupported and permission failures without host fallback', () => {
    const unsupported = PlatformError.badArgument({
      module: 'Remote',
      method: 'readFileString',
      description: 'unsupported operation',
    })
    const denied = PlatformError.systemError({
      _tag: 'PermissionDenied',
      module: 'Remote',
      method: 'spawn',
    })
    const unsupportedSpawn = PlatformError.badArgument({
      module: 'Remote',
      method: 'spawn',
      description: 'unsupported operation',
    })
    const remote = Layer.mergeAll(
      Path.layer,
      FileSystem.layerNoop({
        readFileString: (p) => {
          assert.strictEqual(p, '/remote/read.txt')
          return Effect.fail(unsupported)
        },
        watch: () => Stream.fail(unsupported),
      }),
      Layer.succeed(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make((command) => {
          assert.strictEqual(command._tag, 'StandardCommand')
          if (command._tag === 'StandardCommand') assert.strictEqual(command.options.cwd, '/remote')
          return Effect.fail(
            command._tag === 'StandardCommand' && command.command === 'unsupported'
              ? unsupportedSpawn
              : denied,
          )
        }),
      ),
    )
    return Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
      assert.strictEqual(
        failure(yield* fs.readFileString('read.txt').pipe(Effect.result)),
        unsupported,
      )
      assert.strictEqual(
        failure(yield* fs.watch('.').pipe(Stream.runCollect, Effect.result)),
        unsupported,
      )
      assert.strictEqual(
        failure(yield* spawner.string(ChildProcess.make('remote-command')).pipe(Effect.result)),
        denied,
      )
      assert.strictEqual(
        failure(yield* spawner.string(ChildProcess.make('unsupported')).pipe(Effect.result)),
        unsupportedSpawn,
      )
    }).pipe(
      Effect.provide(
        Layer.merge(
          ToolkitFileSystem.layer({ root: '/remote' }),
          ToolkitProcess.layer({ root: '/remote' }),
        ).pipe(Layer.provide(remote)),
      ),
    )
  })

  it.effect('rejects lexical path and cwd escapes only when explicit policy is enabled', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const root = yield* temp
        yield* Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
          yield* fs.writeFileString('inside.txt', 'ok')
          assert.strictEqual(yield* fs.readFileString('nested/../inside.txt'), 'ok')
          for (const escaped of ['../outside.txt', `${root}-sibling/file.txt`]) {
            assert.strictEqual(
              failure(yield* fs.readFileString(escaped).pipe(Effect.result)).reason._tag,
              'PermissionDenied',
            )
            assert.strictEqual(
              failure(yield* fs.stream(escaped).pipe(Stream.runCollect, Effect.result)).reason._tag,
              'PermissionDenied',
            )
          }
          assert.strictEqual(
            failure(yield* fs.copyFile('inside.txt', '../copy.txt').pipe(Effect.result)).reason
              ._tag,
            'PermissionDenied',
          )
          assert.strictEqual(
            failure(yield* fs.glob('../*').pipe(Effect.result)).reason._tag,
            'PermissionDenied',
          )
          assert.strictEqual(
            failure(
              yield* Stream.make(new Uint8Array([1])).pipe(
                Stream.run(fs.sink('../sink.txt')),
                Effect.result,
              ),
            ).reason._tag,
            'PermissionDenied',
          )
          assert.strictEqual(
            failure(
              yield* spawner
                .string(node("process.stdout.write('unexpected')", [], { cwd: '..' }))
                .pipe(Effect.result),
            ).reason._tag,
            'PermissionDenied',
          )
        }).pipe(Effect.provide(bound({ root, pathPolicy: 'within-root' })))
      }),
    ).pipe(Effect.provide(files)),
  )

  it.effect('retains backend acquisition failures and rejects ambient relative roots', () => {
    const denied = PlatformError.systemError({
      _tag: 'PermissionDenied',
      module: 'Remote',
      method: 'connect',
    })
    const failedBackend = Layer.effect(FileSystem.FileSystem, Effect.fail(denied))
    const failedSpawner = ToolkitProcess.layer({ root: '/remote' }).pipe(
      Layer.provide([
        Path.layer,
        Layer.effect(ChildProcessSpawner.ChildProcessSpawner, Effect.fail(denied)),
      ]),
    )
    const failed = ToolkitFileSystem.layer({ root: '/remote' }).pipe(
      Layer.provide([failedBackend, Path.layer]),
    )
    return Effect.gen(function* () {
      const read = Effect.flatMap(FileSystem.FileSystem, (fs) => fs.readFileString('anything'))
      assert.strictEqual(failure(yield* read.pipe(Effect.provide(failed), Effect.result)), denied)
      assert.strictEqual(
        failure(
          yield* read.pipe(
            Effect.provide(
              ToolkitFileSystem.layer({ root: 'relative' }).pipe(Layer.provide(files)),
            ),
            Effect.result,
          ),
        ).reason._tag,
        'BadArgument',
      )
      const spawn = Effect.flatMap(ChildProcessSpawner.ChildProcessSpawner, (spawner) =>
        spawner.string(ChildProcess.make('unused')),
      )
      assert.strictEqual(
        failure(yield* spawn.pipe(Effect.provide(failedSpawner), Effect.result)),
        denied,
      )
      assert.strictEqual(
        failure(yield* spawn.pipe(Effect.provide(bound({ root: 'relative' })), Effect.result))
          .reason._tag,
        'BadArgument',
      )
    })
  })

  it.effect(
    'closing one invocation terminates only its process and preserves another binding',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const rootA = yield* temp
          const rootB = yield* temp
          const readyA = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>()
          const readyB = yield* Deferred.make<ChildProcessSpawner.ChildProcessHandle>()
          const invocation = (
            root: string,
            ready: Deferred.Deferred<ChildProcessSpawner.ChildProcessHandle>,
          ) =>
            Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* node('setInterval(() => {}, 1000)', [], {
                  forceKillAfter: '100 millis',
                })
                yield* Deferred.succeed(ready, handle)
                return yield* Effect.never
              }),
            ).pipe(Effect.provide(bound({ root })))
          const a = yield* invocation(rootA, readyA).pipe(Effect.forkChild)
          const b = yield* invocation(rootB, readyB).pipe(Effect.forkChild)
          const handleA = yield* Deferred.await(readyA)
          const handleB = yield* Deferred.await(readyB)
          yield* Fiber.interrupt(a)
          assert.isFalse(yield* handleA.isRunning)
          assert.isTrue(yield* handleB.isRunning)
          yield* Fiber.interrupt(b)
          assert.isFalse(yield* handleB.isRunning)
        }),
      ).pipe(Effect.provide(files)),
  )
})
