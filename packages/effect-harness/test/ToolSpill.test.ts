import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as Result from 'effect/Result'
import * as ToolSpill from '../src/ToolSpill.ts'

const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
describe('raw output spill ownership', () => {
  it.effect(
    'persistent reference outlives call and toolkit scopes and revalidates after reopen',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const root = yield* fs.makeTempDirectoryScoped()
          const options: ToolSpill.Options = {
            provider: 'test',
            directory: path.resolve(root),
            lifetime: 'persistent',
          }
          const chunks = [
            new Uint8Array([0xf0, 0x9f]),
            new Uint8Array([0x98, 0x80]),
            new Uint8Array([0xff, 10]),
          ]
          const reference = yield* Effect.scoped(
            Effect.gen(function* () {
              const spill = yield* ToolSpill.ToolSpill
              return yield* Effect.scoped(
                Effect.gen(function* () {
                  const writer = yield* spill.open
                  yield* writer.append('stdout', chunks[0] ?? new Uint8Array())
                  yield* writer.append('stderr', chunks[2] ?? new Uint8Array())
                  yield* writer.append('stdout', chunks[1] ?? new Uint8Array())
                  return yield* writer.commit
                }),
              )
            }).pipe(Effect.provide(ToolSpill.layer(options))),
          )
          assert.deepStrictEqual(
            yield* fs.readFile(reference.stdoutPath),
            new Uint8Array([0xf0, 0x9f, 0x98, 0x80]),
          )
          assert.deepStrictEqual(
            yield* fs.readFile(reference.stderrPath),
            new Uint8Array([0xff, 10]),
          )
          assert.deepStrictEqual(
            yield* fs.readFile(reference.path),
            new Uint8Array([0xf0, 0x9f, 0xff, 10, 0x98, 0x80]),
          )
          yield* Effect.gen(function* () {
            const spill = yield* ToolSpill.ToolSpill
            assert.deepStrictEqual(yield* spill.revalidate(reference), reference)
            assert.isTrue(
              Result.isFailure(
                yield* spill.revalidate({ ...reference, provider: 'another' }).pipe(Effect.result),
              ),
            )
            yield* fs.writeFileString(reference.stdoutPath, 'changed')
            assert.isTrue(Result.isFailure(yield* spill.revalidate(reference).pipe(Effect.result)))
            yield* fs.writeFile(reference.stdoutPath, new Uint8Array([0xf0, 0x9f, 0x98, 0x80]))
            yield* spill.remove(reference)
            assert.isTrue(Result.isFailure(yield* spill.revalidate(reference).pipe(Effect.result)))
          }).pipe(Effect.provide(ToolSpill.layer(options)))
        }),
      ).pipe(Effect.provide(files)),
  )
  it.effect(
    'aborted writers remove all partial files; toolkit lifetime expires only on Layer close',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = path.resolve(yield* fs.makeTempDirectoryScoped())
          const ref = yield* Effect.scoped(
            Effect.gen(function* () {
              const spill = yield* ToolSpill.ToolSpill
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const writer = yield* spill.open
                  yield* writer.append('stdout', new TextEncoder().encode('partial'))
                }),
              )
              assert.deepStrictEqual(yield* fs.readDirectory(directory), [])
              const ref = yield* Effect.scoped(
                Effect.flatMap(spill.open, (writer) => writer.commit),
              )
              assert.isTrue(yield* fs.exists(ref.path))
              assert.deepStrictEqual(yield* spill.revalidate(ref), ref)
              return ref
            }).pipe(
              Effect.provide(
                ToolSpill.layer({ provider: 'scope', directory, lifetime: 'toolkit' }),
              ),
            ),
          )
          assert.isFalse(yield* fs.exists(ref.path))
          assert.deepStrictEqual(yield* fs.readDirectory(directory), [])
        }),
      ).pipe(Effect.provide(files)),
  )

  it.effect('failed partial writes refuse publication and remove only provider-owned files', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const directory = path.resolve(yield* fs.makeTempDirectoryScoped())
        yield* fs.writeFileString(path.join(directory, 'user-file'), 'keep')
        const denied = PlatformError.systemError({
          _tag: 'PermissionDenied',
          module: 'test',
          method: 'writeAll',
        })
        const broken: FileSystem.FileSystem = {
          ...fs,
          open: (p, o) =>
            fs.open(p, o).pipe(
              Effect.map((file) => ({
                ...file,
                writeAll: (bytes) =>
                  file.writeAll(bytes.subarray(0, 1)).pipe(Effect.andThen(Effect.fail(denied))),
              })),
            ),
        }
        yield* Effect.gen(function* () {
          const spill = yield* ToolSpill.ToolSpill
          yield* Effect.scoped(
            Effect.gen(function* () {
              const writer = yield* spill.open
              const failed = yield* writer
                .append('stdout', new TextEncoder().encode('partial'))
                .pipe(Effect.result)
              assert.isTrue(Result.isFailure(failed))
              if (Result.isFailure(failed)) assert.strictEqual(failed.failure.reason, 'write')
              const publication = yield* writer.commit.pipe(Effect.result)
              assert.isTrue(Result.isFailure(publication))
              if (Result.isFailure(publication))
                assert.strictEqual(publication.failure.reason, 'write')
            }),
          )
        }).pipe(
          Effect.provide(
            ToolSpill.layer({ provider: 'broken', directory, lifetime: 'persistent' }).pipe(
              Layer.provide(Layer.succeed(FileSystem.FileSystem, broken)),
            ),
          ),
        )
        assert.deepStrictEqual(yield* fs.readDirectory(directory), ['user-file'])
        assert.strictEqual(yield* fs.readFileString(path.join(directory, 'user-file')), 'keep')
      }),
    ).pipe(Effect.provide(files)),
  )
})
