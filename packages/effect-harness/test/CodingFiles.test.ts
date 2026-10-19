import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as ByteSize from 'effect/ByteSize'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as PlatformError from 'effect/PlatformError'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as FileReader from '../src/FileReader.ts'
import * as Record from 'effect-harness/Record'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import type * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as FileError from '../src/coding/FileError.ts'
import * as MutationCoordinator from '../src/coding/MutationCoordinator.ts'
import * as Read from '../src/coding/Read.ts'
import * as Write from '../src/coding/Write.ts'

const native = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const tools = Toolkit.merge(Read.toolkit, Write.toolkit)
const binding = (root: string, backend = native) =>
  ToolkitFileSystem.layer({ root }).pipe(Layer.provide(backend))
const handlers = (root: string, options: Read.Options = {}, backend = native) =>
  Layer.merge(Read.layer(options), Write.layer).pipe(
    Layer.provideMerge(
      Layer.merge(FileReader.layer, MutationCoordinator.layer).pipe(
        Layer.provideMerge(binding(root, backend)),
      ),
    ),
  )
// No handler uses execution access. Fail immediately if it accidentally starts doing so.
const execution = Layer.succeed(ToolExecution, {
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  taskId: Schema.decodeSync(Record.TaskId)(1),
  callId: 'coding-test',
  snapshot: () => Effect.die('Unexpected snapshot'),
  watch: () => Stream.die('Unexpected watch'),
  commit: () => Effect.die('Unexpected commit'),
  output: () => Effect.die('Unexpected output'),
  details: () => Effect.die('Unexpected details'),
  diagnostic: () => Effect.die('Unexpected diagnostic'),
})
const invoke = (name: 'read' | 'write', args: Schema.JsonObject) =>
  Effect.flatMap(tools, (ready) => ready.invoke({ id: 'call', name, arguments: args })).pipe(
    Effect.provide(execution),
  )
const outcome = (result: ToolResult.Result) =>
  Schema.decodeUnknownEffect(Read.Outcome)(result.structuredOutput)
const writeOutcome = (result: ToolResult.Result) =>
  Schema.decodeUnknownEffect(Write.Outcome)(result.structuredOutput)
const failure = (result: ToolResult.Result) => {
  assert.isTrue(result.isError)
  assert.strictEqual(result.content[0]?.type, 'text')
  assert.isNotEmpty(result.diagnostics)
  return Schema.decodeUnknownEffect(FileError.FailureSchema)(result.structuredOutput)
}
const text = (result: ToolResult.Result) => {
  const part = result.content[0]
  assert.strictEqual(part?.type, 'text')
  return part?.type === 'text' ? part.text : ''
}
const root = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
})
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(native))

describe('ready-made coding file tools', () => {
  it.effect(
    'interrupts a queued public write and allows its successor after a backend failure',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* root
          yield* fs.writeFileString(path.join(directory, 'target'), 'original')
          yield* fs.symlink(path.join(directory, 'target'), path.join(directory, 'alias'))
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const waiterReady = yield* Deferred.make<void>()
          let attempts = 0
          const observed = Layer.effect(
            FileSystem.FileSystem,
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              return {
                ...fs,
                realPath: (name) =>
                  fs
                    .realPath(name)
                    .pipe(
                      Effect.tap(() =>
                        name.endsWith('/alias')
                          ? Deferred.succeed(waiterReady, undefined)
                          : Effect.void,
                      ),
                    ),
                writeFile: (name, bytes, options) =>
                  Effect.gen(function* () {
                    attempts++
                    if (attempts === 1) {
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(release)
                      return yield* PlatformError.systemError({
                        _tag: 'PermissionDenied',
                        module: 'CodingFilesTest',
                        method: 'writeFile',
                        description: 'backend rejected this write',
                      })
                    }
                    return yield* fs.writeFile(name, bytes, options)
                  }),
              } satisfies FileSystem.FileSystem
            }),
          ).pipe(Layer.provideMerge(native))
          yield* Effect.gen(function* () {
            const first = yield* invoke('write', { path: 'target', content: 'fail' }).pipe(
              Effect.forkChild,
            )
            yield* Deferred.await(entered)
            const waiting = yield* invoke('write', { path: 'alias', content: 'interrupted' }).pipe(
              Effect.forkChild,
            )
            yield* Deferred.await(waiterReady)
            yield* Effect.yieldNow
            yield* Fiber.interrupt(waiting)
            assert.strictEqual(attempts, 1)
            const next = yield* invoke('write', { path: './target', content: 'successor' }).pipe(
              Effect.forkChild,
            )
            yield* Deferred.succeed(release, undefined)
            assert.strictEqual((yield* failure(yield* Fiber.join(first))).reason, 'filesystem')
            assert.isFalse((yield* Fiber.join(next)).isError)
            assert.strictEqual(attempts, 2)
          }).pipe(Effect.provide(handlers(directory, {}, observed)))
          assert.strictEqual(yield* fs.readFileString(path.join(directory, 'target')), 'successor')
        }),
      ),
  )

  it.effect(
    'isolates two bound roots and returns created/overwritten outcomes for literal content',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const a = yield* root
          const b = yield* root
          const probe = (directory: string, content: string) =>
            Effect.gen(function* () {
              const created = yield* writeOutcome(
                yield* invoke('write', { path: '@nested/../nested/message.txt', content }),
              )
              assert.strictEqual(created.outcome, 'created')
              assert.strictEqual(created.bytesWritten, new TextEncoder().encode(content).length)
              assert.strictEqual(
                text(yield* invoke('read', { path: 'nested/message.txt' })),
                content,
              )
              const changed = yield* writeOutcome(
                yield* invoke('write', { path: 'nested/message.txt', content: `${content} again` }),
              )
              assert.strictEqual(changed.outcome, 'overwritten')
              return changed.path
            }).pipe(Effect.provide(handlers(directory)))
          yield* Effect.all([probe(a, 'A $(touch nope) "literal" 😀'), probe(b, 'B')], {
            concurrency: 'unbounded',
          })
          assert.strictEqual(
            yield* fs.readFileString(path.join(a, 'nested/message.txt')),
            'A $(touch nope) "literal" 😀 again',
          )
          assert.strictEqual(
            yield* fs.readFileString(path.join(b, 'nested/message.txt')),
            'B again',
          )
          assert.isFalse(yield* fs.exists(path.join(a, 'nope')))
          assert.strictEqual(Tool.policy(Read.tool).replay, 'safe')
          assert.strictEqual(Tool.policy(Write.tool).replay, 'unsafe')
        }),
      ),
  )

  it.effect('selects one-based lines with BOM, CRLF, trailing newline and empty files', () =>
    run(
      Effect.gen(function* () {
        const directory = yield* root
        yield* Effect.gen(function* () {
          yield* invoke('write', { path: 'text.txt', content: '\ufeffone\r\ntwo\r\n' })
          const full = yield* invoke('read', { path: 'text.txt' })
          assert.strictEqual(text(full), 'one\r\ntwo\r\n')
          assert.strictEqual((yield* outcome(full)).totalLines, 3)
          assert.strictEqual(
            text(yield* invoke('read', { path: 'text.txt', offset: 2, limit: 1 })),
            'two\r',
          )
          assert.strictEqual(text(yield* invoke('read', { path: 'text.txt', offset: 3 })), '')
          assert.strictEqual(
            (yield* failure(yield* invoke('read', { path: 'text.txt', offset: 4 }))).reason,
            'beyondEOF',
          )
          yield* invoke('write', { path: 'empty.txt', content: '' })
          const empty = yield* invoke('read', { path: 'empty.txt' })
          assert.strictEqual(text(empty), '')
          assert.strictEqual((yield* outcome(empty)).totalLines, 1)
          const limited = yield* invoke('read', { path: 'text.txt', limit: 1 })
          assert.deepStrictEqual((yield* outcome(limited)).continuation, { offset: 2 })
          assert.include(
            limited.content.map((p) => (p.type === 'text' ? p.text : '')).join(' '),
            'offset=2',
          )
        }).pipe(Effect.provide(handlers(directory)))
      }),
    ),
  )

  it.effect(
    'caps complete lines and huge first lines on UTF-8 boundaries with usable continuation',
    () =>
      run(
        Effect.gen(function* () {
          const directory = yield* root
          yield* Effect.gen(function* () {
            yield* invoke('write', { path: 'huge.txt', content: '😀'.repeat(200000) + '\nend' })
            const first = yield* invoke('read', { path: 'huge.txt' })
            assert.strictEqual(text(first), '😀'.repeat(2))
            assert.strictEqual((yield* outcome(first)).outputBytes, 8)
            const continuation = (yield* outcome(first)).continuation!
            assert.deepStrictEqual(continuation, { offset: 1, byteOffset: '8' })
            const second = yield* invoke('read', { path: 'huge.txt', ...continuation })
            assert.strictEqual(text(second), '😀'.repeat(2))
            assert.strictEqual((yield* outcome(second)).continuation?.byteOffset, '16')
            yield* invoke('write', { path: 'lines.txt', content: 'one\ntwo\nthree\nfour' })
            const lines = yield* invoke('read', { path: 'lines.txt' })
            assert.strictEqual(text(lines), 'one\ntwo')
            assert.deepStrictEqual((yield* outcome(lines)).continuation, { offset: 3 })
          }).pipe(Effect.provide(handlers(directory, { maxBytes: 10, maxLines: 2 })))
          yield* Effect.gen(function* () {
            const lines = yield* invoke('read', { path: 'lines.txt' })
            assert.strictEqual(text(lines), 'one')
            assert.deepStrictEqual((yield* outcome(lines)).continuation, { offset: 2 })
          }).pipe(Effect.provide(handlers(directory, { maxBytes: 6 })))
        }),
      ),
  )

  it.effect(
    'normalizes namespace and filename variants and rejects bad paths, encoding and caps',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* root
          yield* fs.writeFile(path.join(directory, 'bad.txt'), new Uint8Array([0xc3, 0x28]))
          yield* fs.writeFileString(path.join(directory, 'Capture\u202fPM.png.txt'), 'variant')
          yield* fs.writeFileString(path.join(directory, 'cafe\u0301.txt'), 'nfd')
          yield* Effect.gen(function* () {
            assert.strictEqual(text(yield* invoke('read', { path: '@café.txt' })), 'nfd')
            assert.strictEqual(
              text(yield* invoke('read', { path: 'Capture PM.png.txt' })),
              'variant',
            )
            for (const name of ['read', 'write'] as const)
              for (const invalid of ['', '@', 'a\0b']) {
                assert.strictEqual(
                  (yield* failure(yield* invoke(name, { path: invalid, content: '' }))).reason,
                  'invalidPath',
                )
              }
            assert.strictEqual(
              (yield* failure(yield* invoke('read', { path: 'bad.txt' }))).reason,
              'encoding',
            )
            assert.strictEqual(
              (yield* failure(yield* invoke('write', { path: 'bad-write.txt', content: '\ud800' })))
                .reason,
              'encoding',
            )
            for (const value of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])
              for (const field of ['offset', 'limit']) {
                assert.strictEqual(
                  (yield* failure(yield* invoke('read', { path: 'bad.txt', [field]: value })))
                    .reason,
                  'invalidLimits',
                )
              }
            assert.strictEqual(
              (yield* failure(yield* invoke('read', { path: 'missing' }))).reason,
              'filesystem',
            )
            assert.strictEqual(
              (yield* failure(yield* invoke('read', { path: '.' }))).reason,
              'filesystem',
            )
            assert.strictEqual(
              (yield* failure(yield* invoke('read', { path: 'café.txt', byteOffset: '999' })))
                .reason,
              'invalidLimits',
            )
          }).pipe(Effect.provide(handlers(directory)))
          for (const options of [
            { maxBytes: 0 },
            { maxBytes: Read.maxBytes + 1 },
            { maxLines: 0 },
            { maxLines: Read.maxLines + 1 },
          ]) {
            assert.strictEqual(
              (yield* failure(
                yield* invoke('read', { path: 'bad.txt' }).pipe(
                  Effect.provide(handlers(directory, options)),
                ),
              )).reason,
              'invalidLimits',
            )
          }
        }),
      ),
  )

  it.effect(
    'serializes symlink and lexical aliases through public write handlers while other paths proceed',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* root
          yield* fs.writeFileString(path.join(directory, 'target.txt'), 'old')
          yield* fs.symlink(path.join(directory, 'target.txt'), path.join(directory, 'alias.txt'))
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const canonicalized = yield* Deferred.make<void>()
          let active = 0
          let maximum = 0
          let writes = 0
          const observed = Layer.effect(
            FileSystem.FileSystem,
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              return {
                ...fs,
                realPath: (name) =>
                  fs
                    .realPath(name)
                    .pipe(
                      Effect.tap(() =>
                        name.endsWith('alias.txt')
                          ? Deferred.succeed(canonicalized, undefined)
                          : Effect.void,
                      ),
                    ),
                writeFile: (name, bytes, options) =>
                  Effect.gen(function* () {
                    if (!name.endsWith('target.txt'))
                      return yield* fs.writeFile(name, bytes, options)
                    active++
                    maximum = Math.max(maximum, active)
                    writes++
                    if (writes === 1) {
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(release)
                    }
                    yield* fs.writeFile(name, bytes, options)
                  }).pipe(
                    Effect.ensuring(
                      Effect.sync(() => {
                        if (name.endsWith('target.txt')) active--
                      }),
                    ),
                  ),
              } satisfies FileSystem.FileSystem
            }),
          ).pipe(Layer.provideMerge(native))
          yield* Effect.gen(function* () {
            const first = yield* invoke('write', { path: './target.txt', content: 'first' }).pipe(
              Effect.forkChild,
            )
            yield* Deferred.await(entered)
            const second = yield* invoke('write', { path: 'alias.txt', content: 'second' }).pipe(
              Effect.forkChild,
            )
            yield* Deferred.await(canonicalized)
            const unrelated = yield* invoke('write', { path: 'other.txt', content: 'independent' })
            assert.isFalse(unrelated.isError)
            assert.strictEqual(writes, 1)
            yield* Deferred.succeed(release, undefined)
            assert.isFalse((yield* Fiber.join(first)).isError)
            assert.isFalse((yield* Fiber.join(second)).isError)
            assert.strictEqual(maximum, 1)
          }).pipe(Effect.provide(handlers(directory, {}, observed)))
          assert.strictEqual(yield* fs.readFileString(path.join(directory, 'target.txt')), 'second')
        }),
      ),
  )

  it.effect(
    'releases permits on failure, interruption and interrupted waits and closes with its scope',
    () =>
      run(
        Effect.gen(function* () {
          const directory = yield* root
          const owner = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
          const context = yield* Layer.buildWithScope(
            MutationCoordinator.layer.pipe(Layer.provide(binding(directory))),
            owner,
          )
          const coordinator = Context.get(context, MutationCoordinator.MutationCoordinator)
          const ready = yield* Deferred.make<void>()
          const first = yield* coordinator
            .withPath('missing/nested.txt', () =>
              Deferred.succeed(ready, undefined).pipe(Effect.andThen(Effect.never)),
            )
            .pipe(Effect.forkChild)
          yield* Deferred.await(ready)
          let waiterRan = false
          const waiter = yield* coordinator
            .withPath('missing/../missing/nested.txt', () =>
              Effect.sync(() => {
                waiterRan = true
              }),
            )
            .pipe(Effect.forkChild)
          yield* Effect.yieldNow
          yield* Fiber.interrupt(waiter)
          yield* Fiber.interrupt(first)
          assert.isFalse(waiterRan)
          const failed = yield* coordinator
            .withPath('missing/nested.txt', () => Effect.fail('expected'))
            .pipe(Effect.result)
          assert.isTrue(Result.isFailure(failed))
          assert.strictEqual(
            yield* coordinator.withPath('missing/nested.txt', () => Effect.succeed('released')),
            'released',
          )
          yield* Scope.close(owner, Exit.void)
          const closed = yield* coordinator
            .withPath('missing/nested.txt', () => Effect.void)
            .pipe(Effect.result)
          assert.isTrue(Result.isFailure(closed))
          if (Result.isFailure(closed)) assert.strictEqual(closed.failure.reason, 'closed')
        }),
      ),
  )
})

describe('native scoped positional reader', () => {
  it.effect(
    'bounds actual native read buffers and closes public read handlers on interruption and failure',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* root
          yield* fs.writeFileString(path.join(directory, 'huge'), 'x'.repeat(400000))
          const entered = yield* Deferred.make<void>()
          let opens = 0
          let closes = 0
          let largestBuffer = 0
          let pause = true
          const observed = Layer.effect(
            FileSystem.FileSystem,
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              return {
                ...fs,
                open: (name, options) =>
                  Effect.gen(function* () {
                    const file = yield* fs.open(name, options)
                    opens++
                    yield* Effect.addFinalizer(() =>
                      Effect.sync(() => {
                        closes++
                      }),
                    )
                    return {
                      ...file,
                      stat: file.stat,
                      seek: (offset, from) => file.seek(offset, from),
                      sync: file.sync,
                      truncate: (length) => file.truncate(length),
                      write: (buffer) => file.write(buffer),
                      writeAll: (buffer) => file.writeAll(buffer),
                      readAlloc: (size) => file.readAlloc(size),
                      read: (buffer) =>
                        Effect.gen(function* () {
                          largestBuffer = Math.max(largestBuffer, buffer.length)
                          if (pause) {
                            yield* Deferred.succeed(entered, undefined)
                            return yield* Effect.never
                          }
                          return yield* file.read(buffer)
                        }),
                    } satisfies FileSystem.File
                  }),
              } satisfies FileSystem.FileSystem
            }),
          ).pipe(Layer.provideMerge(native))
          yield* Effect.gen(function* () {
            const reading = yield* invoke('read', { path: 'huge' }).pipe(Effect.forkChild)
            yield* Deferred.await(entered)
            yield* Fiber.interrupt(reading)
            assert.strictEqual(opens, 1)
            assert.strictEqual(closes, 1)
            pause = false
            const result = yield* invoke('read', { path: 'huge' })
            assert.strictEqual((yield* outcome(result)).outputBytes, Read.maxBytes)
            assert.isAtMost(largestBuffer, 64 * 1024)
            const directoryResult = yield* invoke('read', { path: '.' })
            assert.isTrue(directoryResult.isError)
            assert.strictEqual(opens, closes)
          }).pipe(Effect.provide(handlers(directory, {}, observed)))
        }),
      ),
  )

  it.effect(
    'retries one shrinking scan on the same opened identity and reports repeated change',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* root
          const filePath = path.join(directory, 'changing')
          yield* fs.writeFileString(filePath, 'long original content')
          let changes = 1
          const observed = Layer.effect(
            FileSystem.FileSystem,
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              return {
                ...fs,
                open: (name, options) =>
                  Effect.gen(function* () {
                    const file = yield* fs.open(name, options)
                    let armed = true
                    return {
                      ...file,
                      stat: file.stat.pipe(
                        Effect.tap(() =>
                          Effect.sync(() => {
                            armed = true
                          }),
                        ),
                      ),
                      seek: (offset, from) => file.seek(offset, from),
                      sync: file.sync,
                      truncate: (length) => file.truncate(length),
                      write: (buffer) => file.write(buffer),
                      writeAll: (buffer) => file.writeAll(buffer),
                      readAlloc: (size) => file.readAlloc(size),
                      read: (buffer) =>
                        Effect.gen(function* () {
                          if (changes > 0 && armed) {
                            armed = false
                            changes--
                            yield* fs.writeFileString(
                              name,
                              changes === 0 ? 'short' : 'shorter text',
                            )
                          }
                          return yield* file.read(buffer)
                        }),
                    } satisfies FileSystem.File
                  }),
              } satisfies FileSystem.FileSystem
            }),
          ).pipe(Layer.provideMerge(native))
          yield* Effect.gen(function* () {
            assert.strictEqual(text(yield* invoke('read', { path: 'changing' })), 'short')
            yield* fs.writeFileString(filePath, 'a much longer original content')
            changes = 2
            const changed = yield* failure(yield* invoke('read', { path: 'changing' }))
            assert.strictEqual(changed.reason, 'changed')
          }).pipe(Effect.provide(handlers(directory, {}, observed)))
        }),
      ),
  )

  it.effect(
    'keeps opened identity through rename, serializes positions, rejects guards and use after close',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const directory = yield* root
          yield* fs.writeFileString(path.join(directory, 'file'), 'abcdef')
          const owner = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(owner, Exit.void))
          const service = yield* FileReader.FileReader.pipe(Effect.provide(FileReader.layer))
          const reader = yield* service
            .open(path.join(directory, 'file'))
            .pipe(Effect.provideService(Scope.Scope, owner))
          yield* fs.rename(path.join(directory, 'file'), path.join(directory, 'renamed'))
          yield* fs.writeFileString(path.join(directory, 'file'), 'replacement')
          const chunks = yield* Effect.all(
            [reader.readAt(0n, 2), reader.readAt(4n, 2), reader.readAt(1n, 3)],
            { concurrency: 'unbounded' },
          )
          assert.deepStrictEqual(
            chunks.map((b) => new TextDecoder().decode(b)),
            ['ab', 'ef', 'bcd'],
          )
          assert.strictEqual(ByteSize.toBigInt((yield* reader.stat).size), 6n)
          assert.strictEqual((yield* reader.readAt(6n, 2)).length, 0)
          for (const [offset, length] of [
            [-1n, 1],
            [0n, -1],
            [0n, FileReader.maxReadBytes + 1],
          ] as const) {
            const result = yield* reader.readAt(offset, length).pipe(Effect.result)
            assert.isTrue(Result.isFailure(result))
            if (Result.isFailure(result)) assert.strictEqual(result.failure.reason, 'invalid')
          }
          const noFollow = yield* Effect.scoped(
            service.open(path.join(directory, 'file'), { noFollow: true }),
          ).pipe(Effect.result)
          assert.isTrue(Result.isFailure(noFollow))
          if (Result.isFailure(noFollow)) assert.strictEqual(noFollow.failure.reason, 'unsupported')
          yield* Scope.close(owner, Exit.void)
          for (const operation of [
            Effect.asVoid(reader.stat),
            Effect.asVoid(reader.readAt(0n, 1)),
          ]) {
            const result = yield* operation.pipe(Effect.result)
            assert.isTrue(Result.isFailure(result))
            if (Result.isFailure(result)) assert.strictEqual(result.failure.reason, 'closed')
          }
        }),
      ),
  )
})
