import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Fiber from 'effect/Fiber'
import * as Path from 'effect/Path'
import * as Ref from 'effect/Ref'
import * as Stream from 'effect/Stream'
import {
  Env,
  FileError,
  ExecutionError,
  NativeFiles,
  fromPlatform,
  type BinaryReader,
  ExecutionTimeout,
  FileInvalid,
  FileNotSupported,
} from '../../src/Env.ts'
import * as NodeEnv from '../../src/env/Node.ts'
import * as Watch from '../../src/env/Watch.ts'
import * as Mutation from '../../src/tools/Mutation.ts'
import * as Read from '../../src/tools/Read.ts'
import * as Write from '../../src/tools/Write.ts'
import * as Image from '../../src/tools/Image.ts'
import * as Bash from '../../src/tools/Bash.ts'
import { ToolCall } from '../../src/Invocation.ts'
import { withEnv, message, recording } from './Helpers.ts'

describe('boundary race and failure regressions', () => {
  it.effect(
    'temp prefixes/suffixes persist through cleanup and filesystem error meanings remain semantic',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const file = yield* env.createTempFile({ prefix: 'prefix-', suffix: '.data' })
          assert.strictEqual(env.path.basename(file).startsWith('prefix-'), true)
          assert.strictEqual(file.endsWith('.data'), true)
          yield* env.cleanup
          assert.strictEqual(yield* env.exists(file), true)
          yield* env.remove(env.path.dirname(file), { recursive: true })
          yield* env.createDir('dir')
          assert.strictEqual((yield* Effect.flip(env.readBinaryFile('dir'))).code, 'is_directory')
          yield* env.writeFile('file', 'x')
          assert.strictEqual(
            (yield* Effect.flip(env.readBinaryFile('file/child'))).code,
            'not_directory',
          )
        }),
      ),
  )
  it.live(
    'nonblocking regular-file validation refuses a FIFO and canceled open does not escape a live reader',
    () =>
      withEnv(
        Effect.scoped(
          Effect.gen(function* () {
            const env = yield* Env
            yield* env.exec(['/usr/bin/mkfifo', env.path.join(env.cwd, 'pipe')])
            assert.strictEqual((yield* Effect.flip(env.openBinaryReader('pipe'))).code, 'invalid')
            const fork = yield* env
              .openBinaryReader('absent')
              .pipe(Effect.andThen(Effect.never), Effect.forkChild)
            yield* Fiber.interrupt(fork)
            const exit = yield* Fiber.await(fork)
            assert.strictEqual(exit._tag, 'Failure')
          }),
        ),
      ),
  )
  it.effect(
    'read accepts growing logs, retries shrink once, and rejects a file changing in both attempts; closes resources',
    () =>
      withEnv(
        Effect.gen(function* () {
          const real = yield* Env
          const fs = yield* FileSystem.FileSystem
          // Simulate an external in-place writer; Env.writeFile now atomically replaces the inode.
          const overwrite = (path: string, content: string) =>
            fs
              .writeFileString(path, content)
              .pipe(Effect.mapError((error) => fromPlatform(error, path)))
          yield* real.writeFile('file', 'original')
          const calls = yield* Ref.make(0)
          const saved = yield* Ref.make<BinaryReader | undefined>(undefined)
          const shrinking: Env['Service'] = {
            ...real,
            openBinaryReader: (path, options) =>
              real.openBinaryReader(path, options).pipe(
                Effect.map((reader) => ({
                  ...reader,
                  info: Effect.gen(function* () {
                    const count = yield* Ref.updateAndGet(calls, (n) => n + 1)
                    if (count === 2) yield* overwrite(path, 'new')
                    return yield* reader.info
                  }),
                })),
                Effect.tap((reader) => Ref.set(saved, reader)),
              ),
          }
          assert.strictEqual(
            message(
              yield* Read.handler({ path: 'file' }).pipe(Effect.provideService(Env, shrinking)),
            ),
            'new',
          )
          assert.strictEqual(yield* Ref.get(calls), 4)
          const closed = yield* Ref.get(saved)
          assert.isDefined(closed)
          if (closed !== undefined)
            assert.strictEqual((yield* Effect.flip(closed.read(0, 1))).code, 'invalid')
          yield* real.writeFile('file', 'original')
          yield* Ref.set(calls, 0)
          const unstable: Env['Service'] = {
            ...shrinking,
            openBinaryReader: (path, options) =>
              real.openBinaryReader(path, options).pipe(
                Effect.map((reader) => ({
                  ...reader,
                  info: Effect.gen(function* () {
                    const count = yield* Ref.updateAndGet(calls, (n) => n + 1)
                    if (count === 2) yield* overwrite(path, 'four')
                    if (count === 4) yield* overwrite(path, 'x')
                    return yield* reader.info
                  }),
                })),
              ),
          }
          assert.match(
            (yield* Effect.flip(
              Read.handler({ path: 'file' }).pipe(Effect.provideService(Env, unstable)),
            )).message,
            /changed while/,
          )
          yield* real.writeFile('file', 'one')
          yield* Ref.set(calls, 0)
          const growing: Env['Service'] = {
            ...real,
            openBinaryReader: (path, options) =>
              real.openBinaryReader(path, options).pipe(
                Effect.map((reader) => ({
                  ...reader,
                  read: (offset, length) =>
                    reader
                      .read(offset, length)
                      .pipe(
                        Effect.tap(() =>
                          Ref.updateAndGet(calls, (n) => n + 1).pipe(
                            Effect.flatMap((n) =>
                              n === 1 ? real.appendFile(path, '\ntwo') : Effect.void,
                            ),
                          ),
                        ),
                      ),
                })),
              ),
          }
          assert.strictEqual(
            message(
              yield* Read.handler({ path: 'file' }).pipe(Effect.provideService(Env, growing)),
            ),
            'one\ntwo',
          )
        }),
      ),
  )
  it.effect(
    'missing files under canonical directory aliases share a mutex including POSIX backslash basenames',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          yield* env.createDir('real')
          yield* fs.symlink(env.path.join(env.cwd, 'real'), env.path.join(env.cwd, 'alias'))
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const secondRan = yield* Ref.make(false)
          const first = yield* Mutation.withFile(
            env.path.join(env.cwd, 'alias', 'back\\slash'),
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ).pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          const second = yield* Mutation.withFile(
            env.path.join(env.cwd, 'real', 'back\\slash'),
            Ref.set(secondRan, true),
          ).pipe(Effect.forkChild)
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(secondRan), false)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(first)
          yield* Fiber.join(second)
          assert.strictEqual(yield* Ref.get(secondRan), true)
        }),
      ),
  )
  it.effect('different namespaces are independent and a failed mutation releases its permit', () =>
    withEnv(
      Effect.gen(function* () {
        const real = yield* Env
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const first = yield* Mutation.withFile(
          real.path.join(real.cwd, 'file'),
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ).pipe(Effect.forkChild)
        yield* Deferred.await(entered)
        const changed: Env['Service'] = { ...real, id: 'other-namespace' }
        yield* Write.handler({ path: 'file', content: 'independent' }).pipe(
          Effect.provideService(Env, changed),
        )
        assert.strictEqual(yield* real.readTextFile('file'), 'independent')
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(first)
        yield* Mutation.withFile(
          real.path.join(real.cwd, 'file'),
          Effect.fail(new FileError({ reason: new FileInvalid({ message: 'failed' }) })),
        ).pipe(Effect.exit)
        yield* Write.handler({ path: 'file', content: 'next' })
        assert.strictEqual(yield* real.readTextFile('file'), 'next')
      }),
    ),
  )
  it.effect(
    'late APNG chunk walk is bounded and recognizes BMP/JPEG/GIF/WEBP signatures accurately',
    () => {
      const bytes = new Uint8Array(70060)
      bytes.set([137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82])
      new DataView(bytes.buffer).setUint32(33, 70000)
      bytes.set(new TextEncoder().encode('junk'), 37)
      bytes.set(new TextEncoder().encode('acTL'), 70049)
      const reads: number[] = []
      return Image.detectSupportedImageMimeTypeOf({
        size: bytes.length,
        read: (offset, length) =>
          Effect.sync(() => {
            reads.push(length)
            return bytes.subarray(offset, offset + length)
          }),
      }).pipe(
        Effect.tap((result) =>
          Effect.sync(() => {
            assert.strictEqual(result, undefined)
            assert.strictEqual(Math.max(...reads), 65536)
          }),
        ),
      )
    },
  )
  it.live(
    'watch unavailable native coverage falls back with overflow; growing budget emits one terminal error and ends',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const path = yield* Path.Path
          const native = yield* NativeFiles.pipe(Effect.provide(NodeEnv.nativeLayer))
          const unavailable: NativeFiles['Service'] = {
            ...native,
            watchDirectory: () =>
              Effect.fail(
                new FileError({ reason: new FileNotSupported({ message: 'No native watcher' }) }),
              ),
          }
          const watcher = yield* Watch.make(fs, path, unavailable, [{ path: env.cwd }], {
            pollIntervalMs: 10,
          })
          assert.strictEqual(watcher.mode, 'polling')
          const overflow = yield* watcher.changes.pipe(Stream.take(1), Stream.runCollect)
          assert.deepStrictEqual(overflow, [{ overflow: true }])
          yield* watcher.close
          const limited = yield* env.watch([{ path: '.', recursive: true }], {
            mode: 'polling',
            pollIntervalMs: 10,
            directoryBudget: 2,
          })
          const result = yield* limited.changes.pipe(Stream.runCollect, Effect.forkChild)
          yield* env.writeFile('deep/a/file', 'new')
          const events = yield* Fiber.join(result).pipe(Effect.timeout(2000))
          assert.strictEqual(events.length, 1)
          assert.strictEqual(events[0] !== undefined && 'error' in events[0], true)
          yield* limited.close
        }),
      ),
  )
  it.effect(
    'PowerShell empty programs and nonzero started candidate stop fallback, shell diagnostics survive typed failure',
    () =>
      withEnv(
        Effect.gen(function* () {
          const real = yield* Env
          const captured = yield* recording
          const count = yield* Ref.make(0)
          const env: Env['Service'] = {
            ...real,
            exec: () => Ref.update(count, (n) => n + 1).pipe(Effect.as({ exitCode: 7 })),
          }
          assert.match(
            (yield* Effect.flip(
              Bash.powerShellHandler({ programs: [] })({ command: 'x' }).pipe(
                Effect.provideService(ToolCall, captured.api),
              ),
            )).message,
            /No command/,
          )
          assert.match(
            (yield* Effect.flip(
              Bash.powerShellHandler({ programs: ['started', 'next'] })({ command: 'x' }).pipe(
                Effect.provideService(Env, env),
                Effect.provideService(ToolCall, captured.api),
              ),
            )).message,
            /code 7/,
          )
          assert.strictEqual(yield* Ref.get(count), 1)
        }),
      ),
  )
  it.effect(
    'remote result-only spill paths are diagnosed before failure and early publications are deduplicated',
    () =>
      withEnv(
        Effect.gen(function* () {
          const real = yield* Env
          for (const early of [false, true]) {
            const captured = yield* recording
            const env: Env['Service'] = {
              ...real,
              exec: (_command, options) =>
                Effect.gen(function* () {
                  if (options?.onOutput !== undefined)
                    yield* options.onOutput('partial', { stream: 'stdout' })
                  if (early && options?.onSpill !== undefined)
                    yield* options.onSpill('/remote/output.log')
                  return yield* new ExecutionError({
                    reason: new ExecutionTimeout({
                      message: 'remote timeout',
                      spillPath: '/remote/output.log',
                    }),
                  })
                }),
            }
            assert.match(
              (yield* Effect.flip(
                Bash.handler()({ command: 'remote' }).pipe(
                  Effect.provideService(Env, env),
                  Effect.provideService(ToolCall, captured.api),
                ),
              )).message,
              /remote timeout/,
            )
            assert.strictEqual(yield* Ref.get(captured.output), 'partial')
            assert.deepStrictEqual(
              (yield* Ref.get(captured.diagnostics)).map((item) => item.kind),
              ['full_output'],
            )
          }
        }),
      ),
  )
})
