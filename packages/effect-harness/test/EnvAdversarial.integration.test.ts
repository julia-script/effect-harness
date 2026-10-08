import { assertNone } from '@effect/vitest/utils'

import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'

import { makeBinaryReader } from 'effect-harness/NativeFiles'

import { assert, describe, it } from '@effect/vitest'

import * as Cause from 'effect/Cause'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Scope from 'effect/Scope'

import * as Exit from 'effect/Exit'

import * as FileSystem from 'effect/FileSystem'

import * as Fiber from 'effect/Fiber'

import * as Path from 'effect/Path'

import * as Ref from 'effect/Ref'

import * as Stream from 'effect/Stream'

import { Env, fromPlatform } from 'effect-harness/Env'
import { FileError, FileInvalidError, FileNotSupportedError } from 'effect-harness/FileError'
import { ExecutionError, ExecutionTimeoutError } from 'effect-harness/ExecutionError'
import { NativeFiles, type BinaryReader } from 'effect-harness/NativeFiles'

import * as NodeEnv from 'effect-harness/NodeEnv'

import * as Layer from 'effect/Layer'

import * as Context from 'effect/Context'

// effect-nit-allow P8-tests-import-public-specifiers: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
// effect-nit-allow P9-no-internal-cross-import: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
import * as watch from '../src/env/internal/watch.ts'

// effect-nit-allow P8-tests-import-public-specifiers: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
// effect-nit-allow P9-no-internal-cross-import: this fixture directly tests private construction wiring; its public export denial is verified by NodeEnvExports.
import * as mutation from '../src/tools/internal/mutation.ts'

import * as Read from 'effect-harness/tools/Read'

import * as Write from 'effect-harness/tools/Write'

import * as Image from 'effect-harness/tools/Image'

import * as Bash from 'effect-harness/tools/Bash'

import { ToolCall } from 'effect-harness/Invocation'

import { withEnv } from './EnvFixture.ts'

import { message } from './ToolResultText.ts'

import { recording } from './InvocationRecorder.ts'

describe('EnvAdversarial', () => {
  describe('boundary race and failure regressions', () => {
    it.effect(
      'temp prefixes/suffixes persist through cleanup and filesystem error meanings remain semantic',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const ownerScope = yield* Scope.fork(yield* Scope.Scope)
            const owned = Context.get(
              yield* Layer.build(NodeEnv.layer({ cwd: env.cwd, shell: '/bin/sh' })).pipe(
                Scope.provide(ownerScope),
              ),
              Env,
            )
            const file = yield* owned.createTempFile({ prefix: 'prefix-', suffix: '.data' })
            assert.strictEqual(env.path.basename(file).startsWith('prefix-'), true)
            assert.strictEqual(file.endsWith('.data'), true)
            yield* Scope.close(ownerScope, Exit.void)
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
    // Native child-process spawn, pipe delivery and kill/join callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'nonblocking regular-file validation refuses a FIFO and canceled open does not escape a live reader',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            yield* env.exec(['/usr/bin/mkfifo', env.path.join(env.cwd, 'pipe')])
            assert.strictEqual((yield* Effect.flip(env.openBinaryReader('pipe'))).code, 'invalid')
            yield* env.writeFile('admitted', 'owned reader')
            const admitted = yield* Deferred.make<BinaryReader>()
            const fork = yield* Effect.scoped(
              env.openBinaryReader('admitted').pipe(
                Effect.tap((reader) => Deferred.succeed(admitted, reader)),
                Effect.andThen(Effect.never),
              ),
            ).pipe(Effect.forkChild)
            const reader = yield* Deferred.await(admitted)
            yield* Fiber.interrupt(fork)
            const exit = yield* Fiber.await(fork)
            assert.isTrue(Exit.isFailure(exit))
            if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterruptsOnly(exit.cause))
            const closed = yield* Effect.flip(reader.read(0, 1))
            assert.instanceOf(closed, FileError)
            assert.strictEqual(closed.reason._tag, 'FileInvalidError')
            assert.strictEqual(closed.message, 'Reader is closed')
          }),
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
            const shrinking: Env['Service'] = Env.of({
              ...real,
              openBinaryReader: (path, options) =>
                real.openBinaryReader(path, options).pipe(
                  Effect.map((reader) =>
                    makeBinaryReader({
                      ...reader,
                      info: Effect.gen(function* () {
                        const count = yield* Ref.updateAndGet(calls, (n) => n + 1)
                        if (count === 2) yield* overwrite(path, 'new')
                        return yield* reader.info
                      }),
                    }),
                  ),
                  Effect.tap((reader) => Ref.set(saved, reader)),
                ),
            })
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
            const unstable: Env['Service'] = Env.of({
              ...shrinking,
              openBinaryReader: (path, options) =>
                real.openBinaryReader(path, options).pipe(
                  Effect.map((reader) =>
                    makeBinaryReader({
                      ...reader,
                      info: Effect.gen(function* () {
                        const count = yield* Ref.updateAndGet(calls, (n) => n + 1)
                        if (count === 2) yield* overwrite(path, 'four')
                        if (count === 4) yield* overwrite(path, 'x')
                        return yield* reader.info
                      }),
                    }),
                  ),
                ),
            })
            assert.match(
              (yield* Effect.flip(
                Read.handler({ path: 'file' }).pipe(Effect.provideService(Env, unstable)),
              )).message,
              /changed while/,
            )
            yield* real.writeFile('file', 'one')
            yield* Ref.set(calls, 0)
            const growing: Env['Service'] = Env.of({
              ...real,
              openBinaryReader: (path, options) =>
                real.openBinaryReader(path, options).pipe(
                  Effect.map((reader) =>
                    makeBinaryReader({
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
                    }),
                  ),
                ),
            })
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
            const first = yield* mutation
              .withFile(
                env.path.join(env.cwd, 'alias', 'back\\slash'),
                Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
              )
              .pipe(Effect.forkChild)
            yield* Deferred.await(entered)
            const second = yield* mutation
              .withFile(env.path.join(env.cwd, 'real', 'back\\slash'), Ref.set(secondRan, true))
              .pipe(Effect.forkChild)
            yield* Effect.yieldNow
            assert.strictEqual(yield* Ref.get(secondRan), false)
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(first)
            yield* Fiber.join(second)
            assert.strictEqual(yield* Ref.get(secondRan), true)
          }),
        ),
    )
    it.effect(
      'different namespaces are independent and a failed mutation releases its permit',
      () =>
        withEnv(
          Effect.gen(function* () {
            const real = yield* Env
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const first = yield* mutation
              .withFile(
                real.path.join(real.cwd, 'file'),
                Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
              )
              .pipe(Effect.forkChild)
            yield* Deferred.await(entered)
            const changed: Env['Service'] = Env.of({ ...real, id: 'other-namespace' })
            yield* Write.handler({ path: 'file', content: 'independent' }).pipe(
              Effect.provideService(Env, changed),
            )
            assert.strictEqual(yield* real.readTextFile('file'), 'independent')
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(first)
            yield* mutation
              .withFile(
                real.path.join(real.cwd, 'file'),
                Effect.fail(new FileError({ reason: new FileInvalidError({ message: 'failed' }) })),
              )
              .pipe(Effect.exit)
            yield* Write.handler({ path: 'file', content: 'next' })
            assert.strictEqual(yield* real.readTextFile('file'), 'next')
          }),
        ),
    )
    it.effect(
      'late APNG chunk walk is bounded and recognizes BMP/JPEG/GIF/WEBP signatures accurately',
      () =>
        Effect.gen(function* () {
          const bytes = new Uint8Array(70060)
          bytes.set([137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82])
          new DataView(bytes.buffer).setUint32(33, 70000)
          bytes.set(new TextEncoder().encode('junk'), 37)
          bytes.set(new TextEncoder().encode('acTL'), 70049)
          const reads: Array<number> = []
          return yield* Image.detectSupportedImageMimeTypeOf({
            size: bytes.length,
            read: (offset, length) =>
              Effect.sync(() => {
                reads.push(length)
                return bytes.subarray(offset, offset + length)
              }),
          }).pipe(
            Effect.tap((result) =>
              Effect.sync(() => {
                assertNone(result)
                assert.strictEqual(Math.max(...reads), 65536)
              }),
            ),
          )
        }),
    )
    // Native fs.watch installation, delivery and close callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'watch unavailable native coverage falls back with overflow; growing budget emits one terminal error and ends',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            const path = yield* Path.Path
            const native = yield* NativeFiles.pipe(Effect.provide(NodeNativeFiles.layer))
            const unavailable: NativeFiles['Service'] = {
              ...native,
              watchDirectory: () =>
                Effect.fail(
                  new FileError({
                    reason: new FileNotSupportedError({ message: 'No native watcher' }),
                  }),
                ),
            }
            const watcherScope = yield* Scope.fork(yield* Scope.Scope)
            const watcher = yield* watch
              .make({
                fs: fs,
                path: path,
                native: unavailable,
                targets: [{ path: env.cwd }],
                options: {
                  pollInterval: 10,
                },
              })
              .pipe(Scope.provide(watcherScope))
            assert.strictEqual(watcher.mode, 'polling')
            const overflow = yield* watcher.changes.pipe(Stream.take(1), Stream.runCollect)
            assert.deepStrictEqual(overflow, [{ _tag: 'Overflow' }])
            yield* Scope.close(watcherScope, Exit.void)
            const limitedScope = yield* Scope.fork(yield* Scope.Scope)
            const limited = yield* env
              .watch([{ path: '.', recursive: true }], {
                mode: 'polling',
                pollInterval: 10,
                directoryBudget: 2,
              })
              .pipe(Scope.provide(limitedScope))
            const result = yield* limited.changes.pipe(Stream.runCollect, Effect.forkChild)
            yield* env.writeFile('deep/a/file', 'new')
            const events = yield* Fiber.join(result).pipe(Effect.timeout(2000))
            assert.strictEqual(events.length, 1)
            assert.strictEqual(events[0] !== undefined && 'error' in events[0], true)
            yield* Scope.close(limitedScope, Exit.void)
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
            const env: Env['Service'] = Env.of({
              ...real,
              exec: () => Ref.update(count, (n) => n + 1).pipe(Effect.as({ exitCode: 7 })),
            })
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
              const env: Env['Service'] = Env.of({
                ...real,
                exec: (_command, options) =>
                  Effect.gen(function* () {
                    if (options?.onOutput !== undefined)
                      yield* options.onOutput('partial', { stream: 'stdout' })
                    if (early && options?.onSpill !== undefined)
                      yield* options.onSpill('/remote/output.log')
                    return yield* new ExecutionError({
                      reason: new ExecutionTimeoutError({
                        message: 'remote timeout',
                        spillPath: '/remote/output.log',
                      }),
                    })
                  }),
              })
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
})
