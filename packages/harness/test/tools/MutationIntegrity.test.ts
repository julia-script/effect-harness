import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Option from 'effect/Option'
import * as FileSystem from 'effect/FileSystem'
import * as PlatformError from 'effect/PlatformError'
import * as Result from 'effect/Result'
import { Env, make } from '../../src/Env.ts'
import * as NodeEnv from '../../src/env/Node.ts'
import * as Edit from '../../src/tools/Edit.ts'
import * as Write from '../../src/tools/Write.ts'
import * as EditDiff from '../../src/tools/EditDiff.ts'
import { withEnv } from './Helpers.ts'

describe('MutationIntegrity', () => {
  it.effect('overlapping repeated targets reject before changing the file', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        for (const [content, oldText] of [
          ['aaa', 'aa'],
          ['ababa', 'aba'],
        ]) {
          if (content === undefined || oldText === undefined)
            return yield* Effect.die('Invalid test data')
          yield* env.writeFile('file', content)
          const error = yield* Effect.flip(
            Edit.handler({ path: 'file', edits: [{ oldText, newText: 'X' }] }),
          )
          assert.match(error.message, /2 occurrences/)
          assert.strictEqual(yield* env.readTextFile('file'), content)
        }
      }),
    ),
  )
  it('safe diff helpers return typed failures for invalid input', () => {
    const failure = EditDiff.applyEditsToNormalizedContent(
      'abc',
      [{ oldText: '', newText: 'x' }],
      'file',
    )
    assert.strictEqual(Result.isFailure(failure), true)
    if (Result.isFailure(failure)) assert.strictEqual(failure.failure._tag, 'EditError')
    assert.strictEqual(
      Result.isFailure(
        EditDiff.applyReplacementsPreservingUnchangedLines('abc', 'abc', [
          { matchIndex: -1, matchLength: 1, newText: 'x' },
        ]),
      ),
      true,
    )
  })
  it.effect('a partial write failure retains the original bytes and cleans staging files', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        const fs = yield* FileSystem.FileSystem
        yield* env.writeFile('file', 'original bytes')
        const fail = PlatformError.badArgument({
          module: 'FileSystem',
          method: 'writeAll',
          description: 'injected partial write failure',
        })
        let injected = false
        const broken: FileSystem.FileSystem = {
          ...fs,
          open: (file, options) =>
            fs.open(file, options).pipe(
              Effect.map((handle) => ({
                ...handle,
                stat: handle.stat,
                sync: handle.sync,
                writeAll: (bytes) =>
                  handle.writeAll(bytes.subarray(0, 3)).pipe(
                    Effect.andThen(
                      Effect.sync(() => {
                        injected = true
                      }),
                    ),
                    Effect.andThen(Effect.fail(fail)),
                  ),
              })),
            ),
          // Failure control also targets the former truncating implementation.
          writeFileString: (file, content, options) =>
            fs.writeFileString(file, content.slice(0, 3), options).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  injected = true
                }),
              ),
              Effect.andThen(Effect.fail(fail)),
            ),
        }
        const custom = yield* make({ id: env.id, cwd: env.cwd }).pipe(
          Effect.provideService(FileSystem.FileSystem, broken),
          Effect.provide(NodeEnv.nativeLayer),
        )
        const error = yield* Effect.flip(custom.writeFile('file', 'replacement'))
        assert.match(error.message, /injected partial write failure/)
        assert.strictEqual(injected, true)
        assert.strictEqual(yield* env.readTextFile('file'), 'original bytes')
        assert.deepStrictEqual(yield* fs.readDirectory(env.cwd), ['file'])
      }),
    ),
  )
  it.live('successful exec waits for a 300ms admitted output consumer', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        let delivered = ''
        let interrupted = false
        const result = yield* env.exec('printf admitted', {
          onOutput: (text) =>
            Effect.sleep(300).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  delivered += text
                }),
              ),
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  interrupted = true
                }),
              ),
            ),
        })
        assert.strictEqual(result.exitCode, 0)
        assert.strictEqual(delivered, 'admitted')
        assert.strictEqual(interrupted, false)
      }),
    ),
  )
  it('safe diff failures cover every expected reason and preserve normalized UTF-16 coordinates', () => {
    const cases: ReadonlyArray<readonly [string, ReadonlyArray<EditDiff.Edit>, string]> = [
      ['abc', [], 'empty'],
      ['abc', [{ oldText: '   ', newText: 'x' }], 'empty'],
      ['abc', [{ oldText: 'missing', newText: 'x' }], 'not_found'],
      ['aaa', [{ oldText: 'aa', newText: 'x' }], 'duplicate'],
      [
        'abc',
        [
          { oldText: 'ab', newText: 'x' },
          { oldText: 'bc', newText: 'y' },
        ],
        'overlap',
      ],
      ['abc', [{ oldText: 'abc', newText: 'abc' }], 'no_change'],
    ]
    for (const [content, edits, code] of cases) {
      const result = EditDiff.applyEditsToNormalizedContent(content, edits, 'file')
      assert.strictEqual(Result.isFailure(result), true)
      if (Result.isFailure(result)) assert.strictEqual(result.failure.code, code)
    }
    assert.strictEqual(Option.isNone(EditDiff.fuzzyFindText('abc', 'missing')), true)
    assert.strictEqual(Option.isNone(EditDiff.fuzzyFindText('abc', '')), true)
    const exact = Result.getOrThrow(
      EditDiff.applyEditsToNormalizedContent('😀abc', [{ oldText: 'abc', newText: 'X' }], 'file'),
    )
    assert.strictEqual(exact.newContent, '😀X')
    const fuzzy = Result.getOrThrow(
      EditDiff.applyEditsToNormalizedContent(
        '😀ﬁ target\nuntouched —   ',
        [{ oldText: 'fi target', newText: 'X' }],
        'file',
      ),
    )
    assert.strictEqual(fuzzy.newContent, '😀X\nuntouched —   ')
    for (const replacement of [
      { matchIndex: 1, matchLength: 99, newText: 'x' },
      { matchIndex: 0, matchLength: 0, newText: 'x' },
      { matchIndex: 0.5, matchLength: 1, newText: 'x' },
    ])
      assert.strictEqual(
        Result.isFailure(
          EditDiff.applyReplacementsPreservingUnchangedLines('abc', 'abc', [replacement]),
        ),
        true,
      )
  })
  it.effect(
    'replacement follows symlinks, preserves mode/owner and leaves append on the existing inode',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          const target = env.path.join(env.cwd, 'target')
          const alias = env.path.join(env.cwd, 'alias')
          yield* env.writeFile(target, 'old')
          yield* fs.chmod(target, 0o751)
          const before = yield* fs.stat(target)
          yield* fs.symlink(target, alias)
          yield* Write.handler({ path: alias, content: 'new' })
          const after = yield* fs.stat(target)
          assert.strictEqual(after.mode & 0o7777, 0o751)
          assert.deepStrictEqual(after.uid, before.uid)
          assert.deepStrictEqual(after.gid, before.gid)
          assert.strictEqual((yield* env.fileInfo(alias)).kind, 'symlink')
          assert.strictEqual(yield* env.readTextFile(target), 'new')
          assert.strictEqual(
            Option.isSome(after.ino) &&
              Option.isSome(before.ino) &&
              after.ino.value !== before.ino.value,
            true,
          )
          yield* env.appendFile(alias, ' appended')
          assert.deepStrictEqual((yield* fs.stat(target)).ino, after.ino)
          assert.strictEqual(yield* env.readTextFile(target), 'new appended')
          yield* fs.symlink(env.path.join(env.cwd, 'missing'), env.path.join(env.cwd, 'dangling'))
          assert.strictEqual(
            (yield* Effect.flip(env.writeFile('dangling', 'x'))).code,
            'not_supported',
          )
          assert.strictEqual((yield* env.fileInfo('dangling')).kind, 'symlink')
          yield* fs.link(target, env.path.join(env.cwd, 'hardlink'))
          assert.strictEqual(
            (yield* Effect.flip(env.writeFile(target, 'unsupported'))).code,
            'not_supported',
          )
          assert.strictEqual(yield* env.readTextFile('hardlink'), 'new appended')
        }),
      ),
  )
  it.effect(
    'cancellation retains the canonical mutex until a partial failed write settles and cleans up',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          yield* env.writeFile('file', 'original')
          yield* fs.symlink(env.path.join(env.cwd, 'file'), env.path.join(env.cwd, 'alias'))
          const admitted = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined).pipe(Effect.asVoid))
          const writes = yield* Ref.make(0)
          const slow: FileSystem.FileSystem = {
            ...fs,
            open: (file, options) =>
              fs.open(file, options).pipe(
                Effect.map((handle) => ({
                  ...handle,
                  stat: handle.stat,
                  sync: handle.sync,
                  writeAll: (bytes) =>
                    Effect.gen(function* () {
                      const count = yield* Ref.updateAndGet(writes, (n) => n + 1)
                      if (count === 1) {
                        yield* handle.writeAll(bytes.subarray(0, 2))
                        yield* Deferred.succeed(admitted, undefined)
                        yield* Deferred.await(release)
                        return yield* PlatformError.badArgument({
                          module: 'FileSystem',
                          method: 'writeAll',
                          description: 'admitted write failed',
                        })
                      }
                      yield* handle.writeAll(bytes)
                    }),
                })),
              ),
          }
          const custom = yield* make({ id: env.id, cwd: env.cwd }).pipe(
            Effect.provideService(FileSystem.FileSystem, slow),
            Effect.provide(NodeEnv.nativeLayer),
          )
          const first = yield* Write.handler({ path: 'file', content: 'first' }).pipe(
            Effect.provideService(Env, custom),
            Effect.forkChild,
          )
          yield* Deferred.await(admitted)
          const staging = (yield* fs.readDirectory(env.cwd)).find((name) =>
            name.startsWith('.effect-harness-'),
          )
          assert.isDefined(staging)
          if (staging !== undefined)
            assert.strictEqual(
              (yield* fs.stat(env.path.join(env.cwd, staging))).mode & 0o777,
              0o600,
            )
          const settled = yield* Ref.make(false)
          const stopping = yield* Fiber.interrupt(first).pipe(
            Effect.andThen(Ref.set(settled, true)),
            Effect.forkChild,
          )
          const second = yield* Write.handler({ path: 'alias', content: 'second' }).pipe(
            Effect.provideService(Env, custom),
            Effect.forkChild,
          )
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(settled), false)
          assert.strictEqual(yield* Ref.get(writes), 1)
          assert.strictEqual(yield* env.readTextFile('file'), 'original')
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(stopping)
          yield* Fiber.join(second)
          assert.strictEqual(yield* env.readTextFile('file'), 'second')
          assert.deepStrictEqual((yield* fs.readDirectory(env.cwd)).sort(), ['alias', 'file'])
        }),
      ),
  )
  it.live(
    'slow admitted spill and output callbacks finish, while explicit timeout remains semantic',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          let published: string | undefined
          let delivered = ''
          const result = yield* env.exec('printf spill', {
            spill: { afterBytes: 0, afterLines: 100 },
            onSpill: (file) =>
              Effect.sleep(300).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    published = file
                  }),
                ),
              ),
            onOutput: (text) =>
              Effect.sleep(300).pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    delivered += text
                  }),
                ),
              ),
          })
          assert.strictEqual(result.exitCode, 0)
          assert.strictEqual(result.spillPath, published)
          assert.strictEqual(delivered, 'spill')
          if (published !== undefined) yield* env.remove(published)
          let interrupted = false
          const failure = yield* Effect.flip(
            env.exec('printf timeout', {
              timeout: 0.05,
              onOutput: () =>
                Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Effect.sync(() => {
                      interrupted = true
                    }),
                  ),
                ),
            }),
          )
          assert.strictEqual(failure.code, 'timeout')
          assert.strictEqual(interrupted, true)
        }),
      ),
  )
  it.effect('sync and rename failures leave the original file intact and no staging file', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        const fs = yield* FileSystem.FileSystem
        for (const phase of ['sync', 'rename'] as const) {
          yield* env.writeFile('file', 'original')
          const error = PlatformError.badArgument({
            module: 'FileSystem',
            method: phase,
            description: `injected ${phase} failure`,
          })
          const broken: FileSystem.FileSystem = {
            ...fs,
            open: (file, options) =>
              fs.open(file, options).pipe(
                Effect.map((handle) => ({
                  ...handle,
                  stat: handle.stat,
                  writeAll: (bytes) => handle.writeAll(bytes),
                  sync:
                    phase === 'sync' && options?.flag === 'r+' ? Effect.fail(error) : handle.sync,
                })),
              ),
            rename: (source, destination) =>
              phase === 'rename' ? Effect.fail(error) : fs.rename(source, destination),
          }
          const custom = yield* make({ id: env.id, cwd: env.cwd }).pipe(
            Effect.provideService(FileSystem.FileSystem, broken),
            Effect.provide(NodeEnv.nativeLayer),
          )
          assert.match(
            (yield* Effect.flip(custom.writeFile('file', 'replacement'))).message,
            new RegExp(`injected ${phase} failure`),
          )
          assert.strictEqual(yield* env.readTextFile('file'), 'original')
          assert.deepStrictEqual(yield* fs.readDirectory(env.cwd), ['file'])
        }
      }),
    ),
  )
  it.live('a slow admitted callback failure reaches the caller after the idle cutoff', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        const failure = yield* Effect.flip(
          env.exec('printf admitted', {
            onOutput: () =>
              Effect.sleep(300).pipe(Effect.andThen(Effect.die('late consumer failed'))),
          }),
        )
        assert.strictEqual(failure.code, 'callback_error')
        assert.match(failure.message, /late consumer failed/)
      }),
    ),
  )
})
