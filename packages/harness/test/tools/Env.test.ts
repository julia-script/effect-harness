import * as Option from 'effect/Option'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as FileSystem from 'effect/FileSystem'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import { Env, FileError } from '../../src/Env.ts'
import * as Decode from '../../src/env/Decode.ts'
import * as Scanner from '../../src/env/LineScan.ts'
import { withEnv } from './Helpers.ts'

describe('portable native Env filesystem resources', () => {
  it.effect('normalizes cwd/file URLs/home paths and retains outside cwd access', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        assert.strictEqual(
          yield* env.absolutePath('nested/../file'),
          env.path.join(env.cwd, 'file'),
        )
        assert.strictEqual(
          yield* env.absolutePath((yield* env.path.toFileUrl(env.path.join(env.cwd, 'a b'))).href),
          env.path.join(env.cwd, 'a b'),
        )
        assert.strictEqual((yield* env.absolutePath('~/file')).endsWith('/file'), true)
        assert.strictEqual(yield* env.absolutePath('/tmp/outside'), '/tmp/outside')
        assert.strictEqual(
          yield* env.joinPath([env.cwd, 'a', '..', 'b']),
          env.path.join(env.cwd, 'b'),
        )
      }),
    ),
  )
  it.effect(
    'write/append create parents; truncate extends zero bytes, refuses missing/invalid; flush/rename metadata',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.appendFile('a/b', 'first')
          yield* env.appendFile('a/b', ' second')
          assert.strictEqual(yield* env.readTextFile('a/b'), 'first second')
          yield* env.truncateFile('a/b', 14)
          assert.deepStrictEqual(Array.from((yield* env.readBinaryFile('a/b')).slice(-2)), [0, 0])
          yield* env.truncateFile('a/b', 5)
          yield* env.flushFile('a/b')
          yield* env.writeFile('replaced', 'old')
          yield* env.renameFile('a/b', 'replaced')
          assert.strictEqual(yield* env.readTextFile('replaced'), 'first')
          assert.strictEqual((yield* env.fileInfo('replaced')).kind, 'file')
          assert.strictEqual((yield* Effect.flip(env.truncateFile('missing', 0))).code, 'not_found')
          assert.strictEqual((yield* Effect.flip(env.truncateFile('replaced', -1))).code, 'invalid')
          assert.strictEqual(yield* env.exists('missing'), false)
          yield* env.remove('a', { recursive: true })
          assert.strictEqual(yield* env.exists('a'), false)
        }),
      ),
  )
  it.effect(
    'opened binary inode survives rename, noFollow rejects final links but accepts earlier links; close/ranges reject',
    () =>
      withEnv(
        Effect.scoped(
          Effect.gen(function* () {
            const env = yield* Env
            const fs = yield* FileSystem.FileSystem
            yield* env.writeFile('target', 'abcdef')
            yield* fs.symlink(env.path.join(env.cwd, 'target'), env.path.join(env.cwd, 'link'))
            assert.strictEqual((yield* env.fileInfo('link')).kind, 'symlink')
            assert.strictEqual(
              (yield* Effect.flip(env.openBinaryReader('link', { noFollow: true }))).code,
              'invalid',
            )
            const readerScope = yield* Scope.fork(yield* Scope.Scope)
            const reader = yield* env.openBinaryReader('link').pipe(Scope.provide(readerScope))
            yield* env.renameFile('target', 'moved')
            yield* env.writeFile('target', 'replacement')
            assert.strictEqual((yield* reader.info).size, 6)
            assert.strictEqual(
              new TextDecoder().decode(yield* reader.read(1, Number.MAX_SAFE_INTEGER)),
              'bcdef',
            )
            assert.strictEqual((yield* Effect.flip(reader.read(-1, 1))).code, 'invalid')
            assert.strictEqual(
              (yield* Effect.flip(reader.scanLines({ startLine: 2, endLine: 2 }))).code,
              'invalid',
            )
            yield* Scope.close(readerScope, Exit.void)
            yield* Scope.close(readerScope, Exit.void)
            assert.strictEqual((yield* Effect.flip(reader.read(0, 1))).code, 'invalid')
            yield* env.createDir('real')
            yield* env.writeFile('real/file', 'good')
            yield* fs.symlink(env.path.join(env.cwd, 'real'), env.path.join(env.cwd, 'alias'))
            assert.strictEqual(
              new TextDecoder().decode(
                yield* (yield* env.openBinaryReader('alias/file', { noFollow: true })).read(0, 4),
              ),
              'good',
            )
            assert.strictEqual(
              (yield* Effect.flip(env.openBinaryReader('real'))).code,
              'is_directory',
            )
          }),
        ),
      ),
  )
  it.effect(
    'resource scope invalidates handles and directory pages expose symlinks exactly once',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          for (const name of ['a', 'b', 'c']) yield* env.writeFile(name, name)
          yield* fs.symlink(env.path.join(env.cwd, 'a'), env.path.join(env.cwd, 'link'))
          const scope = yield* Scope.fork(yield* Scope.Scope)
          const reader = yield* env
            .openDirReader('.')
            .pipe(Effect.provideService(Scope.Scope, scope))
          const names: string[] = []
          while (true) {
            const page = yield* reader.next(2)
            names.push(...page.entries.map((entry) => entry.name))
            if (page.done) break
          }
          assert.deepStrictEqual(names.sort(), ['a', 'b', 'c', 'link'])
          assert.strictEqual((yield* reader.next(2)).done, true)
          yield* Scope.close(scope, Exit.void)
          assert.strictEqual((yield* Effect.flip(reader.next(1))).code, 'invalid')
          assert.strictEqual(
            (yield* Effect.flip(env.openDirReader('a').pipe(Effect.scoped))).code,
            'not_directory',
          )
          assert.strictEqual(
            (yield* Effect.flip(env.openDirReader('absent').pipe(Effect.scoped))).code,
            'not_found',
          )
          yield* env.writeFile('buffered', 'first\nsecond\n')
          const textScope = yield* Scope.fork(yield* Scope.Scope)
          const text = yield* env
            .openTextLineReader('buffered')
            .pipe(Effect.provideService(Scope.Scope, textScope))
          assert.deepStrictEqual(Option.getOrThrow(yield* text.readLine), {
            text: 'first',
            terminated: true,
          })
          yield* Scope.close(textScope, Exit.void)
          assert.strictEqual((yield* Effect.flip(text.readLine)).code, 'invalid')
        }),
      ),
  )
  it.effect(
    'text lines preserve CR and LF termination, leading BOM drops, interior BOM/split invalid UTF8 matches one-shot',
    () =>
      withEnv(
        Effect.scoped(
          Effect.gen(function* () {
            const env = yield* Env
            yield* env.writeFile('lines', '\ufeffa\r\nb\n\ufeffc')
            const readerScope = yield* Scope.fork(yield* Scope.Scope)
            const reader = yield* env.openTextLineReader('lines').pipe(Scope.provide(readerScope))
            assert.deepStrictEqual(Option.getOrThrow(yield* reader.readLine), {
              text: 'a\r',
              terminated: true,
            })
            assert.deepStrictEqual(Option.getOrThrow(yield* reader.readLine), {
              text: 'b',
              terminated: true,
            })
            assert.deepStrictEqual(Option.getOrThrow(yield* reader.readLine), {
              text: '\ufeffc',
              terminated: false,
            })
            assert.isTrue(Option.isNone(yield* reader.readLine))
            yield* Scope.close(readerScope, Exit.void)
            assert.strictEqual((yield* Effect.flip(reader.readLine)).code, 'invalid')
            yield* env.writeFile('empty', '')
            assert.deepStrictEqual(yield* env.readTextLines('empty'), [])
            yield* env.writeFile('empty', '\n')
            assert.deepStrictEqual(yield* env.readTextLines('empty'), [''])
            assert.deepStrictEqual(yield* env.readTextLines('lines', { maxLines: 1 }), ['a\r'])
            const invalid = yield* Effect.flip(env.readTextLines('lines', { maxLines: -1 }))
            assert.instanceOf(invalid, FileError)
            assert.strictEqual(invalid.reason._tag, 'FileInvalid')
            assert.strictEqual(invalid.message, 'Invalid maxLines')
            assert.strictEqual(invalid.path, 'lines')
            assert.strictEqual(invalid.cause, undefined)
          }),
        ),
      ),
  )
  it('stream decoder and line scanner are chunk invariant for BOM, invalid bytes and newline selections', () => {
    const bytes = new Uint8Array([...new TextEncoder().encode('\ufeffα\n\ufeffβ\n'), 0xe2, 0x82])
    const expected = new TextDecoder().decode(bytes)
    for (let size = 1; size <= 7; size++) {
      const decoder = Decode.make()
      let text = ''
      const scan = Result.getOrThrow(Scanner.make(1, 3))
      for (let position = 0; position < bytes.length; position += size) {
        const chunk = bytes.subarray(position, position + size)
        text += Decode.decode(decoder, chunk)
        Scanner.push(scan, chunk)
      }
      text += Decode.decode(decoder)
      assert.strictEqual(text, expected)
      const result = Scanner.finish(scan)
      assert.strictEqual(result.newlines, 2)
      assert.strictEqual(
        result.selectedBytes,
        new TextEncoder().encode(expected.split('\n').slice(1, 3).join('\n')).length,
      )
    }
  })
})
