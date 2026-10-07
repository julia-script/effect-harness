/**
 * Reusable scoped environment fixtures and capability conformance cases.
 *
 * @since 0.0.0
 */
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import { constTrue, constFalse } from 'effect/Function'
import * as Option from 'effect/Option'
import * as Deferred from 'effect/Deferred'
import * as Duration from 'effect/Duration'
import * as DateTime from 'effect/DateTime'
// Environment conformance adapted from pi-durable (MIT), pinned 636703a0; see package NOTICE.
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import { Env, type FileError, type ExecutionError, WatchChange, type WatchTarget } from '../Env.ts'

/**
 * EnvConformance assertions contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Assertions {
  readonly strictEqual: (actual: unknown, expected: unknown) => void
  readonly deepStrictEqual: (actual: unknown, expected: unknown) => void
  readonly ok: (condition: unknown, message?: string) => void
}
/**
 * EnvConformance options contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Options = makeEnvConformance.Options
/**
 * EnvConformance case contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Case {
  readonly name: string
  readonly timeoutMs?: Duration.Input | undefined
  readonly run: Effect.Effect<void, FileError | ExecutionError, Env>
}

/**
 * Acquires a fresh writable cwd per Layer build; resource scope closes before directory removal.
 *
 * @category combinators
 * @since 0.0.0
 */
export const freshLayer = <E, R>(
  make: (cwd: string) => Layer.Layer<Env, E, R>,
): Layer.Layer<Env, E | import('effect/PlatformError').PlatformError, R | FileSystem.FileSystem> =>
  Layer.effect(
    Env,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-env-conformance-' })
      const resources = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
        Scope.close(scope, exit),
      )
      const context = yield* Layer.build(make(cwd)).pipe(Scope.provide(resources))
      return Context.get(context, Env)
    }),
  )

/** Runs once per resource scope. Adapter errors and caller service requirements stay visible. */
const withEnvImpl = <A, E, R, E2, R2>(
  effect: Effect.Effect<A, E, R>,
  layer: Layer.Layer<Env, E2, R2>,
): Effect.Effect<A, E | E2, Exclude<R, Env> | R2> =>
  Effect.scoped(effect.pipe(Effect.provide(layer)))
/**
 * Runs an operation once in a fresh adapter resource scope.
 *
 * @category combinators
 * @since 0.0.0
 */
export const withEnv: {
  <E2, R2>(
    layer: Layer.Layer<Env, E2, R2>,
  ): <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E | E2, Exclude<R, Env> | R2>
  <A, E, R, E2, R2>(
    self: Effect.Effect<A, E, R>,
    layer: Layer.Layer<Env, E2, R2>,
  ): Effect.Effect<A, E | E2, Exclude<R, Env> | R2>
} = dual(2, withEnvImpl)

const failure = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<E, never, R> =>
  effect.pipe(
    Effect.matchEffect({
      onFailure: Effect.succeed,
      onSuccess: () => Effect.die(new Error('Expected adapter failure')),
    }),
  )

const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes)
const covers = (self: WatchChange, path: string): boolean =>
  WatchChange.$match(self, {
    Overflow: constTrue,
    Error: constFalse,
    Paths: ({ paths }) =>
      paths.some(
        (reported) =>
          path === reported || path.startsWith(`${reported}/`) || path.startsWith(`${reported}\\`),
      ),
  })

interface Watching {
  readonly changes: Ref.Ref<ReadonlyArray<WatchChange>>
  readonly expectChange: (
    path: string,
    change: Effect.Effect<void, FileError>,
  ) => Effect.Effect<void, FileError>
}
const watching = Effect.fnUntraced(function* <E, R>(
  targets: ReadonlyArray<WatchTarget>,
  run: (helpers: Watching) => Effect.Effect<void, E, R>,
): Effect.fn.Return<void, E | FileError, R | Env | Scope.Scope> {
  const env = yield* Env
  const changes = yield* Ref.make<ReadonlyArray<WatchChange>>([])
  const watcher = yield* env.watch(targets)
  yield* watcher.changes.pipe(
    Stream.runForEach((change) => Ref.update(changes, (old) => [...old, change])),
    Effect.forkScoped,
  )
  const expectChange = Effect.fnUntraced(function* (
    path: string,
    change: Effect.Effect<void, FileError>,
  ) {
    const absolute = yield* env.absolutePath(path)
    const from = (yield* Ref.get(changes)).length
    yield* change
    const deadline = DateTime.addDuration(yield* DateTime.now, '3 seconds')
    while (!(yield* Ref.get(changes)).slice(from).some((value) => covers(value, absolute))) {
      const error = Arr.findFirst(yield* Ref.get(changes), WatchChange.$is('Error'))
      yield* Option.match(error, {
        onNone: () => Effect.void,
        onSome: (self) => Effect.fail(self.error),
      })
      if (DateTime.isGreaterThanOrEqualTo(yield* DateTime.now, deadline))
        return yield* Effect.die(`No watch change reported ${absolute}`)
      yield* Effect.sleep('20 millis')
    }
  })
  yield* run({ changes, expectChange })
}, Effect.scoped)

/**
 * Runner-independent native Effects.
 *
 * **Details**
 *
 * Supply a fresh empty Env Layer separately for every case.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeEnvConformance = (options: Options): Array<Case> => {
  const assert = options.assertions
  const shell = options.shell ?? ['sh', '-c']
  const test = (
    name: string,
    run: Effect.Effect<void, FileError | ExecutionError, Env | Scope.Scope>,
    timeoutMs?: Duration.Input,
  ): Case => ({ name, run: Effect.scoped(run), ...(timeoutMs === undefined ? {} : { timeoutMs }) })
  const watch = (
    name: string,
    run: Effect.Effect<void, FileError | ExecutionError, Env | Scope.Scope>,
  ) => test(name, run, '30 seconds')
  const collect = Effect.fnUntraced(function* (
    command: string | ReadonlyArray<string>,
    cwd?: string,
  ) {
    const env = yield* Env
    const output = yield* Ref.make({ stdout: '', stderr: '' })
    const result = yield* env.exec(command, {
      ...(cwd === undefined ? {} : { cwd }),
      onOutput: (text, info) =>
        Ref.update(output, (value) => ({ ...value, [info.stream]: value[info.stream] + text })),
    })
    return { result, ...(yield* Ref.get(output)) }
  })
  const cases: Array<Case> = [
    test(
      'binary reader reads byte ranges of the opened file',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.writeFile('data.txt', 'hello world')
        const readerScope = yield* Scope.fork(yield* Scope.Scope)
        const reader = yield* env.openBinaryReader('data.txt').pipe(Scope.provide(readerScope))
        const info = yield* reader.info
        assert.strictEqual(info.name, 'data.txt')
        assert.strictEqual(info.kind, 'file')
        assert.strictEqual(info.size, 11)
        assert.strictEqual(decode(yield* reader.read(0, 5)), 'hello')
        assert.strictEqual(decode(yield* reader.read(6, 100)), 'world')
        for (const [offset, length] of [
          [11, 4],
          [50, 1],
          [3, 0],
        ] as const)
          assert.strictEqual((yield* reader.read(offset, length)).length, 0)
        assert.strictEqual((yield* failure(reader.read(-1, 1))).code, 'invalid')
        assert.strictEqual((yield* failure(reader.read(0, 1.5))).code, 'invalid')
        yield* Scope.close(readerScope, Exit.void)
        yield* Scope.close(readerScope, Exit.void)
        assert.strictEqual((yield* failure(reader.read(0, 1))).code, 'invalid')
        assert.strictEqual((yield* failure(reader.info)).code, 'invalid')
      }),
    ),
    test(
      'binary reader scans lines like decoding the whole file',
      Effect.gen(function* () {
        const env = yield* Env
        const bytes = Uint8Array.from([
          0xef, 0xbb, 0xbf, 0x61, 10, 0xe2, 0x82, 10, 10, 0xef, 0xbb, 0xbf, 0x62, 10, 0xc3, 0xa9,
        ])
        yield* env.writeFile('lines.txt', bytes)
        const lines = decode(bytes).split('\n')
        const reader = yield* env.openBinaryReader('lines.txt')
        for (const [startLine, endLine] of [
          [0, undefined],
          [0, 1],
          [1, 3],
          [2, 3],
          [3, undefined],
          [4, 9],
        ] as const) {
          const scan = yield* reader.scanLines({
            startLine,
            ...(endLine === undefined ? {} : { endLine }),
          })
          const selected = lines.slice(startLine, endLine).join('\n')
          assert.strictEqual(scan.newlines, lines.length - 1)
          assert.strictEqual(
            new TextDecoder('utf-8', { ignoreBOM: scan.start > 0 }).decode(
              bytes.subarray(scan.start, scan.end),
            ),
            selected,
          )
          assert.strictEqual(scan.selectedBytes, new TextEncoder().encode(selected).length)
          assert.strictEqual(
            new TextDecoder('utf-8', { ignoreBOM: scan.start > 0 }).decode(
              bytes.subarray(scan.start, scan.firstLineEnd),
            ),
            lines[startLine],
          )
          assert.strictEqual(scan.firstLineBytes, new TextEncoder().encode(lines[startLine]).length)
        }
        assert.strictEqual((yield* reader.scanLines({ startLine: 9 })).start, bytes.length)
        assert.strictEqual(
          (yield* failure(reader.scanLines({ startLine: 2, endLine: 2 }))).code,
          'invalid',
        )
      }),
    ),
    test(
      'binary reader keeps reading the file it opened after a rename',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.writeFile('a.txt', 'one')
        const reader = yield* env.openBinaryReader('a.txt')
        yield* env.renameFile('a.txt', 'b.txt')
        yield* env.writeFile('a.txt', 'two')
        assert.strictEqual(decode(yield* reader.read(0, 10)), 'one')
      }),
    ),
    test(
      'binary reader refuses directories and missing files',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.createDir('dir')
        assert.strictEqual((yield* failure(env.openBinaryReader('dir'))).code, 'is_directory')
        assert.strictEqual((yield* failure(env.openBinaryReader('missing'))).code, 'not_found')
      }),
    ),
    test(
      'directory reader pages every entry exactly once',
      Effect.gen(function* () {
        const env = yield* Env
        const names = ['a.txt', 'b.txt', 'c.txt', 'd.txt', 'e.txt']
        for (const name of names) yield* env.writeFile(name, name)
        yield* env.createDir('sub')
        const reader = yield* env.openDirReader('.')
        const found: Array<string> = []
        let done = false
        for (let index = 0; index < 1000 && !done; index++) {
          const page = yield* reader.next(2)
          assert.ok(page.entries.length <= 2)
          found.push(...page.entries.map((entry) => entry.name))
          for (const entry of page.entries) {
            assert.strictEqual(entry.kind, entry.name === 'sub' ? 'directory' : 'file')
            if (entry.name !== 'sub') assert.strictEqual(entry.size, 5)
          }
          done = page.done
        }
        assert.ok(done)
        assert.deepStrictEqual(found.sort(), [...names, 'sub'].sort())
      }),
    ),
    test(
      'directory reader reports the end and refuses use after close',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.createDir('empty')
        const readerScope = yield* Scope.fork(yield* Scope.Scope)
        const reader = yield* env.openDirReader('empty').pipe(Scope.provide(readerScope))
        assert.deepStrictEqual(yield* reader.next(10), { entries: [], done: true })
        assert.deepStrictEqual(yield* reader.next(10), { entries: [], done: true })
        assert.strictEqual((yield* failure(reader.next(0))).code, 'invalid')
        yield* Scope.close(readerScope, Exit.void)
        yield* Scope.close(readerScope, Exit.void)
        assert.strictEqual((yield* failure(reader.next(1))).code, 'invalid')
      }),
    ),
    test(
      'directory reader refuses missing paths and files',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.writeFile('file.txt', 'x')
        assert.strictEqual((yield* failure(env.openDirReader('missing'))).code, 'not_found')
        assert.strictEqual((yield* failure(env.openDirReader('file.txt'))).code, 'not_directory')
      }),
    ),
    test(
      'directory reader skips entries removed during enumeration',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.createDir('dir')
        for (const name of ['x', 'y', 'z']) yield* env.writeFile(`dir/${name}`, name)
        const reader = yield* env.openDirReader('dir')
        for (const name of ['x', 'y', 'z']) yield* env.remove(`dir/${name}`)
        assert.deepStrictEqual((yield* reader.next(10)).entries, [])
      }),
    ),
    watch(
      "watch reports a missing file's creation, changes, replacement and removal",
      Effect.gen(function* () {
        const env = yield* Env
        yield* watching(
          [{ path: 'AGENTS.md' }],
          Effect.fnUntraced(function* ({ expectChange }) {
            yield* expectChange('AGENTS.md', env.writeFile('AGENTS.md', 'one'))
            yield* expectChange('AGENTS.md', env.writeFile('AGENTS.md', 'two!'))
            yield* expectChange(
              'AGENTS.md',
              env
                .writeFile('AGENTS.tmp', 'three')
                .pipe(Effect.andThen(env.renameFile('AGENTS.tmp', 'AGENTS.md'))),
            )
            yield* expectChange('AGENTS.md', env.remove('AGENTS.md'))
          }),
        )
      }),
    ),
    watch(
      'watch reports a missing target whose ancestors are created',
      Effect.gen(function* () {
        const env = yield* Env
        yield* watching([{ path: 'a/b/c/AGENTS.md' }], ({ expectChange }) =>
          expectChange('a/b/c/AGENTS.md', env.writeFile('a/b/c/AGENTS.md', 'x')),
        )
      }),
    ),
    watch(
      'watch follows directories created together with their contents',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.createDir('skills')
        yield* watching(
          [{ path: 'skills', recursive: true }],
          Effect.fnUntraced(function* ({ expectChange }) {
            yield* expectChange('skills/a/b/SKILL.md', env.writeFile('skills/a/b/SKILL.md', 'one'))
            yield* expectChange('skills/a/b/SKILL.md', env.writeFile('skills/a/b/SKILL.md', 'two!'))
            yield* expectChange(
              'skills/a/b/c/SKILL.md',
              env.writeFile('skills/a/b/c/SKILL.md', 'deeper'),
            )
          }),
        )
      }),
    ),
    watch(
      'watch keeps watching a path whose parent is renamed and recreated',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.writeFile('proj/.pi/skills/x.md', 'x')
        yield* watching(
          [{ path: 'proj/.pi/skills', recursive: true }],
          Effect.fnUntraced(function* ({ expectChange }) {
            yield* expectChange('proj/.pi/skills', env.renameFile('proj/.pi', 'proj/old'))
            yield* expectChange('proj/.pi/skills/y.md', env.writeFile('proj/.pi/skills/y.md', 'y'))
            yield* expectChange('proj/.pi/skills/y.md', env.writeFile('proj/.pi/skills/y.md', 'yy'))
          }),
        )
      }),
    ),
    watch(
      'watch skips excluded entries and reports a rename out of them',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.createDir('skills')
        yield* watching(
          [{ path: 'skills', recursive: true, exclude: { hidden: true, names: ['node_modules'] } }],
          Effect.fnUntraced(function* ({ changes, expectChange }) {
            yield* env.writeFile('skills/node_modules/dep/SKILL.md', 'dep')
            yield* env.writeFile('skills/.SKILL.tmp', 'draft')
            yield* expectChange(
              'skills/SKILL.md',
              env.renameFile('skills/.SKILL.tmp', 'skills/SKILL.md'),
            )
            const excluded = yield* Effect.forEach(
              ['skills/node_modules', 'skills/.SKILL.tmp'],
              (path) => env.absolutePath(path),
            )
            for (const change of yield* Ref.get(changes))
              if (WatchChange.$is('Paths')(change))
                for (const path of change.paths)
                  assert.ok(
                    !excluded.some((target) => path === target || path.startsWith(`${target}/`)),
                  )
          }),
        )
      }),
    ),
    watch(
      'watch keeps recursive coverage where a non-recursive target overlaps',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.writeFile('skills/a/one.md', 'one')
        yield* watching(
          [{ path: 'skills' }, { path: 'skills', recursive: true }],
          ({ expectChange }) =>
            expectChange('skills/a/two.md', env.writeFile('skills/a/two.md', 'two')),
        )
      }),
    ),
    watch(
      'watch follows a directory replaced at the same path',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.writeFile('skills/a/x.md', 'x')
        yield* watching(
          [{ path: 'skills', recursive: true }],
          Effect.fnUntraced(function* ({ expectChange }) {
            yield* expectChange(
              'skills/a',
              env
                .renameFile('skills/a', 'skills-old')
                .pipe(Effect.andThen(env.createDir('skills/a'))),
            )
            yield* expectChange('skills/a/y.md', env.writeFile('skills/a/y.md', 'y'))
            yield* expectChange('skills/a/y.md', env.writeFile('skills/a/y.md', 'yy'))
          }),
        )
      }),
    ),
    watch(
      'watch stops reporting once closed',
      Effect.gen(function* () {
        const env = yield* Env
        const watcherScope = yield* Scope.fork(yield* Scope.Scope)
        const watcher = yield* env.watch([{ path: 'file.txt' }]).pipe(Scope.provide(watcherScope))
        const changes = yield* Ref.make<ReadonlyArray<WatchChange>>([])
        yield* watcher.changes.pipe(
          Stream.runForEach((change) => Ref.update(changes, (old) => [...old, change])),
          Effect.forkScoped,
        )
        assert.ok(watcher.mode === 'native' || watcher.mode === 'polling')
        yield* Scope.close(watcherScope, Exit.void)
        yield* Scope.close(watcherScope, Exit.void)
        yield* env.writeFile('file.txt', 'x')
        // Negative native/polling delivery window: OS event delivery and actual
        // adapter polling use the host clock; virtual time cannot exercise them.
        yield* Effect.sleep('300 millis')
        assert.deepStrictEqual(yield* Ref.get(changes), [])
      }),
    ),
    test(
      'argv exec passes arguments to the program without shell parsing',
      Effect.gen(function* () {
        const env = yield* Env
        const hostile = "it's $(touch pwned) `touch pwned` *; touch pwned"
        const value = yield* collect([
          ...shell,
          'printf "%s|%s" "$1" "$2"',
          'argv0',
          hostile,
          'a b',
        ])
        assert.strictEqual(value.result.exitCode, 0)
        assert.strictEqual(value.stdout, `${hostile}|a b`)
        assert.strictEqual(yield* env.exists('pwned'), false)
      }),
    ),
    test(
      'exec reports the stream of every chunk in both forms',
      Effect.gen(function* () {
        const script = 'printf out; printf err >&2; printf more'
        for (const command of [[...shell, script], script]) {
          const value = yield* collect(command)
          assert.strictEqual(value.result.exitCode, 0)
          assert.strictEqual(value.stdout, 'outmore')
          assert.strictEqual(value.stderr, 'err')
        }
      }),
    ),
    test(
      'argv exec honors cwd and exit codes',
      Effect.gen(function* () {
        const env = yield* Env
        yield* env.createDir('sub')
        const value = yield* collect([...shell, 'printf x > made.txt; exit 3'], 'sub')
        assert.strictEqual(value.result.exitCode, 3)
        assert.strictEqual(yield* env.readTextFile('sub/made.txt'), 'x')
      }),
    ),
    test(
      'argv exec reports missing programs and empty argv as spawn errors',
      Effect.gen(function* () {
        const env = yield* Env
        for (const command of [['harness-conformance-missing-program'], []])
          assert.strictEqual((yield* failure(env.exec(command))).code, 'spawn_error')
      }),
    ),
    test(
      'windowed exec keeps the exact tail and counts what it skips',
      Effect.gen(function* () {
        const env = yield* Env
        const counted = yield* Ref.make({ bytes: 0, newlines: 0, tail: '' })
        const window = {
          maxBytes: 200,
          maxLines: 5,
          minIntervalMs: 0,
          bytesPerSecond: 1_000_000_000,
        }
        const result = yield* env.exec(
          [...shell, 'i=0; while [ $i -lt 2000 ]; do echo line-$i; i=$((i+1)); done'],
          {
            window,
            onOutput: (text, info) =>
              Ref.update(counted, (value) => {
                if (info.skipped !== undefined)
                  assert.ok(
                    new TextEncoder().encode(text).length > window.maxBytes ||
                      text.split('\n').length - 1 > window.maxLines,
                  )
                return {
                  bytes:
                    value.bytes +
                    (info.skipped?.bytes ?? 0) +
                    new TextEncoder().encode(text).length,
                  newlines:
                    value.newlines + (info.skipped?.newlines ?? 0) + text.split('\n').length - 1,
                  tail: (info.skipped === undefined ? value.tail : '') + text,
                }
              }),
          },
        )
        const expected = Arr.makeBy(2000, (index) => `line-${index}\n`)
        assert.strictEqual(result.exitCode, 0)
        const { bytes, newlines, tail } = yield* Ref.get(counted)
        assert.strictEqual(bytes, expected.join('').length)
        assert.strictEqual(newlines, 2000)
        assert.ok(tail.endsWith(expected.slice(-5).join('')))
      }),
    ),
    test(
      'argv exec distinguishes timeout from native interruption',
      Effect.gen(function* () {
        const env = yield* Env
        assert.strictEqual(
          (yield* failure(env.exec([...shell, 'sleep 2'], { timeout: '100 millis' }))).code,
          'timeout',
        )
        const started = yield* Deferred.make<void>()
        const fiber = yield* env
          .exec([...shell, 'printf admitted; sleep 2'], {
            onOutput: () => Deferred.succeed(started, undefined).pipe(Effect.asVoid),
          })
          .pipe(Effect.forkScoped)
        yield* Deferred.await(started).pipe(
          Effect.timeoutOrElse({
            duration: '3 seconds',
            orElse: () => Effect.die('Native process output was not admitted'),
          }),
        )
        yield* Fiber.interrupt(fiber)
        const exit = yield* Fiber.await(fiber)
        assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause))
      }),
    ),
  ]
  if (options.symlinks !== false)
    cases.push(
      test(
        'binary reader follows symlinks unless noFollow refuses the final one',
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.writeFile('target.txt', 'target')
          yield* env.writeFile('sub/inner.txt', 'inner')
          assert.strictEqual(
            (yield* env.exec([...shell, 'ln -s target.txt link.txt && ln -s sub dirlink']))
              .exitCode,
            0,
          )
          assert.strictEqual(
            decode(yield* (yield* env.openBinaryReader('link.txt')).read(0, 10)),
            'target',
          )
          assert.strictEqual(
            (yield* failure(env.openBinaryReader('link.txt', { noFollow: true }))).code,
            'invalid',
          )
          assert.strictEqual(
            decode(
              yield* (yield* env.openBinaryReader('dirlink/inner.txt', { noFollow: true })).read(
                0,
                10,
              ),
            ),
            'inner',
          )
        }),
      ),
      watch(
        'watch reports changes to the file a watched symbolic link points to',
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.writeFile('data/real.md', 'one')
          yield* env.createDir('config')
          assert.strictEqual(
            (yield* env.exec([...shell, 'ln -s ../data/real.md config/AGENTS.md'])).exitCode,
            0,
          )
          yield* watching([{ path: 'config/AGENTS.md' }], ({ expectChange }) =>
            expectChange('config/AGENTS.md', env.writeFile('data/real.md', 'two!')),
          )
        }),
      ),
    )
  return cases
}

/**
 * Type contracts owned by makeEnvConformance.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace makeEnvConformance {
  /**
   * Configuration for makeEnvConformance.
   *
   * @category models
   * @since 0.0.0
   */
  interface Options {
    readonly assertions: Assertions
    /** Program and arguments that accept a POSIX script as the next argument. */
    readonly shell?: ReadonlyArray<string> | undefined
    readonly symlinks?: boolean | undefined
  }
}
