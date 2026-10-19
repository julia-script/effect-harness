import { assert, describe, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
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
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import * as Record from 'effect-harness/Record'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import type * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as Edit from '../src/coding/Edit.ts'
import * as MutationCoordinator from 'effect-harness/coding/MutationCoordinator'
import * as Write from 'effect-harness/coding/Write'

const filesystem = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const native = NodeChildProcessSpawner.layer.pipe(Layer.provideMerge(filesystem))
const tools = Toolkit.merge(Edit.toolkit, Write.toolkit)
const execution = Layer.succeed(ToolExecution, {
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  taskId: Schema.decodeSync(Record.TaskId)(1),
  callId: 'edit-test',
  snapshot: () => Effect.die('Unexpected snapshot'),
  watch: () => Stream.die('Unexpected watch'),
  commit: () => Effect.die('Unexpected commit'),
  output: () => Effect.die('Unexpected output'),
  details: () => Effect.die('Unexpected details'),
  diagnostic: () => Effect.die('Unexpected diagnostic'),
})
const handlers = (root: string, options: Edit.Options = {}, backend = filesystem) => {
  const bound = ToolkitFileSystem.layer({ root }).pipe(Layer.provide(backend))
  const services = MutationCoordinator.layer.pipe(Layer.provideMerge(bound))
  return Layer.merge(Edit.layer(options), Write.layer).pipe(Layer.provideMerge(services))
}
const invoke = (name: 'edit' | 'write', args: Schema.JsonObject) =>
  Effect.flatMap(tools, (ready) => ready.invoke({ id: 'call', name, arguments: args })).pipe(
    Effect.provide(execution),
  )
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(native))
const root = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
})
const change = (oldText: string, newText: string): Edit.Replacement => ({ oldText, newText })
const outcome = (result: ToolResult.Result) => {
  assert.isFalse(result.isError)
  return Schema.decodeUnknownEffect(Edit.Outcome)(result.structuredOutput)
}
const failure = (result: ToolResult.Result) => {
  assert.isTrue(result.isError)
  assert.strictEqual(result.content[0]?.type, 'text')
  assert.isNotEmpty(result.diagnostics)
  return Schema.decodeUnknownEffect(Edit.FailureSchema)(result.structuredOutput)
}
const denied = (method: string) =>
  PlatformError.systemError({
    _tag: 'PermissionDenied',
    module: 'CodingEditTest',
    method,
    description: 'Injected failure',
  })

// Verify interoperability with an independent native unified-patch consumer.
const verifyPatch = Effect.fn('CodingEditTest.verifyPatch')(function* (
  path: string,
  before: string,
  after: string,
  patch: string,
) {
  const fs = yield* FileSystem.FileSystem
  const paths = yield* Path.Path
  const process = yield* ChildProcessSpawner.ChildProcessSpawner
  const patchFile = yield* fs.makeTempFileScoped()
  yield* fs.writeFileString(patchFile, patch)
  yield* fs.writeFileString(path, before)
  const exit = yield* process.exitCode(
    ChildProcess.make(
      'git',
      ['apply', '--no-index', '--unsafe-paths', '--whitespace=nowarn', '-p0', patchFile],
      { cwd: paths.dirname(path) },
    ),
  )
  assert.strictEqual(Number(exit), 0)
  assert.deepStrictEqual(yield* fs.readFile(path), new TextEncoder().encode(after))
})

describe('ready-made coding edit', () => {
  it.effect(
    'native patch application reproduces insertions, deletions and repeated-line edits',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const scenarios = [
            ['a\nb\na\nc\n', 'b\nx\na\nc\n'],
            ['a\nb\na\nc\n', 'a\nx\nb\na\nc\n'],
            ['a\nb\na\nc\n', 'a\na\n'],
            ['a\n\nb\n', '\na\n\nb\n\n'],
            ['a\nb\nc', 'a\nc\n'],
            ['a\nb\nc\n', 'x\na\nb\ny\nc\nz\n'],
            ['a\na\na\nb\na\n', 'a\nb\na\na\n'],
          ] as const
          for (const [before, after] of scenarios) {
            yield* fs.writeFileString(`${directory}/file`, before)
            const saved = yield* invoke('edit', {
              path: 'file',
              edits: [change(before, after)],
            }).pipe(Effect.provide(handlers(directory)), Effect.flatMap(outcome))
            yield* verifyPatch(saved.path, before, after, saved.patch)
          }
        }),
      ),
  )

  it.effect('reports unrelated lines as context between distant edits', () =>
    run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* root
        const before = 'first\nunchanged “quotes”  \nlast\n'
        const after = 'one\nunchanged “quotes”  \nthree\n'
        yield* fs.writeFileString(`${directory}/file`, before)
        const saved = yield* invoke('edit', {
          path: 'file',
          edits: [change('first', 'one'), change('last', 'three')],
        }).pipe(Effect.provide(handlers(directory)), Effect.flatMap(outcome))
        assert.include(saved.diff, ' 2 unchanged “quotes”  ')
        assert.notInclude(saved.diff, '-2 unchanged')
        assert.notInclude(saved.patch, '-unchanged')
        yield* verifyPatch(saved.path, before, after, saved.patch)
      }),
    ),
  )

  it.effect('quotes unusual path bytes so the actual patch remains applicable', () =>
    run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* root
        const file = 'unicode-é-tab\t-newline\n-control\u0001'
        yield* fs.writeFileString(`${directory}/${file}`, 'before\n')
        const saved = yield* invoke('edit', {
          path: file,
          edits: [change('before', 'after')],
        }).pipe(Effect.provide(handlers(directory)), Effect.flatMap(outcome))
        yield* verifyPatch(saved.path, 'before\n', 'after\n', saved.patch)
      }),
    ),
  )

  it.effect(
    'combines exact and fuzzy replacements in one original view, including disjoint edits on the same line',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const before = 'outside “kept”  \nunique “left” middle “right”  \r\nexact one\r\n'
          const after = 'outside “kept”  \nunique first middle second\r\nexact two\r\n'
          yield* fs.writeFileString(`${directory}/file`, before)
          const saved = yield* invoke('edit', {
            path: 'file',
            edits: [
              change('unique "left"', 'unique first'),
              change('middle "right"', 'middle second'),
              change('exact one', 'exact two'),
            ],
          }).pipe(Effect.provide(handlers(directory)), Effect.flatMap(outcome))
          assert.deepStrictEqual(
            yield* fs.readFile(`${directory}/file`),
            new TextEncoder().encode(after),
          )
          yield* verifyPatch(saved.path, before, after, saved.patch)
          assert.strictEqual(saved.firstChangedLine, 2)
        }),
      ),
  )

  it.effect(
    'rejects no-op edits in mixed exact/fuzzy batches and normalized overlapping matches',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const before = 'unique “left”\nkeep\n'
          yield* fs.writeFileString(`${directory}/file`, before)
          yield* Effect.gen(function* () {
            const noOp = yield* invoke('edit', {
              path: 'file',
              edits: [change('unique "left"', 'changed'), change('keep', 'keep')],
            }).pipe(Effect.flatMap(failure))
            assert.strictEqual(noOp.reason, 'noOp')
            const overlap = yield* invoke('edit', {
              path: 'file',
              edits: [change('unique "left"', 'changed'), change('left', 'other')],
            }).pipe(Effect.flatMap(failure))
            assert.strictEqual(overlap.reason, 'overlap')
          }).pipe(Effect.provide(handlers(directory)))
          assert.deepStrictEqual(
            yield* fs.readFile(`${directory}/file`),
            new TextEncoder().encode(before),
          )
        }),
      ),
  )

  it.effect(
    'failed staging cannot change the target, while direct backend failures report unknown partial outcomes and release permits',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          yield* fs.writeFileString(`${directory}/file`, 'before')
          let failing = true
          const observed = Layer.succeed(FileSystem.FileSystem, {
            ...fs,
            writeFile: (name, bytes, options) =>
              Effect.gen(function* () {
                if (failing) {
                  yield* fs.writeFileString(name, 'partial')
                  return yield* denied('writeFile')
                }
                yield* fs.writeFile(name, bytes, options)
              }),
          }).pipe(Layer.provideMerge(NodePath.layer))
          for (const writeMode of ['rename', 'direct'] as const) {
            yield* fs.writeFileString(`${directory}/file`, 'before')
            failing = true
            yield* Effect.gen(function* () {
              const failed = yield* invoke('edit', {
                path: 'file',
                edits: [change('before', 'after')],
              }).pipe(Effect.flatMap(failure))
              assert.strictEqual(failed.reason, 'filesystem')
              assert.strictEqual(
                yield* fs.readFileString(`${directory}/file`),
                writeMode === 'rename' ? 'before' : 'partial',
              )
              assert.deepStrictEqual(yield* fs.readDirectory(directory), ['file'])
              failing = false
              yield* invoke('write', { path: 'file', content: 'recovered' })
              assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'recovered')
            }).pipe(Effect.provide(handlers(directory, { writeMode }, observed)))
          }
        }),
      ),
  )

  it.effect('interrupts a queued edit alias without leaking its permit reference', () =>
    run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* root
        yield* fs.writeFileString(`${directory}/file`, 'before')
        yield* fs.symlink(`${directory}/file`, `${directory}/alias`)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const resolved = yield* Deferred.make<void>()
        let first = true
        const observed = Layer.succeed(FileSystem.FileSystem, {
          ...fs,
          realPath: (name) =>
            fs
              .realPath(name)
              .pipe(
                Effect.tap(() =>
                  name.endsWith('/alias') ? Deferred.succeed(resolved, undefined) : Effect.void,
                ),
              ),
          writeFile: (name, bytes, options) =>
            Effect.gen(function* () {
              if (first) {
                first = false
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
              }
              yield* fs.writeFile(name, bytes, options)
            }),
        }).pipe(Layer.provideMerge(NodePath.layer))
        yield* Effect.gen(function* () {
          const writer = yield* invoke('write', { path: 'file', content: 'written' }).pipe(
            Effect.forkChild,
          )
          yield* Deferred.await(entered)
          const editor = yield* invoke('edit', {
            path: 'alias',
            edits: [change('written', 'cancelled')],
          }).pipe(Effect.forkChild)
          yield* Deferred.await(resolved)
          yield* Fiber.interrupt(editor)
          const exit = yield* Fiber.await(editor)
          assert.isTrue(Exit.isFailure(exit))
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(writer)
          yield* invoke('edit', { path: 'file', edits: [change('written', 'successor')] }).pipe(
            Effect.flatMap(outcome),
          )
          assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'successor')
        }).pipe(Effect.provide(handlers(directory, {}, observed)))
      }),
    ),
  )

  it.effect(
    'matches disjoint edits against the original, preserves BOM/CRLF and returns an applicable patch',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const before = '\uFEFFalpha\r\nbeta\r\ngamma\r\n'
          yield* fs.writeFileString(`${directory}/file`, before)
          const receipt = yield* invoke('edit', {
            path: '@file',
            edits: [change('alpha', 'beta'), change('beta', 'delta')],
          }).pipe(Effect.provide(handlers(directory)))
          const saved = yield* outcome(receipt)
          const after = '\uFEFFbeta\r\ndelta\r\ngamma\r\n'
          assert.deepStrictEqual(
            yield* fs.readFile(`${directory}/file`),
            new TextEncoder().encode(after),
          )
          yield* verifyPatch(saved.path, before, after, saved.patch)
          assert.strictEqual(saved.firstChangedLine, 1)
          assert.isFalse(saved.fuzzy)
          assert.strictEqual(saved.replacements, 2)
          assert.strictEqual(saved.bytesWritten, new TextEncoder().encode(after).length)
          assert.include(saved.diff, '+1 \uFEFFbeta')
          assert.deepStrictEqual(receipt.details, {
            diff: saved.diff,
            patch: saved.patch,
            firstChangedLine: 1,
          })
          assert.strictEqual(Tool.policy(Edit.tool).replay, 'unsafe')
        }),
      ),
  )

  it.effect(
    'fuzzy normalization rewrites only touched lines, including duplicate normalized line preservation',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const before =
            '\uFEFFoutside “kept”  \r\nunique Ａ “quote”—value\u00a0 \r\nrepeat “x”  \nrepeat "x"\r\nlast\r'
          const after =
            '\uFEFFoutside “kept”  \r\nunique B "quote"-value\r\nrepeat “x”  \nrepeat "x"\r\nlast\r'
          yield* fs.writeFileString(`${directory}/file`, before)
          const saved = yield* invoke('edit', {
            path: 'file',
            edits: [change('unique A "quote"-value', 'unique B "quote"-value')],
          }).pipe(Effect.provide(handlers(directory)), Effect.flatMap(outcome))
          assert.deepStrictEqual(
            yield* fs.readFile(`${directory}/file`),
            new TextEncoder().encode(after),
          )
          yield* verifyPatch(saved.path, before, after, saved.patch)
          assert.strictEqual(saved.firstChangedLine, 2)
          assert.isTrue(saved.fuzzy)
          assert.include(saved.diff, '-2 unique Ａ “quote”—value')
        }),
      ),
  )

  it.effect('preserves mixed untouched endings for exact multiline edits and EOF patches', () =>
    run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* root
        const scenarios = [
          {
            before: 'head\r\na\nb\r\ntail',
            edits: [change('a\nb', 'c\nd')],
            after: 'head\r\nc\r\nd\r\ntail',
            first: 2,
          },
          { before: 'head\nlast', edits: [change('last', 'end')], after: 'head\nend', first: 2 },
          { before: 'head\nlast\n', edits: [change('last\n', '')], after: 'head\n', first: 2 },
          { before: 'head', edits: [change('head', '')], after: '', first: 1 },
          { before: 'one\rtwo\r', edits: [change('two', 'new')], after: 'one\rnew\r', first: 2 },
        ]
        for (const scenario of scenarios) {
          yield* fs.writeFileString(`${directory}/file`, scenario.before)
          const saved = yield* invoke('edit', { path: 'file', edits: scenario.edits }).pipe(
            Effect.provide(handlers(directory)),
            Effect.flatMap(outcome),
          )
          assert.deepStrictEqual(
            yield* fs.readFile(`${directory}/file`),
            new TextEncoder().encode(scenario.after),
          )
          yield* verifyPatch(saved.path, scenario.before, scenario.after, saved.patch)
          assert.strictEqual(saved.firstChangedLine, scenario.first)
        }
      }),
    ),
  )

  it.effect(
    'rejects every invalid plan before any direct or staging write and leaves bytes untouched',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const before = 'aaaa\nfirst\nsecond\nquotes “x”\nquotes "x"\n'
          yield* fs.writeFileString(`${directory}/file`, before)
          let writes = 0
          let staging = 0
          const observed = Layer.succeed(FileSystem.FileSystem, {
            ...fs,
            writeFile: (...args) =>
              Effect.suspend(() => {
                writes++
                return fs.writeFile(...args)
              }),
            makeTempDirectoryScoped: (...args) =>
              Effect.suspend(() => {
                staging++
                return fs.makeTempDirectoryScoped(...args)
              }),
          }).pipe(Layer.provideMerge(NodePath.layer))
          const cases = [
            { edits: [change('first', 'good'), change('missing', 'bad')], reason: 'missing' },
            { edits: [change('aa', 'b')], reason: 'ambiguous' },
            {
              edits: [change('first\nsecond', 'combined'), change('second', 'bad')],
              reason: 'overlap',
            },
            { edits: [change('', 'bad')], reason: 'emptyOldText' },
            { edits: [change('   ', 'bad')], reason: 'emptyOldText' },
            { edits: [change('first', 'first')], reason: 'noOp' },
            { edits: [change('quotes "x"', 'bad')], reason: 'ambiguous' },
          ]
          for (const writeMode of ['direct', 'rename'] as const) {
            const program = Effect.gen(function* () {
              for (const scenario of cases) {
                const error = yield* invoke('edit', { path: 'file', edits: scenario.edits }).pipe(
                  Effect.flatMap(failure),
                )
                assert.strictEqual(error.reason, scenario.reason)
                assert.deepStrictEqual(
                  yield* fs.readFile(`${directory}/file`),
                  new TextEncoder().encode(before),
                )
              }
            })
            yield* program.pipe(Effect.provide(handlers(directory, { writeMode }, observed)))
          }
          assert.strictEqual(writes, 0)
          assert.strictEqual(staging, 0)
        }),
      ),
  )

  it.effect(
    'repairs legacy, object and JSON-string forms at public invocation without modifying arguments',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const forms: Schema.JsonObject[] = [
            { path: 'file', oldText: 'first', newText: 'one' },
            { path: 'file', edits: change('first', 'one') },
            { path: 'file', edits: JSON.stringify(change('first', 'one')) },
            { path: 'file', edits: JSON.stringify([change('first', 'one')]) },
            { path: 'file', edits: [change('first', 'one')], oldText: 'second', newText: 'two' },
          ]
          for (const args of forms) {
            yield* fs.writeFileString(`${directory}/file`, 'first\nsecond\n')
            const copy = structuredClone(args)
            const saved = yield* invoke('edit', args).pipe(
              Effect.provide(handlers(directory)),
              Effect.flatMap(outcome),
            )
            assert.deepStrictEqual(args, copy)
            assert.strictEqual(saved.replacements, args.oldText === 'second' ? 2 : 1)
            assert.strictEqual(
              yield* fs.readFileString(`${directory}/file`),
              args.oldText === 'second' ? 'one\ntwo\n' : 'one\nsecond\n',
            )
          }
          for (const args of [
            { path: 'file', edits: [] },
            { path: 'file' },
            { path: 'file', edits: 'not json' },
            { path: 'file', edits: JSON.stringify([{ oldText: 4, newText: 'x' }]) },
            { path: 'file', edits: [change('one', 'x')], oldText: 'second' },
            { path: 'file', oldText: 'one', newText: 4 },
          ]) {
            const result = yield* invoke('edit', args).pipe(
              Effect.provide(handlers(directory)),
              Effect.result,
            )
            assert.isTrue(Result.isFailure(result))
          }
          assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'one\ntwo\n')
        }),
      ),
  )

  it.effect(
    'rejects invalid UTF-8, surrogate arguments, directories and missing files without mutation',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          const bytes = new Uint8Array([0xff, 0xfe, 0x61])
          yield* fs.writeFile(`${directory}/file`, bytes)
          yield* fs.writeFileString(`${directory}/valid`, 'a')
          yield* Effect.gen(function* () {
            for (const [path, edits, reason] of [
              ['file', [change('a', 'b')], 'encoding'],
              ['valid', [change('a', '\ud800')], 'encoding'],
              ['.', [change('a', 'b')], 'notFile'],
              ['missing', [change('a', 'b')], 'filesystem'],
            ] as const) {
              const error = yield* invoke('edit', { path, edits: [...edits] }).pipe(
                Effect.flatMap(failure),
              )
              assert.strictEqual(error.reason, reason)
            }
          }).pipe(Effect.provide(handlers(directory)))
          assert.deepStrictEqual(yield* fs.readFile(`${directory}/file`), bytes)
          assert.strictEqual(yield* fs.readFileString(`${directory}/valid`), 'a')
        }),
      ),
  )

  it.effect(
    'stages rename replacements, preserves mode and cleans temporary paths after success or failure',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          yield* fs.writeFileString(`${directory}/file`, 'before')
          yield* fs.chmod(`${directory}/file`, 0o640)
          let failRename = true
          const observed = Layer.succeed(FileSystem.FileSystem, {
            ...fs,
            rename: (from, to) =>
              failRename ? Effect.fail(denied('rename')) : fs.rename(from, to),
          }).pipe(Layer.provideMerge(NodePath.layer))
          yield* Effect.gen(function* () {
            const error = yield* invoke('edit', {
              path: 'file',
              edits: [change('before', 'after')],
            }).pipe(Effect.flatMap(failure))
            assert.strictEqual(error.reason, 'filesystem')
            assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'before')
            assert.deepStrictEqual(yield* fs.readDirectory(directory), ['file'])
            failRename = false
            const saved = yield* invoke('edit', {
              path: 'file',
              edits: [change('before', 'after')],
            }).pipe(Effect.flatMap(outcome))
            assert.strictEqual(saved.writeMode, 'rename')
            assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'after')
            assert.strictEqual((yield* fs.stat(`${directory}/file`)).mode & 0o777, 0o640)
            assert.deepStrictEqual(yield* fs.readDirectory(directory), ['file'])
          }).pipe(Effect.provide(handlers(directory, { writeMode: 'rename' }, observed)))
        }),
      ),
  )

  it.effect(
    'interruption during staging cleans resources and releases the shared permit for write',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          yield* fs.writeFileString(`${directory}/file`, 'before')
          const entered = yield* Deferred.make<void>()
          const observed = Layer.succeed(FileSystem.FileSystem, {
            ...fs,
            writeFile: (name, bytes, options) =>
              name.includes('.coding-edit-')
                ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
                : fs.writeFile(name, bytes, options),
          }).pipe(Layer.provideMerge(NodePath.layer))
          yield* Effect.gen(function* () {
            const fiber = yield* invoke('edit', {
              path: 'file',
              edits: [change('before', 'after')],
            }).pipe(Effect.forkChild)
            yield* Deferred.await(entered)
            yield* Fiber.interrupt(fiber)
            const exit = yield* Fiber.await(fiber)
            assert.isTrue(Exit.isFailure(exit))
            assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'before')
            assert.deepStrictEqual(yield* fs.readDirectory(directory), ['file'])
            yield* invoke('write', { path: 'file', content: 'successor' })
            assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'successor')
          }).pipe(Effect.provide(handlers(directory, { writeMode: 'rename' }, observed)))
        }),
      ),
  )

  it.effect(
    'serializes lexical and symlink write/edit aliases while disjoint paths remain concurrent',
    () =>
      run(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* root
          yield* fs.writeFileString(`${directory}/file`, 'before')
          yield* fs.writeFileString(`${directory}/other`, 'other')
          yield* fs.symlink(`${directory}/file`, `${directory}/alias`)
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const aliasResolved = yield* Deferred.make<void>()
          let targetWrites = 0
          const observed = Layer.succeed(FileSystem.FileSystem, {
            ...fs,
            realPath: (name) =>
              fs
                .realPath(name)
                .pipe(
                  Effect.tap(() =>
                    name.endsWith('/alias')
                      ? Deferred.succeed(aliasResolved, undefined)
                      : Effect.void,
                  ),
                ),
            writeFile: (name, bytes, options) =>
              Effect.gen(function* () {
                if (name === `${directory}/file`) {
                  targetWrites++
                  if (targetWrites === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                  }
                }
                yield* fs.writeFile(name, bytes, options)
              }),
          }).pipe(Layer.provideMerge(NodePath.layer))
          yield* Effect.gen(function* () {
            const writer = yield* invoke('write', {
              path: './nested/../file',
              content: 'written',
            }).pipe(Effect.forkChild)
            yield* Deferred.await(entered)
            const editor = yield* invoke('edit', {
              path: 'alias',
              edits: [change('written', 'edited')],
            }).pipe(Effect.forkChild)
            yield* Deferred.await(aliasResolved)
            yield* invoke('edit', { path: 'other', edits: [change('other', 'independent')] }).pipe(
              Effect.flatMap(outcome),
            )
            assert.strictEqual(targetWrites, 1)
            assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'before')
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(writer)
            yield* Fiber.join(editor).pipe(Effect.flatMap(outcome))
            assert.strictEqual(yield* fs.readFileString(`${directory}/file`), 'edited')
            // A failed validation does not retain the lock or partially write.
            yield* invoke('edit', { path: 'alias', edits: [change('missing', 'x')] }).pipe(
              Effect.flatMap(failure),
            )
            yield* invoke('edit', { path: 'file', edits: [change('edited', 'done')] }).pipe(
              Effect.flatMap(outcome),
            )
          }).pipe(Effect.provide(handlers(directory, {}, observed)))
        }),
      ),
  )

  it.effect('independent bindings do not leak roots or block another backend', () =>
    run(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const a = yield* root
        const b = yield* root
        yield* fs.writeFileString(`${a}/file`, 'first')
        yield* fs.writeFileString(`${b}/file`, 'second')
        const entered = yield* Deferred.make<void>()
        const observed = Layer.succeed(FileSystem.FileSystem, {
          ...fs,
          writeFile: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
        }).pipe(Layer.provideMerge(NodePath.layer))
        const stalled = yield* invoke('edit', {
          path: 'file',
          edits: [change('first', 'stalled')],
        }).pipe(Effect.provide(handlers(a, {}, observed)), Effect.forkChild)
        yield* Deferred.await(entered)
        yield* invoke('edit', { path: 'file', edits: [change('second', 'independent')] }).pipe(
          Effect.provide(handlers(b)),
          Effect.flatMap(outcome),
        )
        assert.strictEqual(yield* fs.readFileString(`${a}/file`), 'first')
        assert.strictEqual(yield* fs.readFileString(`${b}/file`), 'independent')
        // Even the same canonical path on another backend/coordinator remains independent.
        yield* invoke('edit', { path: 'file', edits: [change('first', 'recovered')] }).pipe(
          Effect.provide(handlers(a)),
          Effect.flatMap(outcome),
        )
        yield* Fiber.interrupt(stalled)
      }),
    ),
  )
})
