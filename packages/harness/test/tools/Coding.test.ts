import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import { Env, ExecutionError, ExecutionSpawnError, ExecutionUnknown } from '../../src/Env.ts'
import { ToolCall } from '../../src/Invocation.ts'
import { Executor, layer as executorLayer } from '../../src/Executor.ts'
import * as Agent from '../../src/Agent.ts'
import * as Model from '../../src/Model.ts'
import * as Registry from '../../src/Registry.ts'
import * as Tools from '../../src/tools/index.ts'
import * as Read from '../../src/tools/Read.ts'
import * as Write from '../../src/tools/Write.ts'
import * as Edit from '../../src/tools/Edit.ts'
import * as Result from 'effect/Result'
import * as EditDiff from '../../src/tools/EditDiff.ts'
import * as Bash from '../../src/tools/Bash.ts'
import * as Truncate from '../../src/tools/Truncate.ts'
import * as Image from '../../src/tools/Image.ts'
import { withEnv, message, recording } from './Helpers.ts'

describe('native coding tools', () => {
  it.effect(
    'read returns only selected text, continuation/truncation diagnostics and oversized character-boundary prefix',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.writeFile('text', 'zero\none\ntwo\n')
          const result = yield* Read.handler({ path: 'text', offset: 2, limit: 1 })
          assert.strictEqual(message(result), 'one')
          assert.strictEqual(result.diagnostics?.[0]?.kind, 'continuation')
          assert.strictEqual(message(yield* Read.handler({ path: 'text', offset: 4 })), '')
          assert.match(
            (yield* Effect.flip(Read.handler({ path: 'text', offset: 5 }))).message,
            /beyond end/,
          )
          yield* env.writeFile('big', Array.from({ length: 2100 }, (_, i) => String(i)).join('\n'))
          const lines = yield* Read.handler({ path: 'big' })
          assert.strictEqual(message(lines).split('\n').length, 2000)
          assert.strictEqual(lines.diagnostics?.[0]?.kind, 'truncated')
          yield* env.writeFile('long', '😀'.repeat(20000))
          const long = yield* Read.handler({ path: 'long' })
          assert.strictEqual(new TextEncoder().encode(message(long)).length, 51200)
          assert.match(long.diagnostics?.[0]?.message ?? '', /tail -c/)
        }),
      ),
  )
  it.effect(
    'bounded text selection equals whole decode/slice/truncate for fractional/negative limits, CRLF, invalid UTF8 and BOM',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const samples = [
            new TextEncoder().encode(''),
            new TextEncoder().encode('\ufefffirst\r\nsecond\n\ufeffthird\n'),
            new Uint8Array([0xf0, 0x9f, 10, 0xc3, 0xa9, 10, 0xef, 0xbb, 0xbf]),
            new TextEncoder().encode('x'.repeat(60000) + '\ny\nz'),
          ]
          for (const bytes of samples) {
            yield* env.writeFile('selection', bytes)
            const all = new TextDecoder().decode(bytes).split('\n')
            for (const offset of [-Infinity, NaN, -2, 0, 1, 1.5, 2, 3])
              for (const limit of [
                undefined,
                -Infinity,
                NaN,
                -3,
                -1,
                0,
                0.5,
                1,
                1.5,
                5,
                Infinity,
              ]) {
                const start = offset ? Math.max(0, offset - 1) : 0
                if (start >= all.length) continue
                const selected = all
                  .slice(
                    start,
                    limit === undefined ? undefined : Math.min(start + limit, all.length),
                  )
                  .join('\n')
                const bounded = Truncate.truncateHead(selected)
                let expected = bounded.content
                if (bounded.firstLineExceedsLimit) {
                  expected = Number.isInteger(start)
                    ? new TextDecoder().decode(
                        new TextEncoder().encode(all[start] ?? '').subarray(0, 51200),
                      )
                    : ''
                }
                const actual = yield* Read.handler({
                  path: 'selection',
                  offset,
                  ...(limit === undefined ? {} : { limit }),
                })
                assert.strictEqual(message(actual), expected, `offset ${offset} limit ${limit}`)
              }
          }
        }),
      ),
  )
  it.effect('images are rejected by content; late APNG and JPEG-LS are decoded as text', () =>
    withEnv(
      Effect.gen(function* () {
        const env = yield* Env
        const png = new Uint8Array([
          137,
          80,
          78,
          71,
          13,
          10,
          26,
          10,
          0,
          0,
          0,
          13,
          73,
          72,
          68,
          82,
          ...Array.from({ length: 17 }, () => 0),
          0,
          0,
          0,
          0,
          73,
          68,
          65,
          84,
          0,
          0,
          0,
          0,
        ])
        const bmp = new Uint8Array(30)
        const view = new DataView(bmp.buffer)
        bmp.set(new TextEncoder().encode('BM'))
        view.setUint32(2, 100, true)
        view.setUint32(10, 54, true)
        view.setUint32(14, 40, true)
        view.setUint16(26, 1, true)
        view.setUint16(28, 24, true)
        assert.strictEqual(Image.detectSupportedImageMimeType(bmp), 'image/bmp')
        const invalidBmp = bmp.slice()
        invalidBmp[26] = 2
        assert.strictEqual(Image.detectSupportedImageMimeType(invalidBmp), undefined)
        for (const truncated of [
          bmp.subarray(0, 25),
          png.subarray(0, 15),
          new Uint8Array([255, 216]),
          new TextEncoder().encode('GIF89'),
          new TextEncoder().encode('RIFF0000WEB'),
        ])
          assert.strictEqual(Image.detectSupportedImageMimeType(truncated), undefined)
        for (const bytes of [
          bmp,
          png,
          new Uint8Array([255, 216, 255, 224]),
          new TextEncoder().encode('GIF89a'),
          new TextEncoder().encode('RIFF0000WEBP'),
        ]) {
          yield* env.writeFile('image.txt', bytes)
          const result = yield* Read.handler({ path: 'image.txt' })
          assert.strictEqual(result.isError, true)
          assert.deepStrictEqual(result.content, [])
          assert.strictEqual(result.diagnostics?.[0]?.kind, 'unsupported_image')
        }
        const apng = png.slice()
        apng.set(new TextEncoder().encode('acTL'), 37)
        yield* env.writeFile('image.txt', apng)
        assert.notStrictEqual((yield* Read.handler({ path: 'image.txt' })).isError, true)
        assert.strictEqual(
          Image.detectSupportedImageMimeType(new Uint8Array([255, 216, 255, 247])),
          undefined,
        )
      }),
    ),
  )
  it.effect(
    'tool path repairs normalize @/Unicode spaces and read tries NFD/curly apostrophe variants',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* Write.handler({ path: '@one\u00a0file', content: 'ascii' })
          assert.strictEqual(yield* env.readTextFile('one file'), 'ascii')
          yield* env.writeFile('café'.normalize('NFD') + '’s', 'variant')
          assert.strictEqual(message(yield* Read.handler({ path: "café's" })), 'variant')
        }),
      ),
  )
  it.effect(
    'write creates parents; edits preserve BOM/CRLF, original simultaneous targets and both diff formats',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* Write.handler({ path: 'nested/file', content: '\ufeffone\r\ntwo\r\nthree\r\n' })
          const result = yield* Edit.handler({
            path: 'nested/file',
            edits: [
              { oldText: 'one', newText: 'two' },
              { oldText: 'two', newText: 'second' },
            ],
          })
          assert.strictEqual(
            yield* env.readTextFile('nested/file'),
            '\ufefftwo\r\nsecond\r\nthree\r\n',
          )
          const details = yield* Schema.decodeUnknownEffect(
            Schema.Struct({
              diff: Schema.String,
              patch: Schema.String,
              firstChangedLine: Schema.Int,
            }),
          )(result.details)
          assert.match(details.diff, /-1 one/)
          assert.match(details.patch, /--- nested\/file/)
          assert.strictEqual(details.firstChangedLine, 1)
        }),
      ),
  )
  it.effect(
    'edit repairs shapes without mutation; empty/missing/ambiguous/overlapping/nochange errors leave file intact',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          yield* env.writeFile('file', 'abc abc\nother')
          for (const input of [
            { path: 'file', edits: '{"oldText":"other","newText":"new"}' },
            { path: 'file', edits: { oldText: 'other', newText: 'new' } },
            { path: 'file', oldText: 'other', newText: 'new' },
          ]) {
            const before = structuredClone(input)
            const repaired = yield* Edit.repair(input)
            assert.deepStrictEqual(input, before)
            assert.deepStrictEqual(
              (yield* Schema.decodeUnknownEffect(Edit.Parameters)(repaired)).edits,
              [{ oldText: 'other', newText: 'new' }],
            )
          }
          for (const edits of [
            [],
            [{ oldText: '', newText: 'new' }],
            [{ oldText: 'abc', newText: 'new' }],
            [{ oldText: 'missing', newText: 'new' }],
            [{ oldText: 'other', newText: 'other' }],
            [
              { oldText: 'abc abc', newText: 'new' },
              { oldText: 'abc abc\nother', newText: 'new' },
            ],
          ])
            assert.strictEqual(
              (yield* Effect.exit(Edit.handler({ path: 'file', edits })))._tag,
              'Failure',
            )
          assert.strictEqual(yield* env.readTextFile('file'), 'abc abc\nother')
        }),
      ),
  )
  it('fuzzy replacements preserve unrelated original line blocks and line-numbered diff context', () => {
    const original = 'unchanged — curly   \n“target”\nkeep\u00a0space  \nlast'
    const changed = Result.getOrThrow(
      EditDiff.applyEditsToNormalizedContent(
        original,
        [{ oldText: '"target"', newText: 'new' }],
        'file',
      ),
    )
    assert.strictEqual(changed.newContent, 'unchanged — curly   \nnew\nkeep\u00a0space  \nlast')
    assert.match(
      EditDiff.generateUnifiedPatch('file', changed.baseContent, changed.newContent),
      /@@ -1,4 \+1,4 @@/,
    )
  })
  it.effect(
    'canonical and symlink paths serialize edits across shared namespace environment objects',
    () =>
      withEnv(
        Effect.gen(function* () {
          const env = yield* Env
          const fs = yield* FileSystem.FileSystem
          yield* env.writeFile('real', 'start')
          yield* fs.symlink(env.path.join(env.cwd, 'real'), env.path.join(env.cwd, 'alias'))
          const first = yield* Edit.handler({
            path: 'real',
            edits: [{ oldText: 'start', newText: 'middle' }],
          }).pipe(Effect.forkChild)
          const second = yield* Edit.handler({
            path: 'alias',
            edits: [{ oldText: 'middle', newText: 'end' }],
          }).pipe(Effect.forkChild)
          yield* Fiber.join(first)
          yield* Fiber.join(second)
          assert.strictEqual(yield* env.readTextFile('real'), 'end')
        }),
      ),
  )
  it.effect('interruption retains mutation permit until the admitted write actually settles', () =>
    withEnv(
      Effect.gen(function* () {
        const real = yield* Env
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const order = yield* Ref.make<ReadonlyArray<string>>([])
        const env: Env['Service'] = {
          ...real,
          writeFile: (path, content) =>
            Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Ref.update(order, (values) => [...values, String(content)])
                if (content === 'first') {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }
                yield* real.writeFile(path, content)
              }),
            ),
        }
        const first = yield* Write.handler({ path: 'file', content: 'first' }).pipe(
          Effect.provideService(Env, env),
          Effect.forkChild,
        )
        yield* Deferred.await(entered)
        const interrupted = yield* Fiber.interrupt(first).pipe(Effect.forkChild)
        const second = yield* Write.handler({ path: 'file', content: 'second' }).pipe(
          Effect.provideService(Env, env),
          Effect.forkChild,
        )
        yield* Effect.yieldNow
        assert.deepStrictEqual(yield* Ref.get(order), ['first'])
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(interrupted)
        yield* Fiber.join(second)
        assert.strictEqual(yield* real.readTextFile('file'), 'second')
      }),
    ),
  )
  it.live(
    'bash streams partial failure, prefixes/preparation and scoped native toolkit execution yields full-output diagnostics',
    () =>
      withEnv(
        Effect.gen(function* () {
          const captured = yield* recording
          const failure = yield* Effect.flip(
            Bash.handler({
              commandPrefix: 'printf prefix;',
              prepare: (execution) =>
                Effect.sync(() => {
                  execution.command += '; printf "$MARK"; exit 2'
                  execution.env = { MARK: 'value', PATH: '/usr/bin:/bin' }
                  execution.inheritEnv = false
                }),
            })({ command: 'printf body' }).pipe(Effect.provideService(ToolCall, captured.api)),
          )
          assert.match(failure.message, /code 2/)
          assert.strictEqual(yield* Ref.get(captured.output), 'prefixbodyvalue')
          const extension = yield* Tools.make()
          const executor = yield* Executor.pipe(
            Effect.provide(
              executorLayer.pipe(
                Layer.provide(Layer.mergeAll(Registry.layer([extension]), Model.layer([]))),
              ),
            ),
          )
          const agent = yield* executor.resolve(
            {},
            Agent.settings({ progress: { outputIntervalMs: 0 } }),
          )
          const intent = yield* executor.prepareTool(agent, {
            id: 'spill',
            name: 'bash',
            args: { command: "printf '%060000d' 0" },
          })
          const result = yield* executor.tool(intent, agent)
          assert.strictEqual(result.outcome, 'completed')
          assert.strictEqual(new TextEncoder().encode(message(result.result)).length, 51200)
          const spill = result.result.diagnostics?.find(
            (diagnostic) => diagnostic.kind === 'full_output',
          )
          assert.isDefined(spill)
          const data = yield* Schema.decodeUnknownEffect(Schema.Struct({ path: Schema.String }))(
            spill?.detail,
          )
          assert.strictEqual((yield* (yield* Env).readBinaryFile(data.path)).length, 60000)
          yield* (yield* Env).remove(data.path)
        }),
      ),
  )
  it.effect(
    'PowerShell uses exact literal argv, falls back only on spawn failure, forwards output-window and skipped counts',
    () =>
      withEnv(
        Effect.gen(function* () {
          const real = yield* Env
          const calls = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([])
          const captured = yield* recording
          const skips = yield* Ref.make<unknown>(undefined)
          const env: Env['Service'] = {
            ...real,
            exec: (command, options) =>
              Effect.gen(function* () {
                if (typeof command === 'string')
                  return yield* new ExecutionError({
                    reason: new ExecutionUnknown({ message: 'Expected argv' }),
                  })
                yield* Ref.update(calls, (values) => [...values, command])
                if (command[0] === 'missing')
                  return yield* new ExecutionError({
                    reason: new ExecutionSpawnError({ message: 'missing' }),
                  })
                assert.strictEqual(options?.window?.maxBytes, 3)
                if (options?.onOutput !== undefined)
                  yield* options.onOutput('out', {
                    stream: 'stdout',
                    skipped: { bytes: 10, newlines: 1, endsWithNewline: true },
                  })
                return { exitCode: 0 }
              }),
          }
          const api = {
            ...captured.api,
            output: (text: string | Uint8Array, skipped?: import('../../src/Output.ts').Skip) =>
              Ref.set(skips, skipped).pipe(Effect.andThen(captured.api.output(text, skipped))),
            outputWindow: { maxBytes: 3, maxLines: 2, minIntervalMs: 5, bytesPerSecond: 10 },
          }
          yield* Bash.powerShellHandler({ programs: ['missing', 'pwsh'] })({
            command: '$literal; "quoted"',
          }).pipe(Effect.provideService(Env, env), Effect.provideService(ToolCall, api))
          const values = yield* Ref.get(calls)
          assert.strictEqual(values.length, 2)
          assert.deepStrictEqual(values[1]?.slice(0, 6), [
            'pwsh',
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-Command',
          ])
          assert.strictEqual(values[1]?.at(-1)?.endsWith('$literal; "quoted"'), true)
          assert.strictEqual(yield* Ref.get(captured.output), 'out')
          assert.deepStrictEqual(yield* Ref.get(skips), {
            bytes: 10,
            newlines: 1,
            endsWithNewline: true,
          })
        }),
      ),
  )
})
