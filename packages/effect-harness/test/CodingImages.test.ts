import { assert, describe, it } from '@effect/vitest'
import sharp from 'sharp'
import { execFileSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as ByteSize from 'effect/ByteSize'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as FileReader from 'effect-harness/FileReader'
import * as Images from 'effect-harness/ImagePreparation'
import * as Record from 'effect-harness/Record'
import { ToolExecution } from 'effect-harness/ToolExecution'
import type * as ToolResult from 'effect-harness/ToolResult'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ImageRead from '../src/coding/ImageRead.js'
import * as Policy from '../src/coding/ImageReadPolicy.js'
import * as Read from '../src/coding/Read.js'
import * as NodeImages from '../src/images-node/index.js'
import * as WasmImages from '../src/images-wasm/index.js'
import { fixture, limits, rgba } from './fixtures/images-native/Conformance.js'

const native = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const tools = Read.withImages({ maxSourceBytes: 1024 * 1024 })
const vision: Policy.Policy = { vision: true, limits }
const execution = (callId = 'images') =>
  Layer.succeed(ToolExecution, {
    conversationId: Schema.decodeSync(Record.ConversationId)(1),
    taskId: Schema.decodeSync(Record.TaskId)(1),
    callId,
    snapshot: () => Effect.die('Unexpected snapshot'),
    watch: () => Stream.die('Unexpected watch'),
    commit: () => Effect.die('Unexpected commit'),
    output: () => Effect.die('Unexpected output'),
    details: () => Effect.die('Unexpected details'),
    diagnostic: () => Effect.die('Unexpected diagnostic'),
  })
const binding = (root: string) => ToolkitFileSystem.layer({ root }).pipe(Layer.provide(native))
const files = (root: string) => FileReader.layer.pipe(Layer.provideMerge(binding(root)))
const handlers = (root: string, policy: Policy.Policy = vision, codec = NodeImages.layer) =>
  tools.layer.pipe(Layer.provideMerge(Layer.mergeAll(files(root), codec, Policy.layer(policy))))
const invoke = (args: Read.Parameters, callId = 'images') =>
  Effect.flatMap(tools.toolkit, (ready) =>
    ready.invoke({ id: callId, name: 'read', arguments: args }),
  ).pipe(Effect.provide(execution(callId)))
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(native))
const root = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  return yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
})
const write = Effect.fn(function* (directory: string, name: string, bytes: Uint8Array) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  yield* fs.writeFile(path.join(directory, name), bytes)
})
const image = (result: ToolResult.Result) => {
  assert.isFalse(result.isError)
  return Schema.decodeUnknownEffect(ImageRead.Outcome)(result.structuredOutput)
}
const failure = (result: ToolResult.Result) => {
  assert.isTrue(result.isError)
  assert.strictEqual(result.content[0]?.type, 'text')
  assert.isFalse(result.content.some((part) => part.type === 'file'))
  return Schema.decodeUnknownEffect(Read.ImageFailure)(result.structuredOutput)
}
const imageBytes = (result: ToolResult.Result) => {
  const part = result.content.find((part) => part.type === 'file')
  assert.isDefined(part)
  assert.isTrue(part?.data instanceof Uint8Array)
  return part?.data instanceof Uint8Array ? part.data : new Uint8Array()
}

for (const [adapter, codec] of [
  ['Node', NodeImages.layer],
  ['WASM', WasmImages.layer],
] as const) {
  describe(`${adapter} image Read public handlers`, () => {
    for (const [name, mime] of [
      ['opaque.png', 'image/png'],
      ['opaque.jpg', 'image/jpeg'],
      ['animated.gif', 'image/gif'],
      ['animated.webp', 'image/webp'],
      ['opaque.bmp', 'image/bmp'],
    ] as const) {
      it.effect(`reads real ${name} through bound services`, () =>
        run(
          Effect.gen(function* () {
            const directory = yield* root
            const bytes = fixture(name)
            yield* write(directory, name, bytes)
            const result = yield* invoke({ path: name }).pipe(
              Effect.provide(handlers(directory, vision, codec)),
            )
            const outcome = yield* image(result)
            assert.deepStrictEqual(outcome.original, { width: 3, height: 2 })
            assert.deepStrictEqual(outcome.oriented, outcome.original)
            assert.strictEqual(outcome.fileBytes, bytes.length.toString())
            assert.strictEqual(outcome.base64Bytes, Images.base64Length(imageBytes(result).length))
            const part = result.content.find((part) => part.type === 'file')
            assert.strictEqual(part?.mediaType, outcome.mimeType)
            if (mime === 'image/bmp') {
              assert.strictEqual(outcome.convertedFrom, mime)
              assert.isFalse(outcome.passthrough)
            } else {
              assert.strictEqual(outcome.mimeType, mime)
              assert.deepStrictEqual(imageBytes(result), bytes)
            }
            assert.strictEqual(
              outcome.animation,
              mime === 'image/gif' || mime === 'image/webp' ? 'unchanged' : 'static',
            )
            assert.deepStrictEqual((yield* rgba(imageBytes(result))).info.width, 3)
          }),
        ),
      )
    }
    it.effect('normalizes real orientation and resizes with exact metadata', () =>
      run(
        Effect.gen(function* () {
          const directory = yield* root
          yield* write(directory, 'orientation.png', fixture('orientation-6.png'))
          const result = yield* invoke({ path: 'orientation.png' }).pipe(
            Effect.provide(
              handlers(directory, { vision: true, limits: { ...limits, maxWidth: 1 } }, codec),
            ),
          )
          const outcome = yield* image(result)
          assert.strictEqual(outcome.orientation, 6)
          assert.deepStrictEqual(outcome.original, { width: 3, height: 2 })
          assert.deepStrictEqual(outcome.oriented, { width: 2, height: 3 })
          assert.deepStrictEqual(outcome.dimensions, { width: 1, height: 2 })
          assert.deepStrictEqual(outcome.resized, {
            from: { width: 2, height: 3 },
            to: { width: 1, height: 2 },
          })
          assert.strictEqual((yield* rgba(imageBytes(result))).info.width, 1)
          assert.isTrue(
            result.content.some(
              (part) => part.type === 'text' && part.text.includes('Normalized orientation 6'),
            ),
          )
        }),
      ),
    )
  })
}

it.effect('reads supplied policy each invocation and omits nonvision bytes without preparing', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      yield* write(directory, 'same.png', fixture('opaque.png'))
      let prepared = 0
      const service = Layer.effect(
        Images.ImagePreparation,
        Effect.map(Images.ImagePreparation, (original) => ({
          prepare: (bytes, mime, limits) =>
            original.prepare(bytes, mime, limits).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  prepared++
                }),
              ),
            ),
        })),
      ).pipe(Layer.provide(NodeImages.layer))
      const policy = Layer.succeed(Policy.ImageReadPolicy, {
        current: Effect.map(ToolExecution, (call): Policy.Policy =>
          call.callId === 'text-model' ? { vision: false } : vision,
        ),
      })
      yield* Effect.gen(function* () {
        const omitted = yield* invoke({ path: 'same.png' }, 'text-model')
        assert.isFalse(omitted.isError)
        const placeholder = yield* Schema.decodeUnknownEffect(ImageRead.Placeholder)(
          omitted.structuredOutput,
        )
        assert.strictEqual(placeholder.reason, 'modelDoesNotSupportImages')
        assert.isFalse(omitted.content.some((part) => part.type === 'file'))
        assert.strictEqual(prepared, 0)
        yield* image(yield* invoke({ path: 'same.png' }, 'vision-model'))
        assert.strictEqual(prepared, 1)
        yield* invoke({ path: 'same.png' }, 'text-model')
        assert.strictEqual(prepared, 1)
      }).pipe(
        Effect.provide(
          tools.layer.pipe(Layer.provideMerge(Layer.mergeAll(files(directory), service, policy))),
        ),
      )
    }),
  ),
)

it.effect('fails unsupported, mismatched, malformed, huge-pixel and impossible-budget images', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      const corrupt = fixture('opaque.png')
      corrupt[62] = 0
      corrupt[63] = 0
      yield* write(directory, 'corrupt.png', corrupt)
      yield* write(directory, 'mismatch.jpg', fixture('opaque.png'))
      yield* write(directory, 'fake.png', new TextEncoder().encode('ordinary text'))
      yield* write(directory, 'vector.svg', new TextEncoder().encode('<svg/>'))
      yield* write(directory, 'unsupported', Uint8Array.from([0x49, 0x49, 0x2a, 0]))
      yield* write(directory, 'opaque.png', fixture('opaque.png'))
      for (const [name, reason] of [
        ['corrupt.png', 'preparation'],
        ['mismatch.jpg', 'mimeMismatch'],
        ['fake.png', 'mimeMismatch'],
        ['vector.svg', 'unsupportedImage'],
        ['unsupported', 'unsupportedImage'],
      ] as const) {
        assert.strictEqual(
          (yield* failure(yield* invoke({ path: name }).pipe(Effect.provide(handlers(directory)))))
            .reason,
          reason,
        )
      }
      for (const overridden of [
        { ...limits, maxInputPixels: 5 },
        { ...limits, maxBytes: 4 },
      ]) {
        const result = yield* invoke({ path: 'opaque.png' }).pipe(
          Effect.provide(handlers(directory, { vision: true, limits: overridden })),
        )
        assert.strictEqual((yield* failure(result)).reason, 'preparation')
      }
    }),
  ),
)

it.effect('guards opened source size before full-source reads, with bounded chunks', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      yield* write(directory, 'image.png', fixture('opaque.png'))
      const service = yield* FileReader.FileReader.pipe(Effect.provide(files(directory)))
      let largest = 0
      let allocations = 0
      const observed = Layer.succeed(FileReader.FileReader, {
        open: (path, options) =>
          service.open(path, options).pipe(
            Effect.map((reader) => ({
              stat: reader.stat.pipe(
                Effect.map((info) => ({ ...info, size: ByteSize.bytes(2n ** 60n) })),
              ),
              readAt: (offset, length) => {
                allocations++
                largest = Math.max(largest, length)
                return reader.readAt(offset, length)
              },
            })),
          ),
      })
      const result = yield* invoke({ path: 'image.png' }).pipe(
        Effect.provide(
          tools.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(binding(directory), observed, NodeImages.layer, Policy.layer(vision)),
            ),
          ),
        ),
      )
      assert.strictEqual((yield* failure(result)).reason, 'sourceBudget')
      assert.strictEqual(allocations, 1)
      assert.isAtMost(largest, 32)
      const tiny = Read.withImages({ maxSourceBytes: 32 })
      const rejected = yield* Effect.flatMap(tiny.toolkit, (ready) =>
        ready.invoke({ id: 'tiny', name: 'read', arguments: { path: 'image.png' } }),
      ).pipe(
        Effect.provide(
          Layer.merge(
            execution(),
            tiny.layer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(files(directory), NodeImages.layer, Policy.layer(vision)),
              ),
            ),
          ),
        ),
      )
      assert.strictEqual((yield* failure(rejected)).reason, 'sourceBudget')
    }),
  ),
)

it.effect('rejects unsafe source/text/model numeric budgets', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      yield* write(directory, 'image.png', fixture('opaque.png'))
      for (const value of [0, -1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1, 2 ** 32]) {
        const unsafe = Read.withImages({ maxSourceBytes: value })
        const ready = yield* unsafe.toolkit.pipe(
          Effect.provide(
            unsafe.layer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(files(directory), NodeImages.layer, Policy.layer(vision)),
              ),
            ),
          ),
        )
        const result = yield* ready
          .invoke({ id: 'unsafe', name: 'read', arguments: { path: 'image.png' } })
          .pipe(Effect.provide(execution()))
        assert.strictEqual((yield* failure(result)).reason, 'invalidLimits')
      }
      for (const field of ['maxWidth', 'maxHeight', 'maxBytes', 'maxInputPixels'] as const) {
        for (const value of [0, -1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
          const result = yield* invoke({ path: 'image.png' }).pipe(
            Effect.provide(
              handlers(directory, { vision: true, limits: { ...limits, [field]: value } }),
            ),
          )
          assert.strictEqual((yield* failure(result)).reason, 'preparation')
        }
      }
      for (const input of [
        { path: 'image.png', offset: 0 },
        { path: 'image.png', limit: Number.MAX_SAFE_INTEGER + 1 },
      ]) {
        assert.strictEqual(
          (yield* failure(yield* invoke(input).pipe(Effect.provide(handlers(directory))))).reason,
          'invalidLimits',
        )
      }
    }),
  ),
)

it.effect('checks replacement preparer final base64, MIME and metadata', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      yield* write(directory, 'image.png', fixture('opaque.png'))
      const service = yield* Images.ImagePreparation.pipe(Effect.provide(NodeImages.layer))
      const prepared = yield* service.prepare(fixture('opaque.png'), 'image/png', limits)
      for (const [replacement, reason] of [
        [{ ...prepared, bytes: new Uint8Array(100000) }, 'outputBudget'],
        [{ ...prepared, mimeType: 'image/jpeg' as const }, 'invalidOutput'],
        [{ ...prepared, dimensions: { width: 31, height: 2 } }, 'invalidOutput'],
        [{ ...prepared, original: { width: 1.5, height: 2 } }, 'invalidOutput'],
      ] as const) {
        const codec = Layer.succeed(Images.ImagePreparation, {
          prepare: () => Effect.succeed(replacement),
        })
        assert.strictEqual(
          (yield* failure(
            yield* invoke({ path: 'image.png' }).pipe(
              Effect.provide(handlers(directory, vision, codec)),
            ),
          )).reason,
          reason,
        )
      }
    }),
  ),
)

it.effect('same declarations remain isolated across concurrent bound roots and codecs', () =>
  run(
    Effect.gen(function* () {
      const a = yield* root
      const b = yield* root
      yield* write(a, 'same.png', fixture('orientation-6.png'))
      yield* write(b, 'same.png', fixture('opaque.png'))
      const [left, right] = yield* Effect.all(
        [
          invoke({ path: 'same.png' }).pipe(
            Effect.provide(handlers(a, { vision: true, limits: { ...limits, maxWidth: 1 } })),
          ),
          invoke({ path: 'same.png' }).pipe(Effect.provide(handlers(b, vision, WasmImages.layer))),
        ],
        { concurrency: 'unbounded' },
      )
      assert.deepStrictEqual((yield* image(left)).dimensions, { width: 1, height: 2 })
      assert.deepStrictEqual((yield* image(right)).dimensions, { width: 3, height: 2 })
      assert.notStrictEqual((yield* image(left)).path, (yield* image(right)).path)
    }),
  ),
)

it.effect('image builder preserves text BOM/CRLF pagination and huge-line continuation', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      yield* write(directory, 'text', new TextEncoder().encode('\ufeffone\r\ntwo\r\nthree\n'))
      yield* write(directory, 'huge', new TextEncoder().encode('🙂'.repeat(30000)))
      yield* Effect.gen(function* () {
        const first = yield* invoke({ path: 'text', limit: 1 })
        assert.strictEqual(first.content[0]?.type === 'text' ? first.content[0].text : '', 'one\r')
        const outcome = yield* Schema.decodeUnknownEffect(Read.Outcome)(first.structuredOutput)
        assert.deepStrictEqual(outcome.continuation, { offset: 2 })
        const rest = yield* invoke({ path: 'text', offset: 2 })
        assert.strictEqual(
          rest.content[0]?.type === 'text' ? rest.content[0].text : '',
          'two\r\nthree\n',
        )
        const huge = yield* invoke({ path: 'huge' })
        const capped = yield* Schema.decodeUnknownEffect(Read.Outcome)(huge.structuredOutput)
        assert.strictEqual(capped.outputBytes, Read.maxBytes)
        assert.deepStrictEqual(capped.continuation, {
          offset: 1,
          byteOffset: Read.maxBytes.toString(),
        })
        const continued = yield* invoke({ path: 'huge', ...capped.continuation })
        assert.isFalse(continued.isError)
        assert.strictEqual(
          (yield* failure(yield* invoke({ path: 'text', offset: 99 }))).reason,
          'beyondEOF',
        )
      }).pipe(Effect.provide(handlers(directory)))
    }),
  ),
)

it.effect('retries changed images once on the same native opened identity and then fails', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      const fs = yield* FileSystem.FileSystem
      const filePath = `${directory}/changing.png`
      let changes = 1
      let opens = 0
      let closes = 0
      let flip = false
      yield* fs.writeFile(filePath, fixture('opaque.png'))
      const original = yield* FileReader.FileReader.pipe(Effect.provide(files(directory)))
      const reader = Layer.succeed(FileReader.FileReader, {
        open: (path, options) =>
          Effect.gen(function* () {
            const handle = yield* original.open(path, options)
            opens++
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                closes++
              }),
            )
            return {
              stat: handle.stat,
              readAt: (offset, length) =>
                Effect.gen(function* () {
                  if (offset === 0n && length === 32 && changes > 0) {
                    changes--
                    flip = !flip
                    yield* fs
                      .writeFile(filePath, fixture(flip ? 'orientation-6.png' : 'opaque.png'))
                      .pipe(Effect.orDie)
                  }
                  return yield* handle.readAt(offset, length)
                }),
            }
          }),
      })
      yield* Effect.gen(function* () {
        const stable = yield* invoke({ path: 'changing.png' })
        assert.strictEqual((yield* image(stable)).orientation, 6)
        assert.strictEqual(opens, 1)
        assert.strictEqual(closes, 1)
        changes = 2
        assert.strictEqual(
          (yield* failure(yield* invoke({ path: 'changing.png' }))).reason,
          'changed',
        )
        assert.strictEqual(opens, 2)
        assert.strictEqual(closes, 2)
      }).pipe(
        Effect.provide(
          tools.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(binding(directory), reader, NodeImages.layer, Policy.layer(vision)),
            ),
          ),
        ),
      )
    }),
  ),
)

it.effect('opened image survives path replacement and source chunks remain bounded', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      const fs = yield* FileSystem.FileSystem
      yield* write(directory, 'image.png', fixture('opaque.png'))
      const original = yield* FileReader.FileReader.pipe(Effect.provide(files(directory)))
      let largest = 0
      const reader = Layer.succeed(FileReader.FileReader, {
        open: (path, options) =>
          Effect.gen(function* () {
            const handle = yield* original.open(path, options)
            yield* fs.rename(path, `${path}.moved`).pipe(Effect.orDie)
            yield* fs.writeFile(path, fixture('orientation-6.png')).pipe(Effect.orDie)
            return {
              stat: handle.stat,
              readAt: (offset, length) => {
                largest = Math.max(largest, length)
                return handle.readAt(offset, length)
              },
            }
          }),
      })
      const result = yield* invoke({ path: 'image.png' }).pipe(
        Effect.provide(
          tools.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(binding(directory), reader, NodeImages.layer, Policy.layer(vision)),
            ),
          ),
        ),
      )
      assert.deepStrictEqual(imageBytes(result), fixture('opaque.png'))
      assert.strictEqual((yield* image(result)).orientation, 1)
      assert.isAtMost(largest, 64 * 1024)
    }),
  ),
)

for (const stage of ['read', 'prepare'] as const) {
  it.effect(
    `cancellation during image ${stage} closes the native reader and scoped preparation`,
    () =>
      run(
        Effect.gen(function* () {
          const directory = yield* root
          yield* write(directory, 'image.png', fixture('opaque.png'))
          const started = yield* Deferred.make<void>()
          let closed = 0
          let released = 0
          let pause = true
          const original = yield* FileReader.FileReader.pipe(Effect.provide(files(directory)))
          const codec = yield* Images.ImagePreparation.pipe(Effect.provide(NodeImages.layer))
          const reader = Layer.succeed(FileReader.FileReader, {
            open: (path, options) =>
              Effect.gen(function* () {
                const handle = yield* original.open(path, options)
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    closed++
                  }),
                )
                return {
                  stat: handle.stat,
                  readAt: (offset, length) =>
                    Effect.gen(function* () {
                      if (pause && stage === 'read' && length > 32) {
                        yield* Deferred.succeed(started, undefined)
                        return yield* Effect.never
                      }
                      return yield* handle.readAt(offset, length)
                    }),
                }
              }),
          })
          const adapter = Layer.succeed(Images.ImagePreparation, {
            prepare: (bytes, mime, limit) =>
              Effect.scoped(
                Effect.gen(function* () {
                  if (pause && stage === 'prepare') {
                    yield* Effect.addFinalizer(() =>
                      Effect.sync(() => {
                        released++
                      }),
                    )
                    yield* Deferred.succeed(started, undefined)
                    return yield* Effect.never
                  }
                  return yield* codec.prepare(bytes, mime, limit)
                }),
              ),
          })
          yield* Effect.gen(function* () {
            const fiber = yield* invoke({ path: 'image.png' }).pipe(Effect.forkChild)
            yield* Deferred.await(started)
            yield* Fiber.interrupt(fiber)
            const exit = yield* Fiber.await(fiber)
            assert.strictEqual(exit._tag, 'Failure')
            assert.strictEqual(closed, 1)
            assert.strictEqual(released, stage === 'prepare' ? 1 : 0)
            pause = false
            yield* image(yield* invoke({ path: 'image.png' }))
            assert.strictEqual(closed, 2)
          }).pipe(
            Effect.provide(
              tools.layer.pipe(
                Layer.provideMerge(
                  Layer.mergeAll(binding(directory), reader, adapter, Policy.layer(vision)),
                ),
              ),
            ),
          )
        }),
      ),
  )
}

it.effect('streams a real large PNG into the source budget using bounded native reads', () =>
  run(
    Effect.gen(function* () {
      const directory = yield* root
      const pixels = new Uint8Array(512 * 512 * 4)
      let state = 42
      for (let offset = 0; offset < pixels.length; offset++) {
        state ^= state << 13
        state ^= state >>> 17
        state ^= state << 5
        pixels[offset] = offset % 4 === 3 ? 255 : state & 255
      }
      const png = yield* Effect.promise(() =>
        sharp(pixels, { raw: { width: 512, height: 512, channels: 4 } })
          .png()
          .toBuffer(),
      )
      assert.isAbove(png.length, 64 * 1024)
      yield* write(directory, 'large.png', png)
      const original = yield* FileReader.FileReader.pipe(Effect.provide(files(directory)))
      let largest = 0
      let reads = 0
      const reader = Layer.succeed(FileReader.FileReader, {
        open: (path, options) =>
          original.open(path, options).pipe(
            Effect.map((handle) => ({
              stat: handle.stat,
              readAt: (offset, length) => {
                largest = Math.max(largest, length)
                reads++
                return handle.readAt(offset, length)
              },
            })),
          ),
      })
      const result = yield* invoke({ path: 'large.png' }).pipe(
        Effect.provide(
          tools.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                binding(directory),
                reader,
                NodeImages.layer,
                Policy.layer({
                  vision: true,
                  limits: {
                    maxWidth: 600,
                    maxHeight: 600,
                    maxInputPixels: 300000,
                    maxBytes: 2 * 1024 * 1024,
                  },
                }),
              ),
            ),
          ),
        ),
      )
      assert.isFalse(result.isError)
      assert.deepStrictEqual(imageBytes(result), Uint8Array.from(png))
      assert.strictEqual(largest, 64 * 1024)
      assert.strictEqual(reads, 1 + Math.ceil(png.length / (64 * 1024)))
    }),
  ),
)

it.effect('Bun Layers execute the real image handler matrix', () =>
  Effect.sync(() => {
    const output = execFileSync(
      'bun',
      [new URL('./fixtures/CodingImages/Bun.ts', import.meta.url).pathname],
      { encoding: 'utf8', timeout: 30000 },
    )
    assert.include(output, 'Bun bound image Read matrix passed')
  }),
)

it.effect('portable text and image declarations import when codecs are denied', () =>
  Effect.sync(() => {
    const output = execFileSync(
      process.execPath,
      [new URL('./fixtures/CodingImages/Isolation.mjs', import.meta.url).pathname],
      { encoding: 'utf8', timeout: 30000, cwd: tmpdir() },
    )
    assert.include(output, 'Portable text and image Read declarations import without codecs')
  }),
)

it.effect(
  'interrupted real native decode releases its sharp handle and opened image exactly once',
  () =>
    run(
      Effect.gen(function* () {
        const directory = yield* root
        yield* write(directory, 'image.png', fixture('opaque.png'))
        const entered = yield* Deferred.make<void>()
        const photon = yield* WasmImages.Photon.pipe(Effect.provide(WasmImages.photonLayer))
        let created = 0
        let destroyed = 0
        const instrumented: typeof sharp = new Proxy(sharp, {
          apply(target, _receiver, args: Parameters<typeof sharp>) {
            const image = target(...args)
            created++
            const destroy = image.destroy.bind(image)
            image.destroy = (error) => {
              destroyed++
              return destroy(error)
            }
            return image
          },
        })
        const adapter = NodeImages.makeAdapter(instrumented, WasmImages.makeAdapter(photon))
        const codec = Images.layer({
          capabilities: adapter.capabilities,
          decode: Effect.fn(function* (bytes, mime, pixels) {
            yield* adapter.decode(bytes, mime, pixels)
            yield* Deferred.succeed(entered, undefined)
            return yield* Effect.never
          }),
        })
        const service = yield* FileReader.FileReader.pipe(Effect.provide(files(directory)))
        let closes = 0
        const reader = Layer.succeed(FileReader.FileReader, {
          open: (path, options) =>
            service.open(path, options).pipe(
              Effect.tap(() =>
                Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    closes++
                  }),
                ),
              ),
            ),
        })
        yield* Effect.gen(function* () {
          const fiber = yield* invoke({ path: 'image.png' }).pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          assert.strictEqual(created, 1)
          assert.strictEqual(destroyed, 0)
          assert.strictEqual(closes, 0)
          yield* Fiber.interrupt(fiber)
          assert.strictEqual((yield* Fiber.await(fiber))._tag, 'Failure')
          assert.strictEqual(destroyed, 1)
          assert.strictEqual(closes, 1)
        }).pipe(
          Effect.provide(
            tools.layer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(binding(directory), reader, codec, Policy.layer(vision)),
              ),
            ),
          ),
        )
      }),
    ),
)
