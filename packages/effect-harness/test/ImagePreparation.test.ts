import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Deferred from 'effect/Deferred'
import * as Images from '../src/ImagePreparation.js'
import { ImageDecodeFailed, ImageEncodeFailed } from '../src/ImagePreparationError.js'

const limits: Images.Limits = {
  maxWidth: 100,
  maxHeight: 100,
  maxBytes: 100,
  maxInputPixels: 10000,
}
const capabilities: Images.AdapterCapabilities = {
  decode: ['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp'],
  encode: ['image/png', 'image/jpeg', 'image/webp'],
  orientations: [1, 2, 3, 4, 5, 6, 7, 8],
  preserveAlpha: true,
  flattenWhite: true,
}
const metadata: Images.ImageMetadata = {
  mimeType: 'image/png',
  original: { width: 20, height: 10 },
  orientation: 1,
  animated: false,
  hasAlpha: false,
}
const adapter = (
  info: Images.ImageMetadata = metadata,
  encode: Images.DecodedImage['encode'] = () => Effect.succeed(new Uint8Array(3)),
): Images.Adapter => ({ capabilities, decode: () => Effect.succeed({ metadata: info, encode }) })

describe('image policy (fake codecs; native decode/encode conformance belongs to adapters)', () => {
  it.effect('counts padded base64 bytes and exact binary budget boundaries', () =>
    Effect.gen(function* () {
      for (let budget = 1; budget < 40; budget++) {
        const binary = Images.binaryBudget(budget)
        assert.isAtMost(Images.base64Length(binary), budget)
        assert.isAbove(Images.base64Length(binary + 1), budget)
      }
      assert.deepStrictEqual([0, 1, 2, 3, 4, 5, 6].map(Images.base64Length), [0, 4, 4, 4, 8, 8, 8])
      const service = Images.make(adapter())
      const result = yield* service.prepare(new Uint8Array(3), 'image/png', {
        ...limits,
        maxBytes: 4,
      })
      assert.isTrue(result.passthrough)
      const failure = yield* service
        .prepare(new Uint8Array(3), 'image/png', { ...limits, maxBytes: 3 })
        .pipe(Effect.flip)
      assert.strictEqual(failure._tag, 'ImageCannotFit')
    }),
  )

  it.effect('rejects malformed limits before calling the codec', () =>
    Effect.gen(function* () {
      let decodes = 0
      const service = Images.make({
        capabilities,
        decode: () => {
          decodes++
          return Effect.succeed({ metadata, encode: () => Effect.succeed(new Uint8Array(3)) })
        },
      })
      for (const field of ['maxWidth', 'maxHeight', 'maxBytes', 'maxInputPixels'] as const) {
        for (const value of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
          const error = yield* service
            .prepare(new Uint8Array(1), 'image/png', { ...limits, [field]: value })
            .pipe(Effect.flip)
          assert.strictEqual(error._tag, 'InvalidImageLimits')
          if (error._tag === 'InvalidImageLimits') assert.strictEqual(error.field, field)
        }
      }
      for (const malformed of [
        null,
        undefined,
        {},
        { ...limits, transparency: null },
        { ...limits, maxBytes: '4' },
      ]) {
        const error = yield* service
          .prepare(new Uint8Array(1), 'image/png', malformed as Images.Limits)
          .pipe(Effect.flip)
        assert.strictEqual(error._tag, 'InvalidImageLimits')
      }
      assert.strictEqual(decodes, 0)
    }),
  )

  it.effect('validates inline sources and owns passthrough bytes, including Buffer views', () =>
    Effect.gen(function* () {
      for (const mimeType of ['image/png', 'image/jpeg', 'image/gif', 'image/webp'] as const) {
        let decodes = 0
        let encodes = 0
        const backing = Buffer.from([9, 1, 2, 3, 8])
        const input = backing.subarray(1, 4)
        const service = Images.make({
          capabilities,
          decode: (bytes, mime, maxPixels) => {
            decodes++
            assert.notStrictEqual(bytes.buffer, input.buffer)
            assert.strictEqual(mime, mimeType)
            assert.strictEqual(maxPixels, limits.maxInputPixels)
            return Effect.succeed({
              metadata: { ...metadata, mimeType, animated: mimeType === 'image/gif' },
              encode: () => {
                encodes++
                return Effect.succeed(input)
              },
            })
          },
        })
        const result = yield* service.prepare(input, mimeType, limits)
        assert.deepStrictEqual(Array.from(result.bytes), [1, 2, 3])
        assert.strictEqual(result.mimeType, mimeType)
        assert.isTrue(result.passthrough)
        assert.strictEqual(result.animation, mimeType === 'image/gif' ? 'unchanged' : 'static')
        backing.fill(0)
        assert.deepStrictEqual(Array.from(result.bytes), [1, 2, 3])
        result.bytes.fill(7)
        assert.deepStrictEqual(Array.from(input), [0, 0, 0])
        assert.strictEqual(decodes, 1)
        assert.strictEqual(encodes, 0)
      }
    }),
  )

  it.effect('reports decode validation, input allocation guards and unsupported capabilities', () =>
    Effect.gen(function* () {
      const invalid = [
        { ...metadata, original: { width: 0, height: 1 } },
        { ...metadata, original: { width: Infinity, height: 1 } },
        { ...metadata, orientation: 9 } as unknown as Images.ImageMetadata,
        { ...metadata, mimeType: 'image/jpeg' } as const,
      ]
      for (const info of invalid) {
        const error = yield* Images.make(adapter(info))
          .prepare(new Uint8Array(3), 'image/png', limits)
          .pipe(Effect.flip)
        assert.strictEqual(error._tag, 'ImageDecodeFailed')
      }
      const oversized = yield* Images.make(
        adapter({ ...metadata, original: { width: Number.MAX_SAFE_INTEGER, height: 2 } }),
      )
        .prepare(new Uint8Array(3), 'image/png', {
          ...limits,
          maxInputPixels: Number.MAX_SAFE_INTEGER,
        })
        .pipe(Effect.flip)
      assert.strictEqual(oversized._tag, 'ImageInputPixelsExceeded')
      const service = Images.make(adapter())
      const unknown = yield* service
        .prepare(new Uint8Array(1), 'image/svg+xml', limits)
        .pipe(Effect.flip)
      assert.strictEqual(unknown._tag, 'UnsupportedImage')
      const missing = yield* Images.make({
        ...adapter({ ...metadata, mimeType: 'image/bmp' }),
        capabilities: { ...capabilities, decode: ['image/png'] },
      })
        .prepare(new Uint8Array(1), 'image/bmp', limits)
        .pipe(Effect.flip)
      assert.strictEqual(missing._tag, 'UnsupportedImage')
      const rejected = new ImageDecodeFailed({
        mimeType: 'image/png',
        message: 'Damaged pixel data',
      })
      const error = yield* Images.make({ capabilities, decode: () => Effect.fail(rejected) })
        .prepare(new Uint8Array(3), 'image/png', limits)
        .pipe(Effect.flip)
      assert.strictEqual(error, rejected)
    }),
  )

  it.effect('maps every EXIF orientation and passes exact normalization requests', () =>
    Effect.gen(function* () {
      const expected = [
        [
          [0, 0],
          [2, 0],
          [0, 1],
          [2, 1],
        ],
        [
          [2, 0],
          [0, 0],
          [2, 1],
          [0, 1],
        ],
        [
          [2, 1],
          [0, 1],
          [2, 0],
          [0, 0],
        ],
        [
          [0, 1],
          [2, 1],
          [0, 0],
          [2, 0],
        ],
        [
          [0, 0],
          [0, 2],
          [1, 0],
          [1, 2],
        ],
        [
          [1, 0],
          [1, 2],
          [0, 0],
          [0, 2],
        ],
        [
          [1, 2],
          [1, 0],
          [0, 2],
          [0, 0],
        ],
        [
          [0, 2],
          [0, 0],
          [1, 2],
          [1, 0],
        ],
      ]
      const size = { width: 3, height: 2 }
      for (const orientation of capabilities.orientations) {
        const mapped = [
          [0, 0],
          [2, 0],
          [0, 1],
          [2, 1],
        ].map(([x = 0, y = 0]) => {
          const point = Images.orientPoint(size, orientation, x, y)
          return [point.x, point.y]
        })
        assert.deepStrictEqual(mapped, expected[orientation - 1])
        const requests: Array<Images.EncodingRequest> = []
        const service = Images.make(
          adapter({ ...metadata, original: size, orientation }, (request) => {
            requests.push(request)
            return Effect.succeed(new Uint8Array(3))
          }),
        )
        // Force a transform even for orientation 1.
        const result = yield* service.prepare(new Uint8Array(4), 'image/png', {
          ...limits,
          maxBytes: 4,
        })
        assert.strictEqual(requests[0]?.orientation, orientation)
        assert.strictEqual(requests[0]?.frame, 'first')
        assert.deepStrictEqual(result.oriented, orientation >= 5 ? { width: 2, height: 3 } : size)
        assert.deepStrictEqual(result.original, size)
        assert.isFalse(result.passthrough)
      }
    }),
  )

  it.effect('fits without enlargement and retries from original aspect ratio down to 1×1', () =>
    Effect.gen(function* () {
      assert.deepStrictEqual(Images.fitDimensions({ width: 20, height: 10 }, limits), {
        width: 20,
        height: 10,
      })
      assert.deepStrictEqual(Images.fitDimensions({ width: 400, height: 200 }, limits), {
        width: 100,
        height: 50,
      })
      assert.deepStrictEqual(
        Images.fitDimensions({ width: 400, height: 1 }, { maxWidth: 1, maxHeight: 1 }),
        { width: 1, height: 1 },
      )
      const requests: Array<Images.EncodingRequest> = []
      let released = 0
      const service = Images.make({
        capabilities: { ...capabilities, encode: ['image/png'] },
        decode: () =>
          Effect.acquireRelease(
            Effect.succeed({
              metadata: { ...metadata, original: { width: 200, height: 80 } },
              encode: (request: Images.EncodingRequest) => {
                requests.push(request)
                return Effect.succeed(new Uint8Array(4))
              },
            }),
            () =>
              Effect.sync(() => {
                released++
              }),
          ),
      })
      const error = yield* service
        .prepare(new Uint8Array(4), 'image/png', { ...limits, maxBytes: 4, maxInputPixels: 16000 })
        .pipe(Effect.flip)
      assert.strictEqual(error._tag, 'ImageCannotFit')
      assert.deepStrictEqual(requests[0]?.dimensions, { width: 100, height: 40 })
      assert.deepStrictEqual(requests.at(-1)?.dimensions, { width: 1, height: 1 })
      assert.isBelow(requests.length, 30)
      for (const { dimensions } of requests) {
        assert.isAtMost(dimensions.width, 100)
        assert.isAtMost(dimensions.height, 40)
        assert.isAtMost(Math.abs(dimensions.height - dimensions.width * 0.4), 1)
      }
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('caps safe-integer rounding in the service result and codec request', () =>
    Effect.gen(function* () {
      const original = { width: 5325550218379263, height: 1 }
      const maximum = { maxWidth: 3783683143962315, maxHeight: 1 }
      const requests: Array<Images.EncodingRequest> = []
      const service = Images.make({
        capabilities: { ...capabilities, encode: ['image/png'] },
        decode: () =>
          Effect.succeed({
            metadata: { ...metadata, mimeType: 'image/bmp', original },
            encode: (request: Images.EncodingRequest) => {
              requests.push(request)
              return Effect.succeed(new Uint8Array([1]))
            },
          }),
      })
      const result = yield* service.prepare(new Uint8Array([9]), 'image/bmp', {
        ...maximum,
        maxBytes: 4,
        maxInputPixels: Number.MAX_SAFE_INTEGER,
      })
      const expected = { width: maximum.maxWidth, height: 1 }
      assert.deepStrictEqual(
        requests.map((request) => request.dimensions),
        [expected],
      )
      assert.deepStrictEqual(result.dimensions, expected)
      assert.deepStrictEqual(result.resized, { from: original, to: expected })
      assert.isFalse(result.passthrough)
    }),
  )

  it.effect(
    'keeps wide, tall and extreme fits bounded and within one pixel of exact aspect fit',
    () =>
      Effect.sync(() => {
        const edge = Number.MAX_SAFE_INTEGER
        const dimensions = [
          1,
          2,
          7,
          100,
          94906265,
          3783683143962315,
          5325550218379263,
          edge - 1,
          edge,
        ]
        for (const width of dimensions) {
          for (const height of dimensions) {
            // Use integer arithmetic independently of the floating-point fit implementation.
            if (BigInt(width) * BigInt(height) > BigInt(edge)) continue
            for (const maxWidth of dimensions) {
              for (const maxHeight of dimensions) {
                const fitted = Images.fitDimensions({ width, height }, { maxWidth, maxHeight })
                let numerator = 1n
                let denominator = 1n
                for (const [cap, original] of [
                  [maxWidth, width],
                  [maxHeight, height],
                ] as const) {
                  if (BigInt(cap) * denominator < numerator * BigInt(original)) {
                    numerator = BigInt(cap)
                    denominator = BigInt(original)
                  }
                }
                for (const [actual, original, cap] of [
                  [fitted.width, width, maxWidth],
                  [fitted.height, height, maxHeight],
                ] as const) {
                  assert.isTrue(Number.isSafeInteger(actual))
                  assert.isAtLeast(actual, 1)
                  assert.isAtMost(actual, original)
                  assert.isAtMost(actual, cap)
                  const difference = BigInt(actual) * denominator - BigInt(original) * numerator
                  assert.isTrue(difference >= -denominator && difference <= denominator)
                }
              }
            }
          }
        }
      }),
  )

  it.effect('converts BMP, records resize/conversion metadata and detaches codec output', () =>
    Effect.gen(function* () {
      const encoded = new Uint8Array([1, 2, 3])
      let released = 0
      const info: Images.ImageMetadata = {
        ...metadata,
        mimeType: 'image/bmp',
        original: { width: 200, height: 100 },
        orientation: 6,
        animated: false,
        hasAlpha: true,
      }
      const service = Images.make({
        capabilities,
        decode: () =>
          Effect.acquireRelease(
            Effect.succeed({
              metadata: info,
              encode: (request: Images.EncodingRequest) => {
                assert.deepStrictEqual(request.dimensions, { width: 50, height: 100 })
                return Effect.succeed(encoded)
              },
            }),
            () =>
              Effect.sync(() => {
                encoded.fill(0)
                released++
              }),
          ),
      })
      const result = yield* service.prepare(new Uint8Array(3), 'image/bmp', {
        ...limits,
        maxInputPixels: 20000,
      })
      assert.deepStrictEqual(Array.from(result.bytes), [1, 2, 3])
      assert.strictEqual(result.convertedFrom, 'image/bmp')
      assert.strictEqual(result.animation, 'static')
      assert.strictEqual(result.transparency, 'preserved')
      assert.deepStrictEqual(result.resized, {
        from: { width: 100, height: 200 },
        to: { width: 50, height: 100 },
      })
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('orders format/quality candidates and makes white flattening opt-in', () =>
    Effect.gen(function* () {
      const validated = yield* Images.validateLimits(limits)
      const jpeg = Images.encodingCandidates(
        { ...metadata, mimeType: 'image/jpeg' },
        validated,
        capabilities,
      )
      assert.deepStrictEqual(
        jpeg.slice(0, 4).map((c) => [c.mimeType, c.quality]),
        [
          ['image/jpeg', 80],
          ['image/jpeg', 70],
          ['image/jpeg', 55],
          ['image/jpeg', 40],
        ],
      )
      const alpha = { ...metadata, mimeType: 'image/bmp', hasAlpha: true } as const
      const jpegOnly = {
        ...capabilities,
        encode: ['image/jpeg'],
      } satisfies Images.AdapterCapabilities
      const unsupported = yield* Images.make({ ...adapter(alpha), capabilities: jpegOnly })
        .prepare(new Uint8Array(3), 'image/bmp', limits)
        .pipe(Effect.flip)
      assert.strictEqual(unsupported._tag, 'UnsupportedImage')
      const result = yield* Images.make({
        ...adapter(alpha, (request) => {
          assert.strictEqual(request.transparency, 'flatten-white')
          return Effect.succeed(new Uint8Array(3))
        }),
        capabilities: jpegOnly,
      }).prepare(new Uint8Array(3), 'image/bmp', { ...limits, transparency: 'flatten-white' })
      assert.strictEqual(result.transparency, 'flattened-white')
      assert.strictEqual(result.mimeType, 'image/jpeg')
    }),
  )

  it.effect(
    'transforms only the first animated frame and retries quality without resizing first',
    () =>
      Effect.gen(function* () {
        const requests: Array<Images.EncodingRequest> = []
        const service = Images.make(
          adapter({ ...metadata, mimeType: 'image/gif', animated: true }, (request) => {
            requests.push(request)
            // PNG and first WebP quality exceed the binary budget; second WebP fits.
            return Effect.succeed(new Uint8Array(requests.length < 3 ? 4 : 3))
          }),
        )
        const result = yield* service.prepare(new Uint8Array(4), 'image/gif', {
          ...limits,
          maxBytes: 4,
        })
        assert.strictEqual(result.animation, 'first-frame')
        assert.strictEqual(result.convertedFrom, 'image/gif')
        assert.strictEqual(result.mimeType, 'image/webp')
        assert.isUndefined(result.resized)
        assert.deepStrictEqual(
          requests.map((r) => [r.mimeType, r.quality]),
          [
            ['image/png', undefined],
            ['image/webp', 80],
            ['image/webp', 70],
          ],
        )
        for (const request of requests) {
          assert.strictEqual(request.frame, 'first')
          assert.deepStrictEqual(request.dimensions, metadata.original)
        }
      }),
  )

  it.effect('closes resources on encoder failure and snapshots adapter capabilities', () =>
    Effect.gen(function* () {
      const formats: Array<Images.InputMimeType> = ['image/bmp']
      let released = 0
      const rejected = new ImageEncodeFailed({ mimeType: 'image/png', message: 'Failure' })
      const service = Images.make({
        capabilities: { ...capabilities, decode: formats },
        decode: () =>
          Effect.acquireRelease(
            Effect.succeed({
              metadata: { ...metadata, mimeType: 'image/bmp' } as const,
              encode: () => Effect.fail(rejected),
            }),
            () =>
              Effect.sync(() => {
                released++
              }),
          ),
      })
      formats.length = 0
      const error = yield* service.prepare(new Uint8Array(3), 'image/bmp', limits).pipe(Effect.flip)
      assert.strictEqual(error, rejected)
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('keeps semantic encode failures distinct from inability to fit', () =>
    Effect.gen(function* () {
      const rejected = new ImageEncodeFailed({ mimeType: 'image/png', message: 'Codec failed' })
      const error = yield* Images.make(
        adapter({ ...metadata, mimeType: 'image/bmp' }, () => Effect.fail(rejected)),
      )
        .prepare(new Uint8Array(3), 'image/bmp', limits)
        .pipe(Effect.flip)
      assert.strictEqual(error, rejected)
      const empty = yield* Images.make(
        adapter({ ...metadata, mimeType: 'image/bmp' }, () => Effect.succeed(new Uint8Array(0))),
      )
        .prepare(new Uint8Array(3), 'image/bmp', limits)
        .pipe(Effect.flip)
      assert.strictEqual(empty._tag, 'ImageEncodeFailed')
    }),
  )

  it.effect('closes acquired codec resources on interrupted encoding', () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      let released = 0
      const service = Images.make({
        capabilities,
        decode: () =>
          Effect.acquireRelease(
            Effect.succeed({
              metadata: { ...metadata, mimeType: 'image/bmp' } as const,
              encode: () => Effect.andThen(Deferred.succeed(started, undefined), Effect.never),
            }),
            () =>
              Effect.sync(() => {
                released++
              }),
          ),
      })
      const fiber = yield* service
        .prepare(new Uint8Array(3), 'image/bmp', limits)
        .pipe(Effect.forkChild)
      yield* Deferred.await(started)
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(released, 1)
    }),
  )

  it.effect('keeps two provided service Layers isolated', () =>
    Effect.gen(function* () {
      const a = Images.layer(
        adapter({ ...metadata, mimeType: 'image/bmp' }, () => Effect.succeed(new Uint8Array([1]))),
      )
      const b = Images.layer(
        adapter({ ...metadata, mimeType: 'image/bmp' }, () => Effect.succeed(new Uint8Array([2]))),
      )
      const results = yield* Effect.all(
        [
          Images.prepare(new Uint8Array(3), 'image/bmp', limits).pipe(Effect.provide(a)),
          Images.prepare(new Uint8Array(3), 'image/bmp', limits).pipe(Effect.provide(b)),
        ],
        { concurrency: 'unbounded' },
      )
      assert.deepStrictEqual(
        results.map((r) => Array.from(r.bytes)),
        [[1], [2]],
      )
    }),
  )
})
