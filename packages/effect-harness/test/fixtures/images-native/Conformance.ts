import { assert, describe, it } from '@effect/vitest'
import { readFileSync } from 'node:fs'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import sharp from 'sharp'
import * as Images from '../../../src/ImagePreparation.js'
import type { ImagePreparationError } from '../../../src/ImagePreparationError.js'

export const fixture = (name: string): Uint8Array =>
  Uint8Array.from(readFileSync(new URL(name, import.meta.url)))
export const limits: Images.Limits = {
  maxWidth: 30,
  maxHeight: 30,
  maxBytes: 100000,
  maxInputPixels: 100,
}
export const rgba = (bytes: Uint8Array) =>
  Effect.promise(() => sharp(bytes).ensureAlpha().raw().toBuffer({ resolveWithObject: true }))

const colors = [
  [255, 0, 0, 255],
  [0, 255, 0, 255],
  [0, 0, 255, 255],
  [255, 255, 0, 255],
  [255, 0, 255, 255],
  [0, 255, 255, 255],
]
// Explicit pixel grids, independently specified from the implementation's orientation helper.
const grids = [
  [0, 1, 2, 3, 4, 5],
  [2, 1, 0, 5, 4, 3],
  [5, 4, 3, 2, 1, 0],
  [3, 4, 5, 0, 1, 2],
  [0, 3, 1, 4, 2, 5],
  [3, 0, 4, 1, 5, 2],
  [5, 2, 4, 1, 3, 0],
  [2, 5, 1, 4, 0, 3],
]

export const conformance = (
  name: string,
  layer: Layer.Layer<Images.ImagePreparation, ImagePreparationError>,
) => {
  describe(name, () => {
    for (const [file, mime] of [
      ['opaque.png', 'image/png'],
      ['opaque.jpg', 'image/jpeg'],
      ['opaque.webp', 'image/webp'],
      ['animated.gif', 'image/gif'],
      ['animated.webp', 'image/webp'],
      ['opaque.bmp', 'image/bmp'],
    ] as const) {
      it.effect(`fully decodes ${file} and preserves eligible input bytes`, () =>
        Effect.gen(function* () {
          const input = fixture(file)
          const result = yield* Images.prepare(input, mime, limits)
          assert.deepStrictEqual(result.dimensions, { width: 3, height: 2 })
          assert.strictEqual(result.passthrough, mime !== 'image/bmp')
          if (result.passthrough) assert.deepStrictEqual(result.bytes, input)
          else assert.strictEqual(result.convertedFrom, 'image/bmp')
          assert.notStrictEqual(result.bytes.buffer, input.buffer)
        }).pipe(Effect.provide(layer)),
      )
      it.effect(`resizes first frame of ${file} exactly`, () =>
        Effect.gen(function* () {
          const result = yield* Images.prepare(fixture(file), mime, {
            ...limits,
            maxWidth: 2,
            maxHeight: 1,
          })
          assert.deepStrictEqual(result.dimensions, { width: 2, height: 1 })
          const decoded = yield* rgba(result.bytes)
          assert.strictEqual(decoded.info.width, 2)
          assert.strictEqual(decoded.info.height, 1)
          assert.strictEqual(
            result.animation,
            file.startsWith('animated') ? 'first-frame' : 'static',
          )
        }).pipe(Effect.provide(layer)),
      )
      it.effect(`rejects ${file} before oversized pixel allocation`, () =>
        Effect.gen(function* () {
          const error = yield* Images.prepare(fixture(file), mime, {
            ...limits,
            maxInputPixels: 5,
          }).pipe(Effect.flip)
          assert.strictEqual(error._tag, 'ImageInputPixelsExceeded')
        }).pipe(Effect.provide(layer)),
      )
    }
    for (let n = 1; n <= 8; n++) {
      it.effect(`normalizes EXIF orientation ${n} with exact PNG pixels`, () =>
        Effect.gen(function* () {
          const result = yield* Images.prepare(fixture(`orientation-${n}.png`), 'image/png', limits)
          const decoded = yield* rgba(result.bytes)
          assert.deepStrictEqual(
            [...decoded.data],
            grids[n - 1]!.flatMap((i) => colors[i]!),
          )
          assert.deepStrictEqual(
            result.dimensions,
            n < 5 ? { width: 3, height: 2 } : { width: 2, height: 3 },
          )
          assert.strictEqual(result.orientation, n)
          const metadata = yield* Effect.promise(() => sharp(result.bytes).metadata())
          if (!result.passthrough) assert.strictEqual(metadata.orientation, undefined)
        }).pipe(Effect.provide(layer)),
      )
      it.effect(`reads JPEG orientation ${n} and strips stale orientation`, () =>
        Effect.gen(function* () {
          const result = yield* Images.prepare(
            fixture(`orientation-${n}.jpg`),
            'image/jpeg',
            limits,
          )
          assert.strictEqual(result.orientation, n)
          assert.deepStrictEqual(
            result.dimensions,
            n < 5 ? { width: 3, height: 2 } : { width: 2, height: 3 },
          )
          const metadata = yield* Effect.promise(() => sharp(result.bytes).metadata())
          if (!result.passthrough) assert.strictEqual(metadata.orientation, undefined)
        }).pipe(Effect.provide(layer)),
      )
    }
    for (const format of ['png', 'gif', 'webp'] as const) {
      it.effect(`preserves ${format} transparency while resizing`, () =>
        Effect.gen(function* () {
          const result = yield* Images.prepare(fixture(`alpha.${format}`), `image/${format}`, {
            ...limits,
            maxWidth: 2,
          })
          assert.strictEqual(result.transparency, 'preserved')
          const pixels = yield* rgba(result.bytes)
          assert.isTrue([...pixels.data].some((value, index) => index % 4 === 3 && value !== 255))
        }).pipe(Effect.provide(layer)),
      )
    }
    it.effect('enforces padded base64 budget and reports an impossible budget', () =>
      Effect.gen(function* () {
        const result = yield* Images.prepare(fixture('opaque.bmp'), 'image/bmp', {
          ...limits,
          maxBytes: 120,
        })
        assert.isAtMost(Images.base64Length(result.bytes.length), 120)
        const failure = yield* Images.prepare(fixture('opaque.bmp'), 'image/bmp', {
          ...limits,
          maxBytes: 4,
        }).pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'ImageCannotFit')
      }).pipe(Effect.provide(layer)),
    )
    it.effect('validates malformed pixels even when passthrough would fit', () =>
      Effect.gen(function* () {
        const bytes = fixture('opaque.png')
        // Intact dimensions/chunk lengths but corrupt compressed image data.
        bytes[62] = 0
        bytes[63] = 0
        const failure = yield* Images.prepare(bytes, 'image/png', limits).pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'ImageDecodeFailed')
        const mismatch = yield* Images.prepare(fixture('opaque.png'), 'image/jpeg', limits).pipe(
          Effect.flip,
        )
        assert.strictEqual(mismatch._tag, 'ImageDecodeFailed')
      }).pipe(Effect.provide(layer)),
    )
    it.effect('isolates concurrently bound services', () =>
      Effect.gen(function* () {
        const [a, b] = yield* Effect.all(
          [
            Images.prepare(fixture('orientation-6.png'), 'image/png', {
              ...limits,
              maxWidth: 1,
            }).pipe(Effect.provide(layer)),
            Images.prepare(fixture('opaque.bmp'), 'image/bmp', limits).pipe(Effect.provide(layer)),
          ],
          { concurrency: 'unbounded' },
        )
        assert.deepStrictEqual(a.dimensions, { width: 1, height: 2 })
        assert.deepStrictEqual(b.dimensions, { width: 3, height: 2 })
      }),
    )
  })
}

export const malformedAnimation = (format: 'gif' | 'webp'): Uint8Array => {
  const bytes = fixture(`animated.${format}`)
  const view = new DataView(bytes.buffer)
  if (format === 'webp') {
    let count = 0
    for (let offset = 12; offset < bytes.length;) {
      const length = view.getUint32(offset + 4, true)
      if (view.getUint32(offset) === 0x414e4d46 && ++count === 2) {
        const data = offset + 8 + 16 + 8
        // A valid lossless signature/dimensions, but invalid compressed pixel data.
        bytes.fill(0, data + 5, offset + 8 + length)
        break
      }
      offset += 8 + length + (length & 1)
    }
  } else {
    let count = 0,
      offset = 13 + (bytes[10]! & 0x80 ? 3 * 2 ** ((bytes[10]! & 7) + 1) : 0)
    const blocks = () => {
      while (bytes[offset] !== 0) {
        offset += 1 + bytes[offset]!
      }
      offset++
    }
    while (offset < bytes.length) {
      const marker = bytes[offset++]!
      if (marker === 0x3b) break
      if (marker === 0x21) {
        offset++
        blocks()
        continue
      }
      const local = bytes[offset + 8]!
      offset += 9
      if (local & 0x80) offset += 3 * 2 ** ((local & 7) + 1)
      offset++
      if (++count === 2) {
        const n = bytes[offset++]!
        bytes.fill(255, offset, offset + n)
        break
      }
      blocks()
    }
  }
  return bytes
}
