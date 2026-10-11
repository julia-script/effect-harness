import { conformance } from './fixtures/images-native/Conformance.js'
import * as Wasm from '../src/images-wasm/index.js'
conformance('Photon WASM', Wasm.layer)

import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Fiber from 'effect/Fiber'
import * as Photon from '@cf-wasm/photon/node'
import * as Images from '../src/ImagePreparation.js'
import { fixture, limits, rgba } from './fixtures/images-native/Conformance.js'

it.effect(
  'frees every real Photon source and candidate exactly once, including failure and interruption',
  () =>
    Effect.gen(function* () {
      let acquired = 0
      let released = 0
      const seen = new WeakSet<Photon.PhotonImage>()
      class ObservedImage extends Photon.PhotonImage {
        constructor(...args: ConstructorParameters<typeof Photon.PhotonImage>) {
          super(...args)
          acquired++
        }
        override free() {
          assert.isFalse(seen.has(this))
          seen.add(this)
          released++
          super.free()
        }
        static override new_from_byteslice(bytes: Uint8Array) {
          const actual = Photon.PhotonImage.new_from_byteslice(bytes)
          try {
            return new ObservedImage(
              actual.get_raw_pixels(),
              actual.get_width(),
              actual.get_height(),
            )
          } finally {
            actual.free()
          }
        }
      }
      const api: Wasm.PhotonApi = {
        PhotonImage: ObservedImage,
        SamplingFilter: Photon.SamplingFilter,
        resize: (image, width, height, filter) => {
          const actual = Photon.resize(image, width, height, filter)
          try {
            return new ObservedImage(
              actual.get_raw_pixels(),
              actual.get_width(),
              actual.get_height(),
            )
          } finally {
            actual.free()
          }
        },
      }
      const adapter = Wasm.makeAdapter(api)
      yield* Images.make(adapter).prepare(fixture('opaque.bmp'), 'image/bmp', limits)
      assert.strictEqual(acquired, released)
      yield* Images.make(adapter)
        .prepare(fixture('opaque.bmp'), 'image/bmp', { ...limits, maxBytes: 4 })
        .pipe(Effect.flip)
      assert.strictEqual(acquired, released)
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* adapter.decode(fixture('opaque.png'), 'image/png', 100)
          return yield* Effect.never
        }),
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Fiber.interrupt(fiber)
      assert.strictEqual(acquired, released)
      assert.isAbove(acquired, 3)
    }),
)

it.effect(
  'encodes candidates from the original pixels and explicitly flattens white for JPEG',
  () =>
    Effect.gen(function* () {
      const adapter = Wasm.makeAdapter(Photon)
      yield* Effect.scoped(
        Effect.gen(function* () {
          const decoded = yield* adapter.decode(fixture('alpha.png'), 'image/png', 100)
          const request: Images.EncodingRequest = {
            mimeType: 'image/jpeg',
            dimensions: { width: 3, height: 2 },
            orientation: 1,
            frame: 'first',
            transparency: 'flatten-white',
            quality: 100,
          }
          const jpeg = yield* decoded.encode(request)
          const pixels = yield* rgba(jpeg)
          assert.isAbove(pixels.data[0]!, 230)
          assert.isAbove(pixels.data[1]!, 230)
          assert.isAbove(pixels.data[2]!, 230)
          const pngRequest: Images.EncodingRequest = {
            ...request,
            mimeType: 'image/png',
            transparency: 'preserve',
          }
          const before = yield* decoded.encode(pngRequest)
          yield* decoded.encode({ ...request, quality: 1, dimensions: { width: 1, height: 1 } })
          const after = yield* decoded.encode(pngRequest)
          assert.deepStrictEqual(before, after)
          const webp = yield* decoded.encode({ ...pngRequest, mimeType: 'image/webp' })
          assert.strictEqual((yield* rgba(webp)).info.width, 3)
        }),
      )
    }),
)

it.effect('supports environment-supplied initialized Photon Layers', () =>
  Effect.gen(function* () {
    const supplied = Wasm.layerWithPhoton.pipe(
      Layer.provide(Wasm.photonLayerWith(Effect.succeed(Photon))),
    )
    const result = yield* Images.prepare(fixture('opaque.bmp'), 'image/bmp', limits).pipe(
      Effect.provide(supplied),
    )
    assert.strictEqual(result.passthrough, false)
  }),
)

import { malformedAnimation } from './fixtures/images-native/Conformance.js'
for (const format of ['gif', 'webp'] as const) {
  it.effect(`rejects corrupt later ${format} pixels before passthrough`, () =>
    Effect.gen(function* () {
      const failure = yield* Images.prepare(
        malformedAnimation(format),
        `image/${format}`,
        limits,
      ).pipe(Effect.flip)
      assert.strictEqual(failure._tag, 'ImageDecodeFailed')
    }).pipe(Effect.provide(Wasm.layer)),
  )
}

it.effect('rejects oversized headers before constructing a Photon image', () =>
  Effect.gen(function* () {
    let allocations = 0
    class GuardedPhoton extends Photon.PhotonImage {
      static override new_from_byteslice(bytes: Uint8Array) {
        allocations++
        return Photon.PhotonImage.new_from_byteslice(bytes)
      }
    }
    const adapter = Wasm.makeAdapter({ ...Photon, PhotonImage: GuardedPhoton })
    for (const [file, mime] of [
      ['opaque.png', 'image/png'],
      ['opaque.jpg', 'image/jpeg'],
      ['animated.gif', 'image/gif'],
      ['animated.webp', 'image/webp'],
      ['opaque.bmp', 'image/bmp'],
    ] as const) {
      const failure = yield* Effect.scoped(adapter.decode(fixture(file), mime, 5)).pipe(Effect.flip)
      assert.strictEqual(failure._tag, 'ImageInputPixelsExceeded')
    }
    assert.strictEqual(allocations, 0)
  }),
)

for (const format of ['gif', 'webp'] as const) {
  it.effect(`encodes the first ${format} frame, rather than a later frame`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const decoded = yield* Wasm.makeAdapter(Photon).decode(
          fixture(`animated.${format}`),
          `image/${format}`,
          100,
        )
        const bytes = yield* decoded.encode({
          mimeType: 'image/png',
          dimensions: { width: 3, height: 2 },
          orientation: 1,
          frame: 'first',
          transparency: 'preserve',
        })
        assert.deepStrictEqual((yield* rgba(bytes)).data, (yield* rgba(fixture('opaque.png'))).data)
      }),
    ),
  )
}
