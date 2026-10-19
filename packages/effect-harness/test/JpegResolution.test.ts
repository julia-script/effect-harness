import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Photon from '@cf-wasm/photon/node'
import sharp from 'sharp'
import * as Images from '../src/ImagePreparation.js'
import * as WasmImages from '../src/images-wasm/index.js'

for (const [width, height] of [
  [2, 4097],
  [4097, 2],
  [17, 482],
  [482, 17],
  [1001, 1000],
] as const) {
  for (const progressive of [false, true]) {
    for (const chromaSubsampling of ['4:4:4', '4:2:0']) {
      it.effect(
        `JPEG resolution boundary ${width}x${height} ${chromaSubsampling} progressive=${progressive}`,
        () =>
          Effect.gen(function* () {
            const bytes = yield* Effect.promise(() =>
              sharp({ create: { width, height, channels: 3, background: '#274b91' } })
                .jpeg({ progressive, chromaSubsampling })
                .toBuffer(),
            )
            let allocations = 0
            class GuardedPhoton extends Photon.PhotonImage {
              static override new_from_byteslice(input: Uint8Array) {
                allocations++
                return Photon.PhotonImage.new_from_byteslice(input)
              }
            }
            const service = Images.make(
              WasmImages.makeAdapter({ ...Photon, PhotonImage: GuardedPhoton }),
            )
            const pixels = width * height
            const limits = { maxWidth: width, maxHeight: height, maxBytes: 1000000 }
            const failure = yield* service
              .prepare(bytes, 'image/jpeg', { ...limits, maxInputPixels: pixels - 1 })
              .pipe(Effect.flip)
            assert.strictEqual(failure._tag, 'ImageInputPixelsExceeded')
            assert.strictEqual(allocations, 0)
            for (const maxInputPixels of [pixels, pixels + 1]) {
              const result = yield* service.prepare(bytes, 'image/jpeg', {
                ...limits,
                maxInputPixels,
              })
              assert.isTrue(result.passthrough)
              assert.deepStrictEqual(result.original, { width, height })
              assert.deepStrictEqual(result.bytes, Uint8Array.from(bytes))
            }
            assert.strictEqual(allocations, 2)
          }),
      )
    }
  }
}

for (const colourspace of ['b-w', 'cmyk'] as const) {
  for (const progressive of [false, true]) {
    it.effect(`JPEG exact 8194-pixel boundary ${colourspace} progressive=${progressive}`, () =>
      Effect.gen(function* () {
        const bytes = yield* Effect.promise(() =>
          sharp({ create: { width: 2, height: 4097, channels: 3, background: '#274b91' } })
            .toColourspace(colourspace)
            .jpeg({ progressive, chromaSubsampling: '4:4:4' })
            .toBuffer(),
        )
        const service = Images.make(WasmImages.makeAdapter(Photon))
        const result = yield* service.prepare(bytes, 'image/jpeg', {
          maxWidth: 2,
          maxHeight: 4097,
          maxBytes: 1000000,
          maxInputPixels: 8194,
        })
        assert.isTrue(result.passthrough)
        assert.deepStrictEqual(result.bytes, Uint8Array.from(bytes))
      }),
    )
  }
}

it.effect('JPEG resolution conversion accepts integer budget extremes', () =>
  Effect.gen(function* () {
    const bytes = yield* Effect.promise(() =>
      sharp({ create: { width: 1, height: 1, channels: 3, background: '#274b91' } })
        .jpeg()
        .toBuffer(),
    )
    const service = Images.make(WasmImages.makeAdapter(Photon))
    for (const maxInputPixels of [
      1,
      8193,
      8194,
      8195,
      999999,
      1000000,
      1000001,
      65535 * 65535,
      9007199254000000,
      9007199254000001,
      Number.MAX_SAFE_INTEGER,
    ]) {
      const result = yield* service.prepare(bytes, 'image/jpeg', {
        maxWidth: 1,
        maxHeight: 1,
        maxBytes: 1000000,
        maxInputPixels,
      })
      assert.isTrue(result.passthrough)
      assert.deepStrictEqual(result.bytes, Uint8Array.from(bytes))
    }
  }),
)
