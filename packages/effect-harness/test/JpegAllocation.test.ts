import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Photon from '@cf-wasm/photon/node'
import sharp from 'sharp'
import * as Images from '../src/ImagePreparation.js'
import * as WasmImages from '../src/images-wasm/index.js'
import * as Jpeg from '../src/images-wasm/Jpeg.js'

const segment = (bytes: Buffer, marker: number) => {
  for (let offset = 2; offset < bytes.length;) {
    if (bytes[offset + 1] === marker) return offset
    offset += 2 + bytes.readUInt16BE(offset + 2)
  }
  return assert.fail(`Missing JPEG marker ${marker}`)
}

const service = Images.make(WasmImages.makeAdapter(Photon))
for (const [width, height] of [
  [1, 8192],
  [8192, 1],
  [1, 1],
  [7, 9],
  [8, 8],
  [9, 7],
  [15, 17],
  [17, 15],
  [32, 32],
] as const) {
  for (const progressive of [false, true]) {
    for (const chromaSubsampling of ['4:4:4', '4:2:0']) {
      it.effect(
        `strict JPEG accepts ${width}x${height} ${chromaSubsampling} progressive=${progressive} at exact pixel cap`,
        () =>
          Effect.gen(function* () {
            const bytes = yield* Effect.promise(() =>
              sharp({ create: { width, height, channels: 3, background: '#143c78' } })
                .jpeg({ progressive, chromaSubsampling })
                .toBuffer(),
            )
            const prepared = yield* service.prepare(bytes, 'image/jpeg', {
              maxWidth: width,
              maxHeight: height,
              maxBytes: 1000000,
              maxInputPixels: width * height,
            })
            assert.isTrue(prepared.passthrough)
            assert.deepStrictEqual(prepared.original, { width, height })
            assert.deepStrictEqual(prepared.bytes, Uint8Array.from(bytes))
          }),
      )
    }
  }
}

for (const [colourspace, components] of [
  ['b-w', 1],
  ['cmyk', 4],
] as const) {
  for (const progressive of [false, true]) {
    it.effect(`strict JPEG budgets ${components} components, progressive=${progressive}`, () =>
      Effect.gen(function* () {
        const bytes = yield* Effect.promise(() =>
          sharp({ create: { width: 1, height: 8192, channels: 3, background: '#143c78' } })
            .toColourspace(colourspace)
            .jpeg({ progressive, chromaSubsampling: '4:4:4' })
            .toBuffer(),
        )
        const frame = segment(bytes, progressive ? 0xc2 : 0xc0)
        assert.strictEqual(bytes[frame + 9], components)
        const result = yield* service.prepare(bytes, 'image/jpeg', {
          maxWidth: 1,
          maxHeight: 8192,
          maxBytes: 1000000,
          maxInputPixels: 8192,
        })
        assert.isTrue(result.passthrough)
        assert.deepStrictEqual(result.bytes, Uint8Array.from(bytes))
      }),
    )
  }
}

it.effect('strict JPEG covers sampling factors three and four at padded block boundaries', () =>
  Effect.gen(function* () {
    for (const progressive of [false, true]) {
      const bytes = yield* Effect.promise(() =>
        sharp({ create: { width: 9, height: 17, channels: 3, background: '#143c78' } })
          .toColourspace('b-w')
          .jpeg({ progressive })
          .toBuffer(),
      )
      const frame = segment(bytes, progressive ? 0xc2 : 0xc0)
      assert.strictEqual(bytes[frame + 9], 1)
      // A single-component scan visits the same image blocks regardless of sampling;
      // only coefficient MCU padding changes. Keep its valid entropy untouched.
      for (const sampling of [0x33, 0x44]) {
        const sampled = Buffer.from(bytes)
        sampled[frame + 11] = sampling
        yield* Jpeg.validate(sampled, { width: 9, height: 17 }, 153)
      }
    }
  }),
)

it.effect('strict JPEG rejects excess source pixels before Photon allocation', () =>
  Effect.gen(function* () {
    let allocations = 0
    class GuardedPhoton extends Photon.PhotonImage {
      static override new_from_byteslice(bytes: Uint8Array) {
        allocations++
        return Photon.PhotonImage.new_from_byteslice(bytes)
      }
    }
    const guarded = Images.make(WasmImages.makeAdapter({ ...Photon, PhotonImage: GuardedPhoton }))
    const bytes = yield* Effect.promise(() =>
      sharp({ create: { width: 1, height: 8192, channels: 3, background: '#143c78' } })
        .jpeg({ chromaSubsampling: '4:4:4' })
        .toBuffer(),
    )
    const failure = yield* guarded
      .prepare(bytes, 'image/jpeg', {
        maxWidth: 1,
        maxHeight: 8192,
        maxBytes: 1000000,
        maxInputPixels: 8191,
      })
      .pipe(Effect.flip)
    assert.strictEqual(failure._tag, 'ImageInputPixelsExceeded')
    assert.strictEqual(allocations, 0)
  }),
)

it.effect(
  'strict JPEG keeps cumulative allocations bounded even with a huge caller pixel cap',
  () =>
    Effect.gen(function* () {
      const bytes = yield* Effect.promise(() =>
        sharp({ create: { width: 1, height: 1, channels: 3, background: '#143c78' } })
          .jpeg({ chromaSubsampling: '4:4:4' })
          .toBuffer(),
      )
      const offset = segment(bytes, 0xdb)
      const table = bytes.subarray(offset, offset + 2 + bytes.readUInt16BE(offset + 2))
      const repeatedTables = Buffer.concat([
        bytes.subarray(0, offset),
        ...Array.from({ length: 512 }, () => table),
        bytes.subarray(offset),
      ])
      for (const maxInputPixels of [1, Number.MAX_SAFE_INTEGER]) {
        const failure = yield* service
          .prepare(repeatedTables, 'image/jpeg', {
            maxWidth: 1,
            maxHeight: 1,
            maxBytes: 1000000,
            maxInputPixels,
          })
          .pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'ImageDecodeFailed')
        if (failure._tag === 'ImageDecodeFailed') {
          assert.instanceOf(failure.cause, Error)
          if (failure.cause instanceof Error)
            assert.include(failure.cause.message, 'maxMemoryUsageInMB limit exceeded')
        }
      }
    }),
)
