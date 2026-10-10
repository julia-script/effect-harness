import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Photon from '@cf-wasm/photon/node'
import sharp from 'sharp'
import * as Images from '../src/ImagePreparation.js'
import * as NodeImages from '../src/images-node/index.js'
import * as WasmImages from '../src/images-wasm/index.js'
import { apng, mutate } from './fixtures/images-strict/Apng.js'
import { fixture, rgba } from './fixtures/images-native/Conformance.js'

const limits: Images.Limits = { maxWidth: 2, maxHeight: 2, maxBytes: 10000, maxInputPixels: 4 }
const png = (r: number, g: number, b: number, alpha = 1, width = 2, height = 2) =>
  Effect.promise(() =>
    sharp({ create: { width, height, channels: 4, background: { r, g, b, alpha } } })
      .png()
      .toBuffer(),
  )

for (const [name, adapter] of [
  ['Node', NodeImages.makeAdapter(sharp, WasmImages.makeAdapter(Photon))],
  ['Photon', WasmImages.makeAdapter(Photon)],
] as const) {
  const service = Images.make(adapter)
  it.effect(`${name} fully validates every APNG frame before passthrough`, () =>
    Effect.gen(function* () {
      const red = yield* png(255, 0, 0)
      const blue = yield* png(0, 0, 255)
      const good = apng(red, [{ png: red }, { png: blue }], true)
      const prepared = yield* service.prepare(good, 'image/png', limits)
      assert.isTrue(prepared.passthrough)
      assert.strictEqual(prepared.animation, 'unchanged')
      assert.deepStrictEqual(prepared.bytes, Uint8Array.from(good))
      for (const bad of [
        apng(red, [{ png: red }, { png: blue, corrupt: true }], true),
        apng(red, [{ png: blue, corrupt: true }], false),
      ]) {
        const failure = yield* service.prepare(bad, 'image/png', limits).pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'ImageDecodeFailed')
      }
    }),
  )
  for (const included of [true, false]) {
    it.effect(`${name} detects a single APNG frame (default included: ${included})`, () =>
      Effect.gen(function* () {
        const red = yield* png(255, 0, 0)
        const blue = yield* png(0, 0, 255)
        const bytes = apng(red, [{ png: blue }], included)
        const pass = yield* service.prepare(bytes, 'image/png', limits)
        assert.isTrue(pass.passthrough)
        assert.strictEqual(pass.animation, 'unchanged')
        const result = yield* service.prepare(bytes, 'image/png', {
          ...limits,
          maxWidth: 1,
          maxHeight: 1,
        })
        assert.strictEqual(result.animation, 'first-frame')
        assert.deepStrictEqual((yield* rgba(result.bytes)).data, Buffer.from([0, 0, 255, 255]))
      }),
    )
  }
  for (const blend of [0, 1]) {
    it.effect(
      `${name} composes a partial first APNG frame on transparent canvas (blend ${blend})`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const red = yield* png(255, 0, 0)
            const blue = yield* png(0, 0, 255, 0.5, 1, 1)
            const bytes = apng(red, [{ png: blue, left: 1, blend }], false)
            const decoded = yield* adapter.decode(bytes, 'image/png', 4)
            assert.isTrue(decoded.metadata.animated)
            assert.isTrue(decoded.metadata.hasAlpha)
            const result = yield* decoded.encode({
              mimeType: 'image/png',
              dimensions: { width: 2, height: 2 },
              orientation: 1,
              frame: 'first',
              transparency: 'preserve',
            })
            assert.deepStrictEqual(
              (yield* rgba(result)).data,
              Buffer.from([0, 0, 0, 0, 0, 0, 255, 128, 0, 0, 0, 0, 0, 0, 0, 0]),
            )
          }),
        ),
    )
  }
  it.effect(
    `${name} rejects APNG CRC, framing, sequence, count and rectangles before native decode`,
    () =>
      Effect.gen(function* () {
        const red = yield* png(255, 0, 0)
        const bytes = apng(red, [{ png: red }, { png: red }], true)
        let allocations = 0
        class GuardedPhoton extends Photon.PhotonImage {
          static override new_from_byteslice(bytes: Uint8Array) {
            allocations++
            return Photon.PhotonImage.new_from_byteslice(bytes)
          }
        }
        const guardedPortable = WasmImages.makeAdapter({ ...Photon, PhotonImage: GuardedPhoton })
        const guardedNative: typeof sharp = new Proxy(sharp, {
          apply(target, _receiver, args: Parameters<typeof sharp>) {
            allocations++
            return target(...args)
          },
        })
        const guarded = Images.make(
          name === 'Node'
            ? NodeImages.makeAdapter(guardedNative, guardedPortable)
            : guardedPortable,
        )
        const badCrc = Buffer.from(bytes)
        badCrc[badCrc.length - 1] = badCrc[badCrc.length - 1]! ^ 1
        for (const bad of [
          badCrc,
          bytes.subarray(0, bytes.length - 1),
          mutate(bytes, 'fcTL', (data) => {
            data.writeUInt32BE(9, 0)
          }),
          mutate(bytes, 'fdAT', (data) => {
            data.writeUInt32BE(9, 0)
          }),
          mutate(bytes, 'acTL', (data) => {
            data.writeUInt32BE(3, 0)
          }),
          mutate(bytes, 'fcTL', (data) => {
            data.writeUInt32BE(1, 12)
          }),
          mutate(bytes, 'fcTL', (data) => {
            data[25] = 2
          }),
        ]) {
          const failure = yield* guarded.prepare(bad, 'image/png', limits).pipe(Effect.flip)
          assert.strictEqual(failure._tag, 'ImageDecodeFailed')
        }
        const oversized = mutate(bytes, 'fcTL', (data) => {
          data.writeUInt32BE(5, 4)
        })
        const failure = yield* guarded.prepare(oversized, 'image/png', limits).pipe(Effect.flip)
        assert.strictEqual(failure._tag, 'ImageInputPixelsExceeded')
        assert.strictEqual(allocations, 0)
      }),
  )
}

it.effect('Photon strictly rejects premature JPEG entropy markers at 90/50/10 percent', () =>
  Effect.gen(function* () {
    let seed = 331
    const pixels = Buffer.from(
      Array.from({ length: 64 * 64 * 3 }, () => {
        seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
        return seed >>> 24
      }),
    )
    const baseline = yield* Effect.promise(() =>
      sharp(pixels, { raw: { width: 64, height: 64, channels: 3 } })
        .jpeg({ progressive: false, quality: 95 })
        .toBuffer(),
    )
    let start = 2
    while (start < baseline.length) {
      if (baseline[start + 1] === 0xda) {
        start += 2 + baseline.readUInt16BE(start + 2)
        break
      }
      start += 2 + baseline.readUInt16BE(start + 2)
    }
    const service = Images.make(WasmImages.makeAdapter(Photon))
    const budget = { maxWidth: 100, maxHeight: 100, maxBytes: 100000, maxInputPixels: 4096 }
    assert.isTrue((yield* service.prepare(baseline, 'image/jpeg', budget)).passthrough)
    for (const fraction of [0.9, 0.5, 0.1]) {
      const bytes = Buffer.concat([
        baseline.subarray(0, start + Math.floor((baseline.length - start - 2) * fraction)),
        Buffer.from([255, 217]),
      ])
      const failure = yield* service.prepare(bytes, 'image/jpeg', budget).pipe(Effect.flip)
      assert.strictEqual(failure._tag, 'ImageDecodeFailed')
    }
    const progressive = yield* Effect.promise(() =>
      sharp(pixels, { raw: { width: 64, height: 64, channels: 3 } })
        .jpeg({ progressive: true })
        .toBuffer(),
    )
    assert.isTrue((yield* service.prepare(progressive, 'image/jpeg', budget)).passthrough)
    for (const orientation of [1, 2, 3, 4, 5, 6, 7, 8]) {
      const result = yield* service.prepare(
        fixture(`orientation-${orientation}.jpg`),
        'image/jpeg',
        { ...budget, maxInputPixels: 6 },
      )
      assert.strictEqual(result.orientation, orientation)
    }
  }),
)
