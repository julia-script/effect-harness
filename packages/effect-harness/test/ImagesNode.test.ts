import { conformance } from './fixtures/images-native/Conformance.js'
import * as Node from '../src/images-node/index.js'
conformance('Node sharp with portable BMP fallback', Node.layer)

import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import sharp from 'sharp'
import * as Photon from '@cf-wasm/photon/node'
import * as Wasm from '../src/images-wasm/index.js'
import * as Images from '../src/ImagePreparation.js'
import { fixture, limits, rgba } from './fixtures/images-native/Conformance.js'

for (const stage of ['decode', 'encode'] as const) {
  it.effect(`waits for noncancellable native ${stage} before destroying its real handle`, () =>
    Effect.gen(function* () {
      let created = 0,
        destroyed = 0,
        settled = false
      let announce: () => void = () => {}
      let finish: () => void = () => {}
      const started = new Promise<void>((resolve) => {
        announce = resolve
      })
      const lease = new Promise<void>((resolve) => {
        finish = resolve
      })
      const codec: typeof sharp = new Proxy(sharp, {
        apply(target, _receiver, args: Parameters<typeof sharp>) {
          const image = target(...args)
          const own = ++created
          const destroy = image.destroy.bind(image)
          image.destroy = (error) => {
            destroyed++
            assert.isTrue(settled)
            return destroy(error)
          }
          image.toBuffer = new Proxy(image.toBuffer.bind(image), {
            apply(buffer, receiver, args: [{ resolveWithObject: true }]) {
              const pending = buffer.call(receiver, ...args)
              if (own !== (stage === 'decode' ? 1 : 2)) return pending
              announce()
              return pending.then(async (result) => {
                await lease
                settled = true
                return result
              })
            },
          })
          return image
        },
      })
      const adapter = Node.makeAdapter(codec, Wasm.makeAdapter(Photon))
      const fiber = yield* Effect.scoped(
        Effect.gen(function* () {
          const decoded = yield* adapter.decode(fixture('opaque.png'), 'image/png', 100)
          if (stage === 'encode')
            return yield* decoded.encode({
              mimeType: 'image/png',
              dimensions: { width: 2, height: 1 },
              orientation: 1,
              frame: 'first',
              transparency: 'preserve',
            })
          return decoded
        }),
      ).pipe(Effect.forkChild({ startImmediately: true }))
      yield* Effect.promise(() => started)
      fiber.interruptUnsafe()
      yield* Effect.yieldNow
      assert.strictEqual(destroyed, 0)
      finish()
      yield* Fiber.await(fiber)
      assert.strictEqual(created, stage === 'decode' ? 1 : 2)
      assert.strictEqual(destroyed, created)
    }),
  )
}

it.effect('destroys each sharp handle exactly once on success and exhausted budget', () =>
  Effect.gen(function* () {
    let created = 0,
      destroyed = 0
    const seen = new WeakSet<ReturnType<typeof sharp>>()
    const codec: typeof sharp = new Proxy(sharp, {
      apply(target, _receiver, args: Parameters<typeof sharp>) {
        const image = target(...args)
        created++
        const destroy = image.destroy.bind(image)
        image.destroy = (error) => {
          assert.isFalse(seen.has(image))
          seen.add(image)
          destroyed++
          return destroy(error)
        }
        return image
      },
    })
    const service = Images.make(Node.makeAdapter(codec, Wasm.makeAdapter(Photon)))
    yield* service.prepare(fixture('opaque.png'), 'image/png', { ...limits, maxWidth: 2 })
    assert.strictEqual(created, destroyed)
    yield* service
      .prepare(fixture('opaque.png'), 'image/png', { ...limits, maxBytes: 4 })
      .pipe(Effect.flip)
    assert.strictEqual(created, destroyed)
    assert.isAbove(created, 3)
  }),
)

it.effect('sharp candidates use original pixels and all output codecs execute', () =>
  Effect.gen(function* () {
    const adapter = Node.makeAdapter(sharp, Wasm.makeAdapter(Photon))
    yield* Effect.scoped(
      Effect.gen(function* () {
        const decoded = yield* adapter.decode(fixture('alpha.png'), 'image/png', 100)
        const request: Images.EncodingRequest = {
          mimeType: 'image/png',
          dimensions: { width: 3, height: 2 },
          orientation: 1,
          frame: 'first',
          transparency: 'preserve',
        }
        const before = yield* decoded.encode(request)
        const transparent = yield* adapter.decode(fixture('transparent.png'), 'image/png', 100)
        const jpeg = yield* transparent.encode({
          ...request,
          mimeType: 'image/jpeg',
          transparency: 'flatten-white',
          quality: 100,
        })
        const white = yield* rgba(jpeg)
        assert.isAbove(white.data[0]!, 230)
        assert.isAbove(white.data[1]!, 230)
        assert.isAbove(white.data[2]!, 230)
        yield* decoded.encode({
          ...request,
          mimeType: 'image/jpeg',
          quality: 1,
          dimensions: { width: 1, height: 1 },
          transparency: 'flatten-white',
        })
        const after = yield* decoded.encode(request)
        assert.deepStrictEqual(before, after)
        const webp = yield* decoded.encode({ ...request, mimeType: 'image/webp', quality: 70 })
        assert.strictEqual((yield* rgba(webp)).info.width, 3)
      }),
    )
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
    }).pipe(Effect.provide(Node.layer)),
  )
}

it.effect('rejects allocation budgets before constructing either a sharp or BMP handle', () =>
  Effect.gen(function* () {
    let allocations = 0
    class GuardedPhoton extends Photon.PhotonImage {
      static override new_from_byteslice(bytes: Uint8Array) {
        allocations++
        return Photon.PhotonImage.new_from_byteslice(bytes)
      }
    }
    const native: typeof sharp = new Proxy(sharp, {
      apply(target, _receiver, args: Parameters<typeof sharp>) {
        allocations++
        return target(...args)
      },
    })
    const adapter = Node.makeAdapter(
      native,
      Wasm.makeAdapter({ ...Photon, PhotonImage: GuardedPhoton }),
    )
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
        const decoded = yield* Node.makeAdapter(sharp, Wasm.makeAdapter(Photon)).decode(
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

it.effect('maps a real sharp encoder rejection to a semantic encoding error', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const decoded = yield* Node.makeAdapter(sharp, Wasm.makeAdapter(Photon)).decode(
        fixture('opaque.png'),
        'image/png',
        100,
      )
      const failure = yield* decoded
        .encode({
          mimeType: 'image/jpeg',
          quality: 0,
          dimensions: { width: 3, height: 2 },
          orientation: 1,
          frame: 'first',
          transparency: 'preserve',
        })
        .pipe(Effect.flip)
      assert.strictEqual(failure._tag, 'ImageEncodeFailed')
    }),
  ),
)
