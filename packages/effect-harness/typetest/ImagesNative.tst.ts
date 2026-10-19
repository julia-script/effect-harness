import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Images from 'effect-harness/ImagePreparation'
import type { ImagePreparationError, ImageDecodeFailed } from 'effect-harness/ImagePreparationError'
import * as Node from 'effect-harness/images-node'
import * as Wasm from 'effect-harness/images-wasm'

const program = Images.prepare(new Uint8Array(), 'image/png', {
  maxWidth: 10,
  maxHeight: 10,
  maxBytes: 100,
  maxInputPixels: 100,
})

test('native and portable Layers satisfy image preparation without hiding the error channel', () => {
  expect(Node.layer).type.toBe<Layer.Layer<Images.ImagePreparation, ImageDecodeFailed>>()
  expect(Wasm.layer).type.toBe<Layer.Layer<Images.ImagePreparation, ImageDecodeFailed>>()
  expect(program.pipe(Effect.provide(Node.layer))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError>
  >()
  expect(program.pipe(Effect.provide(Wasm.layer))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError>
  >()
  expect(program.pipe(Effect.provide(Wasm.layerWithPhoton))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError, Wasm.Photon>
  >()
  expect(program.pipe(Effect.provide(Node.layerWithPhoton))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError, Wasm.Photon>
  >()
  expect(Node.layerWithPhoton).type.toBe<
    Layer.Layer<Images.ImagePreparation, ImageDecodeFailed, Wasm.Photon>
  >()
  expect(Effect.runPromise).type.not.toBeCallableWith(
    program.pipe(Effect.provide(Wasm.layerWithPhoton)),
  )
})

test('environment-supplied WASM loaders preserve their explicit requirements and failures', () => {
  class AssetLoader extends Context.Service<
    AssetLoader,
    { readonly load: Effect.Effect<Wasm.PhotonApi, 'asset missing'> }
  >()('typetest/ImagesNative/AssetLoader') {}
  const load = Effect.flatMap(AssetLoader, (service) => service.load)
  const binding = Wasm.photonLayerWith(load)
  expect(binding).type.toBe<Layer.Layer<Wasm.Photon, 'asset missing', AssetLoader>>()
  const portable = Wasm.layerWithPhoton.pipe(Layer.provide(binding))
  expect(program.pipe(Effect.provide(portable))).type.toBe<
    Effect.Effect<Images.PreparedImage, ImagePreparationError | 'asset missing', AssetLoader>
  >()
  expect(Wasm.Photon.of).type.not.toBeCallableWith({})
})
