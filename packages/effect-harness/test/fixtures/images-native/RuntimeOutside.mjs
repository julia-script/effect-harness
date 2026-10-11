import { register } from 'node:module'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
register(new URL('./SourceLoader.mjs', import.meta.url))
const Effect = await import('effect/Effect')
const Layer = await import('effect/Layer')
const Images = await import('../../../src/ImagePreparation.ts')
const NodeImages = await import('../../../src/images-node/index.ts')
const WasmImages = await import('../../../src/images-wasm/index.ts')
const Photon = await import('@cf-wasm/photon/others')
await Photon.initPhoton({
  module_or_path: await readFile(new URL(import.meta.resolve('@cf-wasm/photon/photon.wasm'))),
})
const supplied = WasmImages.layerWithPhoton.pipe(
  Layer.provide(WasmImages.photonLayerWith(Effect.succeed(Photon))),
)
const bmp = Uint8Array.from(await readFile(new URL('./opaque.bmp', import.meta.url)))
for (const layer of [supplied, WasmImages.layer, NodeImages.layer]) {
  const image = await Effect.runPromise(
    Images.prepare(bmp, 'image/bmp', {
      maxWidth: 3,
      maxHeight: 2,
      maxBytes: 1000,
      maxInputPixels: 6,
    }).pipe(Effect.provide(layer)),
  )
  assert.equal(image.passthrough, false)
  assert.deepEqual(image.dimensions, { width: 3, height: 2 })
}
console.log('Native, embedded WASM and supplied asset loader pass outside repository cwd')
