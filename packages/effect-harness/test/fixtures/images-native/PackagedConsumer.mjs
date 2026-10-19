import assert from 'node:assert/strict'
import * as Effect from 'effect/Effect'
import * as Images from 'effect-harness/ImagePreparation'
import * as NodeImages from 'effect-harness/images-node'
import * as WasmImages from 'effect-harness/images-wasm'
import * as Photon from '@cf-wasm/photon/others'
import { readFile } from 'node:fs/promises'

// Run after packing/installing effect-harness in an external consumer directory.
// First use the exported packaged asset with an explicit environment-supplied loader.
await Photon.initPhoton({
  module_or_path: await readFile(new URL(import.meta.resolve('@cf-wasm/photon/photon.wasm'))),
})
const supplied = WasmImages.layerWithPhoton.pipe(
  (await import('effect/Layer')).provide(WasmImages.photonLayerWith(Effect.succeed(Photon))),
)
const png = Uint8Array.from(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==',
    'base64',
  ),
)
for (const layer of [supplied, WasmImages.layer, NodeImages.layer]) {
  const image = await Effect.runPromise(
    Images.prepare(png, 'image/png', {
      maxWidth: 1,
      maxHeight: 1,
      maxBytes: 1000,
      maxInputPixels: 1,
    }).pipe(Effect.provide(layer)),
  )
  assert.equal(image.passthrough, true)
  assert.deepEqual(image.bytes, png)
}
console.log('Packaged native, embedded WASM and supplied asset loader pass outside repository')
