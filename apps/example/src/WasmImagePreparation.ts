import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Images from 'effect-harness/ImagePreparation'
import * as WasmImages from 'effect-harness/images-wasm'

// Embedded WASM uses no repository-relative file lookup. Environments may instead supply Photon.
const png = Uint8Array.from(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==',
    'base64',
  ),
)
await Effect.runPromise(
  Images.prepare(png, 'image/png', {
    maxWidth: 1,
    maxHeight: 1,
    maxBytes: 1000,
    maxInputPixels: 1,
  }).pipe(
    Effect.provide(WasmImages.layer),
    Effect.flatMap((image) =>
      Console.log({
        mimeType: image.mimeType,
        dimensions: image.dimensions,
        passthrough: image.passthrough,
      }),
    ),
  ),
)
