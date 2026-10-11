import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Images from 'effect-harness/ImagePreparation'
import * as BunImages from 'effect-harness/images-bun'

// Run with Bun after the integration build. This decodes a real PNG, not metadata.
const png = Uint8Array.from(
  Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==',
    'base64',
  ),
)
const program = Images.prepare(png, 'image/png', {
  maxWidth: 1,
  maxHeight: 1,
  maxBytes: 1000,
  maxInputPixels: 1,
}).pipe(
  Effect.provide(BunImages.layer),
  Effect.flatMap((image) =>
    Console.log({
      mimeType: image.mimeType,
      dimensions: image.dimensions,
      passthrough: image.passthrough,
    }),
  ),
)
await Effect.runPromise(program)
