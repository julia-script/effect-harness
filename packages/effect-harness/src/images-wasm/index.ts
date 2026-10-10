import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as ImagePreparation from '../ImagePreparation.js'
import { ImageDecodeFailed, ImageEncodeFailed } from '../ImagePreparationError.js'
import * as Headers from './Headers.js'
import { additionalFrames } from './Frames.js'
import * as Pixels from './Pixels.js'

/** Initialized Photon API. Its WebP output is lossless; JPEG accepts quality, PNG is lossless.
 * Supply an independently initialized implementation or the package's once-initialized glue.
 * Do not reset shared wasm-bindgen glue while any image handle is alive.
 */
export type PhotonApi = Pick<
  typeof import('@cf-wasm/photon/others'),
  'PhotonImage' | 'resize' | 'SamplingFilter'
>

/** Runtime-local codec loader boundary. No filesystem, cwd, global environment or backend selection. */
export class Photon extends Context.Service<Photon, PhotonApi>()(
  'effect-harness/images-wasm/Photon',
) {}

export const capabilities: ImagePreparation.AdapterCapabilities = {
  decode: ['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/bmp'],
  encode: ['image/png', 'image/jpeg', 'image/webp'],
  orientations: [1, 2, 3, 4, 5, 6, 7, 8],
  preserveAlpha: true,
  flattenWhite: true,
}

/** Embedded WASM supplied by the dependency: no repository-relative asset or Node built-ins.
 * Loading is deferred until Layer acquisition; ESM caching initializes shared glue once.
 * Other environments may supply Photon directly, or use photonLayerWith with their asset loader.
 */
export const photonLayer: Layer.Layer<Photon, ImageDecodeFailed> = Layer.effect(
  Photon,
  Effect.tryPromise({
    try: async () => {
      const api = await import('@cf-wasm/photon/others')
      if (api.initPhoton.initialized) {
        await api.initPhoton.ensure()
        return api
      }
      return import('@cf-wasm/photon/node')
    },
    catch: (cause) =>
      new ImageDecodeFailed({ mimeType: 'image/png', message: 'Cannot load Photon WASM', cause }),
  }).pipe(Effect.uninterruptible),
)

/** Keep custom loader failures and requirements visible at the provisioning boundary. */
export const photonLayerWith = <E, R>(
  loader: Effect.Effect<PhotonApi, E, R>,
): Layer.Layer<Photon, E, R> => Layer.effect(Photon, loader)

/** All Photon operations are synchronous. Scope releases each source/candidate exactly once. */
export const makeAdapter = (photon: PhotonApi): ImagePreparation.Adapter => ({
  capabilities,
  decode: Effect.fn('ImagesWasm.decode')(function* (bytes, mimeType, maxInputPixels) {
    const header = yield* Headers.inspect(bytes, mimeType, maxInputPixels)
    let active = true
    const source = yield* Effect.acquireRelease(
      Effect.try({
        try: () => photon.PhotonImage.new_from_byteslice(bytes),
        catch: (cause) =>
          new ImageDecodeFailed({ mimeType, message: 'Photon decode failed', cause }),
      }),
      (image) =>
        Effect.sync(() => {
          active = false
          image.free()
        }),
    )
    const metadata = yield* Effect.try({
      try: () => {
        if (
          source.get_width() !== header.dimensions.width ||
          source.get_height() !== header.dimensions.height
        ) {
          throw new ImageDecodeFailed({
            mimeType,
            message: 'Decoded dimensions disagree with header',
          })
        }
        const pixels = source.get_raw_pixels()
        if (pixels.length !== header.dimensions.width * header.dimensions.height * 4) {
          throw new ImageDecodeFailed({ mimeType, message: 'Decoder returned invalid RGBA pixels' })
        }
        return {
          mimeType,
          original: header.dimensions,
          orientation: header.orientation,
          animated: header.animated,
          hasAlpha: Pixels.hasAlpha(pixels),
        }
      },
      catch: (cause) =>
        new ImageDecodeFailed({ mimeType, message: 'Photon pixel validation failed', cause }),
    })
    for (const frame of additionalFrames(bytes, mimeType)) {
      yield* Effect.acquireUseRelease(
        Effect.try({
          try: () => photon.PhotonImage.new_from_byteslice(frame),
          catch: (cause) =>
            new ImageDecodeFailed({
              mimeType,
              message: 'Photon animation frame decode failed',
              cause,
            }),
        }),
        (image) =>
          Effect.try({
            try: () => {
              image.get_raw_pixels()
            },
            catch: (cause) =>
              new ImageDecodeFailed({
                mimeType,
                message: 'Photon animation pixel validation failed',
                cause,
              }),
          }),
        (image) => Effect.sync(() => image.free()),
      )
    }
    const encode = Effect.fn('ImagesWasm.encode')(function* (
      request: ImagePreparation.EncodingRequest,
    ) {
      if (!active)
        return yield* new ImageEncodeFailed({
          mimeType: request.mimeType,
          message: 'Decoded image scope is closed',
        })
      const upright = ImagePreparation.orientedDimensions(metadata.original, request.orientation)
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const candidate = yield* Effect.acquireRelease(
            Effect.try({
              try: () => {
                const pixels = Pixels.orient(
                  source.get_raw_pixels(),
                  metadata.original,
                  request.orientation,
                )
                if (request.transparency === 'flatten-white') Pixels.flattenWhite(pixels)
                return new photon.PhotonImage(pixels, upright.width, upright.height)
              },
              catch: (cause) =>
                new ImageEncodeFailed({
                  mimeType: request.mimeType,
                  message: 'Photon orientation failed',
                  cause,
                }),
            }),
            (image) => Effect.sync(() => image.free()),
          )
          const resized = yield* Effect.acquireRelease(
            Effect.try({
              try: () =>
                photon.resize(
                  candidate,
                  request.dimensions.width,
                  request.dimensions.height,
                  photon.SamplingFilter.Lanczos3,
                ),
              catch: (cause) =>
                new ImageEncodeFailed({
                  mimeType: request.mimeType,
                  message: 'Photon resize failed',
                  cause,
                }),
            }),
            (image) => Effect.sync(() => image.free()),
          )
          return yield* Effect.try({
            try: () => {
              switch (request.mimeType) {
                case 'image/png':
                  return Uint8Array.from(resized.get_bytes())
                case 'image/jpeg':
                  return Uint8Array.from(resized.get_bytes_jpeg(request.quality ?? 80))
                case 'image/webp':
                  return Uint8Array.from(resized.get_bytes_webp())
              }
            },
            catch: (cause) =>
              new ImageEncodeFailed({
                mimeType: request.mimeType,
                message: 'Photon encode failed',
                cause,
              }),
          })
        }),
      )
    })
    return { metadata, encode } satisfies ImagePreparation.DecodedImage
  }),
})

/** Bind image policy to an environment-supplied Photon service. */
export const layerWithPhoton: Layer.Layer<ImagePreparation.ImagePreparation, never, Photon> =
  Layer.effect(
    ImagePreparation.ImagePreparation,
    Effect.map(Photon, (photon) => ImagePreparation.make(makeAdapter(photon))),
  )

/** Ready-to-use portable service, backed by packaged embedded Photon WASM. */
export const layer: Layer.Layer<ImagePreparation.ImagePreparation, ImageDecodeFailed> =
  layerWithPhoton.pipe(Layer.provide(photonLayer))
