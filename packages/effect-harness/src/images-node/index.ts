import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type sharp from 'sharp'
import * as ImagePreparation from '../ImagePreparation.js'
import { ImageDecodeFailed, ImageEncodeFailed } from '../ImagePreparationError.js'
import * as Wasm from '../images-wasm/index.js'
import * as Headers from '../images-wasm/Headers.js'
import { additionalFrames } from '../images-wasm/Frames.js'
import * as Pixels from '../images-wasm/Pixels.js'

export const capabilities: ImagePreparation.AdapterCapabilities = Wasm.capabilities

/** Sharp handles PNG/JPEG/GIF/WebP. BMP explicitly delegates to the supplied portable decoder.
 * Borrowed buffers remain live until noncancellable libvips work settles, even on interruption.
 */
export const makeAdapter = (
  codec: typeof sharp,
  bmp: ImagePreparation.Adapter,
): ImagePreparation.Adapter => ({
  capabilities: {
    ...capabilities,
    decode: capabilities.decode.filter(
      (mime) => mime !== 'image/bmp' || bmp.capabilities.decode.includes(mime),
    ),
    encode: capabilities.encode.filter(
      (mime) =>
        !bmp.capabilities.decode.includes('image/bmp') || bmp.capabilities.encode.includes(mime),
    ),
    orientations: capabilities.orientations.filter(
      (orientation) =>
        !bmp.capabilities.decode.includes('image/bmp') ||
        bmp.capabilities.orientations.includes(orientation),
    ),
    preserveAlpha: !bmp.capabilities.decode.includes('image/bmp') || bmp.capabilities.preserveAlpha,
    flattenWhite: !bmp.capabilities.decode.includes('image/bmp') || bmp.capabilities.flattenWhite,
  },
  decode: Effect.fn('ImagesNode.decode')(function* (bytes, mimeType, maxInputPixels) {
    if (mimeType === 'image/bmp') return yield* bmp.decode(bytes, mimeType, maxInputPixels)
    const header = yield* Headers.inspect(bytes, mimeType, maxInputPixels)
    let active = true
    const decoder = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          codec(bytes, { limitInputPixels: maxInputPixels, failOn: 'warning', pages: 1, page: 0 }),
        catch: (cause) =>
          new ImageDecodeFailed({ mimeType, message: 'Sharp decoder creation failed', cause }),
      }),
      (image) =>
        Effect.sync(() => {
          active = false
          image.destroy()
        }),
    )
    // Sharp's Promise is not cancellable. Mask interruption until it has released its borrowed input.
    const decoded = yield* Effect.tryPromise({
      try: () => decoder.ensureAlpha().raw().toBuffer({ resolveWithObject: true }),
      catch: (cause) =>
        new ImageDecodeFailed({ mimeType, message: 'Sharp full decode failed', cause }),
    }).pipe(Effect.uninterruptible)
    if (
      decoded.info.width !== header.dimensions.width ||
      decoded.info.height !== header.dimensions.height ||
      decoded.info.channels !== 4
    ) {
      return yield* new ImageDecodeFailed({
        mimeType,
        message: 'Decoded dimensions disagree with header',
      })
    }
    const metadata: ImagePreparation.ImageMetadata = {
      mimeType,
      original: header.dimensions,
      orientation: header.orientation,
      animated: header.animated,
      hasAlpha: Pixels.hasAlpha(decoded.data),
    }
    for (const frame of additionalFrames(bytes, mimeType)) {
      yield* Effect.acquireUseRelease(
        Effect.try({
          try: () =>
            codec(frame, { limitInputPixels: maxInputPixels, failOn: 'warning', pages: 1 }),
          catch: (cause) =>
            new ImageDecodeFailed({
              mimeType,
              message: 'Sharp animation decoder creation failed',
              cause,
            }),
        }),
        (image) =>
          Effect.tryPromise({
            try: () => image.raw().toBuffer(),
            catch: (cause) =>
              new ImageDecodeFailed({
                mimeType,
                message: 'Sharp animation frame decode failed',
                cause,
              }),
          }).pipe(Effect.uninterruptible),
        (image) => Effect.sync(() => image.destroy()),
      )
    }
    const encode = Effect.fn('ImagesNode.encode')(function* (
      request: ImagePreparation.EncodingRequest,
    ) {
      if (!active)
        return yield* new ImageEncodeFailed({
          mimeType: request.mimeType,
          message: 'Decoded image scope is closed',
        })
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const upright = ImagePreparation.orientedDimensions(
            metadata.original,
            request.orientation,
          )
          const candidate = yield* Effect.acquireRelease(
            Effect.try({
              try: () => {
                const pixels = Pixels.orient(decoded.data, metadata.original, request.orientation)
                if (request.transparency === 'flatten-white') Pixels.flattenWhite(pixels)
                return codec(pixels, {
                  raw: { width: upright.width, height: upright.height, channels: 4 },
                })
              },
              catch: (cause) =>
                new ImageEncodeFailed({
                  mimeType: request.mimeType,
                  message: 'Sharp encoder creation failed',
                  cause,
                }),
            }),
            (image) => Effect.sync(() => image.destroy()),
          )
          return yield* Effect.tryPromise({
            try: async () => {
              candidate.resize(request.dimensions.width, request.dimensions.height, { fit: 'fill' })
              switch (request.mimeType) {
                case 'image/png':
                  candidate.png()
                  break
                case 'image/jpeg':
                  candidate.jpeg({ quality: request.quality ?? 80 })
                  break
                case 'image/webp':
                  candidate.webp({ quality: request.quality ?? 80 })
                  break
              }
              return Uint8Array.from(await candidate.toBuffer())
            },
            catch: (cause) =>
              new ImageEncodeFailed({
                mimeType: request.mimeType,
                message: 'Sharp encode failed',
                cause,
              }),
          }).pipe(Effect.uninterruptible)
        }),
      )
    })
    return { metadata, encode } satisfies ImagePreparation.DecodedImage
  }),
})

/** Optional native imports are deferred until Layer acquisition. Loader errors remain semantic. */
export const layerWithPhoton: Layer.Layer<
  ImagePreparation.ImagePreparation,
  ImageDecodeFailed,
  Wasm.Photon
> = Layer.effect(
  ImagePreparation.ImagePreparation,
  Effect.gen(function* () {
    const photon = yield* Wasm.Photon
    const codec = yield* Effect.tryPromise({
      try: () => import('sharp'),
      catch: (cause) =>
        new ImageDecodeFailed({ mimeType: 'image/png', message: 'Cannot load sharp', cause }),
    }).pipe(Effect.uninterruptible)
    return ImagePreparation.make(makeAdapter(codec.default, Wasm.makeAdapter(photon)))
  }),
)

export const layer: Layer.Layer<ImagePreparation.ImagePreparation, ImageDecodeFailed> =
  layerWithPhoton.pipe(Layer.provide(Wasm.photonLayer))
