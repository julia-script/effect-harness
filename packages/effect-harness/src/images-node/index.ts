import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type sharp from 'sharp'
import * as ImagePreparation from '../ImagePreparation.js'
import { ImageDecodeFailed, ImageEncodeFailed } from '../ImagePreparationError.js'
import * as Wasm from '../images-wasm/index.js'
import * as Headers from '../images-wasm/Headers.js'
import { additionalFrames } from '../images-wasm/Frames.js'
import * as Pixels from '../images-wasm/Pixels.js'
import * as Png from '../images-wasm/Png.js'
import * as Lease from './Lease.js'

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
    const decoder = yield* Lease.acquire(
      Effect.try({
        try: () =>
          codec(header.png?.frames.length ? Png.image(header.png) : bytes, {
            limitInputPixels: maxInputPixels,
            failOn: 'warning',
            pages: 1,
            page: 0,
          }),
        catch: (cause) =>
          new ImageDecodeFailed({ mimeType, message: 'Sharp decoder creation failed', cause }),
      }),
      (image) => {
        image.destroy()
      },
    )
    return yield* Lease.use(
      decoder,
      () => new ImageDecodeFailed({ mimeType, message: 'Decoded image scope is closed' }),
      (image) =>
        Effect.gen(function* () {
          const decoded = yield* Effect.tryPromise({
            try: () => image.ensureAlpha().raw().toBuffer({ resolveWithObject: true }),
            catch: (cause) =>
              new ImageDecodeFailed({ mimeType, message: 'Sharp full decode failed', cause }),
          })
          if (
            decoded.info.width !== header.dimensions.width ||
            decoded.info.height !== header.dimensions.height ||
            decoded.info.channels !== 4
          )
            return yield* new ImageDecodeFailed({
              mimeType,
              message: 'Decoded dimensions disagree with header',
            })
          let pixels: Uint8Array = decoded.data
          if (header.png) {
            for (const frame of header.png.frames) {
              const framePixels = yield* Effect.scoped(
                Effect.gen(function* () {
                  const native = yield* Lease.acquire(
                    Effect.try({
                      try: () =>
                        codec(Png.image(header.png!, frame), {
                          limitInputPixels: maxInputPixels,
                          failOn: 'warning',
                        }),
                      catch: (cause) =>
                        new ImageDecodeFailed({
                          mimeType,
                          message: 'Sharp APNG decoder creation failed',
                          cause,
                        }),
                    }),
                    (handle) => {
                      handle.destroy()
                    },
                  )
                  return yield* Lease.use(
                    native,
                    () =>
                      new ImageDecodeFailed({ mimeType, message: 'APNG decoder scope is closed' }),
                    (handle) =>
                      Effect.tryPromise({
                        try: () => handle.ensureAlpha().raw().toBuffer({ resolveWithObject: true }),
                        catch: (cause) =>
                          new ImageDecodeFailed({
                            mimeType,
                            message: 'Sharp APNG frame decode failed',
                            cause,
                          }),
                      }),
                  )
                }),
              )
              if (
                framePixels.info.width !== frame.width ||
                framePixels.info.height !== frame.height ||
                framePixels.info.channels !== 4
              )
                return yield* new ImageDecodeFailed({
                  mimeType,
                  message: 'Invalid APNG decoded frame dimensions',
                })
              if (frame === header.png.frames[0]) {
                pixels = yield* Effect.try({
                  try: () => Png.firstCanvas(framePixels.data, frame, header.dimensions),
                  catch: (cause) =>
                    new ImageDecodeFailed({
                      mimeType,
                      message: 'APNG canvas composition failed',
                      cause,
                    }),
                })
              }
            }
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
              (handle) =>
                Effect.tryPromise({
                  try: () => handle.raw().toBuffer(),
                  catch: (cause) =>
                    new ImageDecodeFailed({
                      mimeType,
                      message: 'Sharp animation frame decode failed',
                      cause,
                    }),
                }),
              (handle) =>
                Effect.sync(() => {
                  handle.destroy()
                }),
            )
          }
          const metadata: ImagePreparation.ImageMetadata = {
            mimeType,
            original: header.dimensions,
            orientation: header.orientation,
            animated: header.animated,
            hasAlpha: Pixels.hasAlpha(pixels),
          }
          const encode = Effect.fn('ImagesNode.encode')(
            (request: ImagePreparation.EncodingRequest) =>
              Lease.use(
                decoder,
                () =>
                  new ImageEncodeFailed({
                    mimeType: request.mimeType,
                    message: 'Decoded image scope is closed',
                  }),
                () =>
                  Effect.scoped(
                    Effect.gen(function* () {
                      const upright = ImagePreparation.orientedDimensions(
                        metadata.original,
                        request.orientation,
                      )
                      const candidate = yield* Lease.acquire(
                        Effect.try({
                          try: () => {
                            const oriented = Pixels.orient(
                              pixels,
                              metadata.original,
                              request.orientation,
                            )
                            if (request.transparency === 'flatten-white')
                              Pixels.flattenWhite(oriented)
                            return codec(oriented, {
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
                        (handle) => {
                          handle.destroy()
                        },
                      )
                      return yield* Lease.use(
                        candidate,
                        () =>
                          new ImageEncodeFailed({
                            mimeType: request.mimeType,
                            message: 'Encoder scope is closed',
                          }),
                        (handle) =>
                          Effect.tryPromise({
                            try: async () => {
                              handle.resize(request.dimensions.width, request.dimensions.height, {
                                fit: 'fill',
                              })
                              switch (request.mimeType) {
                                case 'image/png':
                                  handle.png()
                                  break
                                case 'image/jpeg':
                                  handle.jpeg({ quality: request.quality ?? 80 })
                                  break
                                case 'image/webp':
                                  handle.webp({ quality: request.quality ?? 80 })
                                  break
                              }
                              return Uint8Array.from(await handle.toBuffer())
                            },
                            catch: (cause) =>
                              new ImageEncodeFailed({
                                mimeType: request.mimeType,
                                message: 'Sharp encode failed',
                                cause,
                              }),
                          }),
                      )
                    }),
                  ),
              ),
          )
          return { metadata, encode } satisfies ImagePreparation.DecodedImage
        }),
    )
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
